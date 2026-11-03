package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"gorm.io/gorm"

	"feed-api/internal/dto"
	"feed-api/internal/repository"
)

const posExample = `{"timestamp": "...", "storeId": "...", "register": "...", "transaction": "...", "seqNumber": "...", "sku": "...", "qty": "...", ...}`

// PosProcessor saves "pos" queue messages with EXACTLY the same steps as POST /pos.
// A body is ONE sale line or an ARRAY of lines:
//
//	[{"timestamp": "2026-09-22T18:01:14Z", "storeId": "106", "register": "002", "operator": "",
//	  "transactionType": "SALE", "transaction": "716886", "seqNumber": "1", "sku": "0078758001005",
//	  "qty": "1", "regularPriceEach": "3.59", "soldPriceEach": "3.59", "isPriceOverride": "0", "discountCode": "0"}]
//
// A sale is NOT idempotent: saving a line again adds its qty again (and deducts stock again).
// So a line is never saved twice by this processor: a repeated line in a batch is a duplicate, and a
// message is only abandoned (retried) when none of its lines was saved yet.
type PosProcessor struct {
	repo *repository.PosRepository
}

func NewPosProcessor(repo *repository.PosRepository) *PosProcessor {
	return &PosProcessor{repo: repo}
}

// posItem is one sale line from one message.
type posItem struct {
	storeID  string
	sale     dto.PosSale
	raw      json.RawMessage
	msg      int
	position int
}

// Handle runs one batch, line by line, in message order:
//
//  1. parse every body (object or array); CLEAN INPUT + VALIDATE (binding tags, then dto.Check:
//     timestamp a real date, integer columns fit, register not 0) => invalid; CONVERT VALUES (dto.Check)
//  2. de-duplicate by store + register + transaction + seqNumber + sku (a repeated line is never applied twice)
//  3. per line: FIND THE STORE by external_id, FIND ACTIVE PRODUCTS by sku / primary_upc / variant
//     => invalid, then deduct stock + replenishment picklists + pos record + remove empty picklists
//     in ONE transaction, the same repository call as the endpoint. A failed picklist safety check
//     => invalid (nothing saved). Matched only by primary_upc (nothing recorded) => unchanged
//  4. message outcome (see finish). On a DB error: messages with no saved line are abandoned (retried);
//     in a message that already saved lines, the lines not saved yet go to the dead-letter queue
//     (retrying the whole message would count the saved lines twice)
func (p *PosProcessor) Handle(ctx context.Context, msgs []*azservicebus.ReceivedMessage) BatchOutcome {
	out := BatchOutcome{Outcomes: make([]Outcome, len(msgs)), Counts: map[string]int64{
		"recorded": 0, "merged": 0, "deducted": 0, "picklistsCreated": 0, "emptyPicklistsRemoved": 0,
		"skipped": 0, "duplicates": 0, "unchanged": 0,
	}}
	notSaved := make([][]InvalidItem, len(msgs))
	savedCount := make([]int, len(msgs))
	broken := make([]bool, len(msgs))
	abandon := make([]bool, len(msgs))

	notSave := func(it posItem, typ, reason string) {
		notSaved[it.msg] = append(notSaved[it.msg], InvalidItem{Item: it.position, ExternalID: it.sale.Sku,
			Name: fmt.Sprintf("store %s / register %d / transaction %d / seq %d", it.storeID, it.sale.Register, it.sale.Transaction, it.sale.SeqNumber),
			Type: typ, Reason: reason, Raw: it.raw})
	}

	// 1. PARSE + VALIDATE + CONVERT
	var items []posItem
	for i, msg := range msgs {
		raws, err := splitBody(msg.Body, posExample)
		if err != nil {
			broken[i] = true
			out.Outcomes[i] = Outcome{Action: DeadLetter, Reason: "invalid message body", Description: err.Error()}
			continue
		}
		for n, raw := range raws {
			var req dto.PosRequest
			it := posItem{raw: raw, msg: i, position: n + 1}
			if err := json.Unmarshal(raw, &req); err != nil {
				notSave(it, itemInvalid, "item is not a JSON object like "+posExample)
				continue
			}
			reason, failed := validateItem(&req)
			sale, extra := req.Check(failed)
			it.storeID, it.sale = string(req.StoreId), sale
			if reason == "" && len(extra) > 0 {
				reason = extra[0].Message
			}
			if reason != "" {
				notSave(it, itemInvalid, reason)
				continue
			}
			items = append(items, it)
		}
	}

	// 2. DE-DUPLICATE by store + register + transaction + seqNumber + sku (the latest wins)
	var unique []posItem
	position := map[string]int{}
	for _, it := range items {
		s := it.sale
		key := fmt.Sprintf("%s\x00%d\x00%d\x00%d\x00%s", it.storeID, s.Register, s.Transaction, s.SeqNumber, s.Sku)
		if j, seen := position[key]; seen {
			notSave(unique[j], itemDuplicate, fmt.Sprintf("the same sale line (store %s, register %d, transaction %d, seq %d, sku %q) is sent again at item %d of message seq %d; only the later one is saved",
				it.storeID, s.Register, s.Transaction, s.SeqNumber, s.Sku, it.position, sequence(msgs[it.msg])))
			unique[j] = it
			continue
		}
		position[key] = len(unique)
		unique = append(unique, it)
	}

	// 3. FIND + IMPORT, line by line in message order (each line = one transaction, like POST /pos)
	findStore := storeLookup(ctx, p.repo.FindStoreByExternalID)
	stopped := -1 // index in unique where a DB error stopped the batch
	for n, it := range unique {
		reason, err := p.importLine(ctx, it, findStore, out.Counts)
		if err != nil {
			log.Printf("Database error (pos queue, store %s sku %s): %v", it.storeID, it.sale.Sku, err)
			stopped = n
			break
		}
		switch {
		case reason == "":
			savedCount[it.msg]++
		case reason == posPrimaryUpcOnly:
			out.Counts["unchanged"]++
			notSave(it, itemUnchanged, reason)
		default:
			notSave(it, itemInvalid, reason)
		}
	}

	// A DB error: lines from `stopped` on were not saved. A message without saved lines is retried whole;
	// in the others, the remaining lines go to the dead-letter queue so they are never counted twice.
	if stopped >= 0 {
		for _, it := range unique[stopped:] {
			if savedCount[it.msg] == 0 {
				abandon[it.msg] = true
				continue
			}
			notSave(it, itemInvalid, "database error before this sale line was saved; resend ONLY this line (the other lines of the message are saved)")
		}
	}

	// 4. OUTCOME per message
	finish("pos", msgs, notSaved, savedCount, broken, abandon, &out)
	return out
}

// posPrimaryUpcOnly is the reason for a line whose code matched a product only by primary_upc.
const posPrimaryUpcOnly = "Code matched a product only by primary_upc: sale not recorded and not deducted (same as POST /pos)."

// importLine runs STEP 4..9 for one line. Returns "" when saved, a reason when not, or a DB error.
// A unique-key race with another worker is retried (the transaction was rolled back, so nothing is doubled).
func (p *PosProcessor) importLine(ctx context.Context, it posItem, findStore storeFinder, counts map[string]int64) (string, error) {
	store, err := findStore(it.storeID)
	if err != nil {
		return "", err
	}
	if store == nil {
		return fmt.Sprintf("Store not found for this storeId (%s) (stores.external_id).", it.storeID), nil
	}
	products, err := p.repo.FindProducts(ctx, it.sale.Sku)
	if err != nil {
		return "", err
	}
	if len(products.All) == 0 {
		return fmt.Sprintf("No active product found for this SKU / UPC / variant (%s).", it.sale.Sku), nil
	}

	var report *dto.PosReport
	for range conflictRetries {
		if report, err = p.repo.Import(ctx, store.ID, products, it.sale); !errors.Is(err, gorm.ErrDuplicatedKey) {
			break
		}
	}
	var guard *repository.PicklistGuardError
	switch {
	case errors.As(err, &guard):
		return guard.Message + " Nothing was saved.", nil
	case errors.Is(err, gorm.ErrDuplicatedKey):
		return "This sale was saved by another request at the same time, retried and still failing. Nothing was saved.", nil
	case err != nil:
		return "", err
	}

	if report.Deduction.Applied {
		counts["deducted"]++
	}
	counts["picklistsCreated"] += int64(len(report.Picklists.CreatedPicklists))
	counts["emptyPicklistsRemoved"] += int64(report.EmptyPicklistsRemoved)
	switch report.PosRecord.Action {
	case "inserted":
		counts["recorded"]++
	case "updated":
		counts["merged"]++
	default:
		return posPrimaryUpcOnly, nil
	}
	return "", nil
}
