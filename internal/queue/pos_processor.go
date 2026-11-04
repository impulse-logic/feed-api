package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/jackc/pgx/v5/pgconn"
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

	// 3. FIND + IMPORT (each line = one transaction, like POST /pos)
	results := p.importAll(ctx, unique)
	var stopped []posItem // lines not saved because of a DB error
	for n, it := range unique {
		res := results[n]
		switch {
		case res.err != nil:
			stopped = append(stopped, it)
		case res.reason == "":
			savedCount[it.msg]++
			countReport(res.report, out.Counts)
		case res.reason == posPrimaryUpcOnly:
			countReport(res.report, out.Counts)
			out.Counts["unchanged"]++
			notSave(it, itemUnchanged, res.reason)
		default:
			notSave(it, itemInvalid, res.reason)
		}
	}

	// A DB error: these lines were not saved. A message without saved lines is retried whole;
	// in the others, the remaining lines go to the dead-letter queue so they are never counted twice.
	for _, it := range stopped {
		if savedCount[it.msg] == 0 {
			abandon[it.msg] = true
			continue
		}
		notSave(it, itemInvalid, "database error before this sale line was saved; resend ONLY this line (the other lines of the message are saved)")
	}

	// 4. OUTCOME per message
	finish("pos", msgs, notSaved, savedCount, broken, abandon, &out)
	return out
}

// posPrimaryUpcOnly is the reason for a line whose code matched a product only by primary_upc.
const posPrimaryUpcOnly = "Code matched a product only by primary_upc: sale not recorded and not deducted (same as POST /pos)."

// posStoreParallel is how many stores of one batch are imported at the same time. Lines of one store
// always run one by one in message order (the deduction, picklists and pos record depend on the order).
var posStoreParallel = 8

// posResult is what happened to one line: saved (reason ""), not saved (reason), or a DB error.
type posResult struct {
	reason string
	report *dto.PosReport
	err    error
}

// importAll finds the stores and products of the whole batch (one query each for the products), then
// imports the lines: stores in parallel, the lines of a store in order. A DB error stops only that
// store's remaining lines. results[n] belongs to lines[n].
func (p *PosProcessor) importAll(ctx context.Context, lines []posItem) []posResult {
	results := make([]posResult, len(lines))
	fail := func(err error) []posResult {
		log.Printf("Database error (pos queue lookups): %v", err)
		for n := range results {
			results[n].err = err
		}
		return results
	}

	findStore := storeLookup(ctx, p.repo.FindStoreByExternalID)
	codes := make([]string, len(lines))
	for n, it := range lines {
		codes[n] = it.sale.Sku
	}
	products, err := p.repo.FindProductsByCodes(ctx, codes)
	if err != nil {
		return fail(err)
	}

	byStore := map[uint][]int{} // stores.id => indexes in lines, in order
	var order []uint
	for n, it := range lines {
		store, err := findStore(it.storeID)
		switch {
		case err != nil:
			return fail(err)
		case store == nil:
			results[n].reason = fmt.Sprintf("Store not found for this storeId (%s) (stores.external_id).", it.storeID)
		case products[it.sale.Sku] == nil:
			results[n].reason = fmt.Sprintf("No active product found for this SKU / UPC / variant (%s).", it.sale.Sku)
		default:
			if byStore[store.ID] == nil {
				order = append(order, store.ID)
			}
			byStore[store.ID] = append(byStore[store.ID], n)
		}
	}

	var wg sync.WaitGroup
	limit := make(chan struct{}, posStoreParallel)
	for _, storeID := range order {
		wg.Add(1)
		limit <- struct{}{}
		go func(storeID uint, indexes []int) {
			defer func() { <-limit; wg.Done() }()
			var stopErr error
			for _, n := range indexes {
				if stopErr != nil {
					results[n].err = stopErr
					continue
				}
				it := lines[n]
				res := p.importLine(ctx, storeID, products[it.sale.Sku], it.sale)
				if res.err != nil {
					log.Printf("Database error (pos queue, store %s sku %s): %v", it.storeID, it.sale.Sku, res.err)
					stopErr = res.err
				}
				results[n] = res
			}
		}(storeID, byStore[storeID])
	}
	wg.Wait()
	return results
}

// importLine runs STEP 6..9 for one line. A unique-key race or a deadlock with another worker is
// retried (the transaction was rolled back, so nothing is doubled).
func (p *PosProcessor) importLine(ctx context.Context, storeID uint, products *repository.PosProducts, sale dto.PosSale) posResult {
	var report *dto.PosReport
	var err error
	for range conflictRetries {
		if report, err = p.repo.Import(ctx, storeID, products, sale); !errors.Is(err, gorm.ErrDuplicatedKey) && !isDeadlock(err) {
			break
		}
	}
	var guard *repository.PicklistGuardError
	switch {
	case errors.As(err, &guard):
		return posResult{reason: guard.Message + " Nothing was saved."}
	case errors.Is(err, gorm.ErrDuplicatedKey):
		return posResult{reason: "This sale was saved by another request at the same time, retried and still failing. Nothing was saved."}
	case err != nil:
		return posResult{err: err}
	}
	if report.PosRecord.Action != "inserted" && report.PosRecord.Action != "updated" {
		return posResult{reason: posPrimaryUpcOnly, report: report}
	}
	return posResult{report: report}
}

// countReport adds what one saved line did to the batch counts.
func countReport(report *dto.PosReport, counts map[string]int64) {
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
	}
}

// isDeadlock reports a Postgres deadlock (40P01) or serialization failure (40001): safe to retry.
func isDeadlock(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "40P01" || pgErr.Code == "40001")
}
