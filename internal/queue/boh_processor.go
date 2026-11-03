package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"
	"gorm.io/gorm"

	"feed-api/internal/dto"
	"feed-api/internal/model"
	"feed-api/internal/repository"
)

const bohExample = `{"storeId": "...", "sku": "...", "qty": "...", "date": "YYYY-MM-DD"}`

// BohProcessor saves "boh" queue messages with EXACTLY the same steps as POST /boh.
// A body is ONE count or an ARRAY of counts:
//
//	[{"storeId": "112", "sku": "0003640911312", "qty": "0", "date": "2022-03-01"}]
type BohProcessor struct {
	repo *repository.BohRepository
}

func NewBohProcessor(repo *repository.BohRepository) *BohProcessor {
	return &BohProcessor{repo: repo}
}

// bohItem is one count from one message.
type bohItem struct {
	storeID, sku, date string
	qty                int // after the qty rule
	raw                json.RawMessage
	msg                int // index of its message in the batch
	position           int // 1-based position inside the message body
}

// Handle runs one batch, count by count:
//
//  1. parse every body (object or array); CLEAN INPUT + VALIDATE every count (bad => invalid)
//  2. QUANTITY RULE (dto.BohQty: round up, negative => 0)
//  3. de-duplicate by (storeId, sku, date), the boh row's unique key (the latest wins)
//  4. per count: FIND THE STORE by external_id, FIND THE PRODUCT by sku (must be active), then save
//     store_qty + boh row + store_products.total_qty in ONE transaction, the same repository calls
//     as the endpoint; nothing changed => unchanged
//  5. message outcome (see finish): all saved => complete; otherwise only the not-saved counts
//     (invalid, duplicate, unchanged) are dead-lettered with the reasons
func (p *BohProcessor) Handle(ctx context.Context, msgs []*azservicebus.ReceivedMessage) BatchOutcome {
	out := BatchOutcome{Outcomes: make([]Outcome, len(msgs)), Counts: map[string]int64{
		"bohInserted": 0, "bohUpdated": 0, "storeProductsInserted": 0, "storeProductsUpdated": 0,
		"skipped": 0, "duplicates": 0, "unchanged": 0,
	}}
	notSaved := make([][]InvalidItem, len(msgs))
	savedCount := make([]int, len(msgs))
	broken := make([]bool, len(msgs))

	notSave := func(it bohItem, typ, reason string) {
		notSaved[it.msg] = append(notSaved[it.msg], InvalidItem{Item: it.position, ExternalID: it.sku,
			Name: fmt.Sprintf("store %s / %s", it.storeID, it.date), Type: typ, Reason: reason, Raw: it.raw})
	}

	// 1. PARSE + VALIDATE, 2. QUANTITY RULE
	var items []bohItem
	for i, msg := range msgs {
		raws, err := splitBody(msg.Body, bohExample)
		if err != nil {
			broken[i] = true
			out.Outcomes[i] = Outcome{Action: DeadLetter, Reason: "invalid message body", Description: err.Error()}
			continue
		}
		for n, raw := range raws {
			req, reason := parseBoh(raw)
			it := bohItem{storeID: string(req.StoreId), sku: string(req.Sku), date: string(req.Date), raw: raw, msg: i, position: n + 1}
			if reason != "" {
				notSave(it, itemInvalid, reason)
				continue
			}
			it.qty = dto.BohQty(string(req.Qty))
			items = append(items, it)
		}
	}

	// 3. DE-DUPLICATE by (storeId, sku, date) (the latest wins, the earlier copy goes to the dead-letter queue)
	var unique []bohItem
	position := map[[3]string]int{}
	for _, it := range items {
		key := [3]string{it.storeID, it.sku, it.date}
		if j, seen := position[key]; seen {
			notSave(unique[j], itemDuplicate, fmt.Sprintf("store %s, sku %q, date %s is sent again at item %d of message seq %d; the later one is used",
				it.storeID, it.sku, it.date, it.position, sequence(msgs[it.msg])))
			unique[j] = it
			continue
		}
		position[key] = len(unique)
		unique = append(unique, it)
	}

	// 4. FIND THE STORE + PRODUCT, then SAVE, count by count (each count = one transaction, like POST /boh)
	dbFailed := false
	stores := map[string]*model.Store{}     // lookups cached for this batch
	products := map[string]*model.Product{} // lookups cached for this batch
	for _, it := range unique {
		store, cached := stores[it.storeID]
		if !cached {
			// Digits only (validated), but too large for a number => no store can match.
			if externalID, err := strconv.ParseInt(it.storeID, 10, 64); err == nil {
				if store, err = p.repo.FindStoreByExternalID(ctx, externalID); err != nil {
					log.Printf("Database error (boh queue store lookup): %v", err)
					dbFailed = true
					break
				}
			}
			stores[it.storeID] = store
		}
		if store == nil {
			notSave(it, itemInvalid, fmt.Sprintf("Store not found for this storeId (%s) (stores.external_id).", it.storeID))
			continue
		}

		product, cached := products[it.sku]
		if !cached {
			var err error
			if product, err = p.repo.FindProductBySku(ctx, it.sku); err != nil {
				log.Printf("Database error (boh queue product lookup): %v", err)
				dbFailed = true
				break
			}
			products[it.sku] = product
		}
		if product == nil {
			notSave(it, itemInvalid, fmt.Sprintf("Product not found for this SKU (%s).", it.sku))
			continue
		}
		if product.Status != "A" {
			notSave(it, itemInvalid, fmt.Sprintf("Product is not active (status %s). BOH is only stored for active products.", product.Status))
			continue
		}

		report, err := p.save(ctx, store, product, it.qty, it.date)
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			notSave(it, itemInvalid, "This count was saved by another request at the same time, retried and still failing.")
			continue
		}
		if err != nil {
			log.Printf("Database error (boh queue, store %s sku %s): %v", it.storeID, it.sku, err)
			dbFailed = true
			break
		}
		changed := false
		switch report.Boh.Action {
		case "inserted":
			out.Counts["bohInserted"]++
			changed = true
		case "updated":
			out.Counts["bohUpdated"]++
			changed = true
		}
		switch report.StoreProduct.Action {
		case "inserted":
			out.Counts["storeProductsInserted"]++
			changed = true
		case "updated":
			out.Counts["storeProductsUpdated"]++
			changed = true
		}
		if !changed {
			out.Counts["unchanged"]++
			notSave(it, itemUnchanged, "already in the database with the same qty for this store, sku and date (nothing to update)")
			continue
		}
		savedCount[it.msg]++
	}

	// 5. OUTCOME per message
	finish("boh", msgs, notSaved, savedCount, broken, abandonAll(len(msgs), dbFailed), &out)
	return out
}

// save calls the endpoint's repository Save. Two writers inserting the same new count at once is
// retried: the next try finds the row.
func (p *BohProcessor) save(ctx context.Context, store *model.Store, product *model.Product, qty int, date string) (*dto.BohReport, error) {
	var err error
	for range conflictRetries {
		var report *dto.BohReport
		if report, err = p.repo.Save(ctx, store, product, qty, date); !errors.Is(err, gorm.ErrDuplicatedKey) {
			return report, err
		}
	}
	return nil, err
}

// parseBoh decodes and validates one count, the same as the endpoint's ShouldBindJSON.
// A non-empty reason means the count is invalid (Laravel-style message, e.g. "The date field must match the format Y-m-d.").
func parseBoh(raw []byte) (dto.BohRequest, string) {
	var req dto.BohRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return req, "item is not a JSON object like " + bohExample
	}
	if err := binding.Validator.ValidateStruct(&req); err != nil {
		var verrs validator.ValidationErrors
		if errors.As(err, &verrs) {
			return req, fieldError(verrs[0])
		}
		return req, "invalid boh item"
	}
	return req, ""
}
