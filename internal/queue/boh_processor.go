package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
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
	store, product     uint // stores.id and products.id, once found
	msg                int  // index of its message in the batch
	position           int  // 1-based position inside the message body
}

// Handle runs one batch, all counts together (a fixed number of queries per batch):
//
//  1. parse every body (object or array); CLEAN INPUT + VALIDATE every count (bad => invalid)
//  2. QUANTITY RULE (dto.BohQty: round up, negative => 0)
//  3. FIND THE STORES by external_id and THE PRODUCTS by sku (one query each); store not found,
//     product not found or not active => invalid
//  4. de-duplicate by (store, product, date), the boh row's unique key (the latest wins)
//  5. save in ONE transaction (BohRepository.SaveBatch): store_qty, boh rows, and
//     store_products.total_qty = the LAST count per store + product (the endpoint's "last count wins")
//  6. a count is saved when its boh row changed or it set a new total_qty; otherwise unchanged
//  7. message outcome (see finish): all saved => complete; otherwise only the not-saved counts
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

	// 3. FIND THE STORES + PRODUCTS (one query each for the whole batch)
	stores, products, err := p.lookup(ctx, items)
	if err != nil {
		log.Printf("Database error (boh queue lookups): %v", err)
		finish("boh", msgs, notSaved, savedCount, broken, abandonAll(len(msgs), true), &out)
		return out
	}
	var found []bohItem
	for _, it := range items {
		// A storeId too large for a number can match no store.
		externalID, _ := strconv.ParseInt(it.storeID, 10, 64)
		store := stores[externalID]
		product := products[it.sku]
		switch {
		case store == nil:
			notSave(it, itemInvalid, fmt.Sprintf("Store not found for this storeId (%s) (stores.external_id).", it.storeID))
		case product == nil:
			notSave(it, itemInvalid, fmt.Sprintf("Product not found for this SKU (%s).", it.sku))
		case product.Status != "A":
			notSave(it, itemInvalid, fmt.Sprintf("Product is not active (status %s). BOH is only stored for active products.", product.Status))
		default:
			it.store, it.product = store.ID, product.ID
			found = append(found, it)
		}
	}

	// 4. DE-DUPLICATE by (store, product, date) (the latest wins, the earlier copy goes to the dead-letter queue)
	var unique []bohItem
	position := map[string]int{}
	for _, it := range found {
		key := dto.BohKey(it.store, it.product, it.date)
		if j, seen := position[key]; seen {
			notSave(unique[j], itemDuplicate, fmt.Sprintf("store %s, sku %q, date %s is sent again at item %d of message seq %d; the later one is used",
				it.storeID, it.sku, it.date, it.position, sequence(msgs[it.msg])))
			unique[j] = it
			continue
		}
		position[key] = len(unique)
		unique = append(unique, it)
	}
	if len(unique) == 0 {
		finish("boh", msgs, notSaved, savedCount, broken, abandonAll(len(msgs), false), &out)
		return out
	}

	// 5. SAVE the whole batch in ONE transaction.
	// total_qty = the LAST count per store + product, in batch order (message, then position).
	rows := make([]dto.BohBatchRow, len(unique))
	last := map[string]int{} // store + product => index in unique of its last count
	for i, it := range unique {
		rows[i] = dto.BohBatchRow{StoreID: it.store, ProductID: it.product, Qty: it.qty, Date: it.date}
		pair := dto.StoreProductKey(it.store, it.product)
		if j, ok := last[pair]; !ok || before(unique[j], it) {
			last[pair] = i
		}
	}
	totals := make([]dto.BohTotal, 0, len(last))
	for _, i := range last {
		totals = append(totals, dto.BohTotal{StoreID: unique[i].store, ProductID: unique[i].product, TotalQty: unique[i].qty})
	}

	var result *dto.BohBatchResult
	for range conflictRetries {
		if result, err = p.repo.SaveBatch(ctx, rows, totals); !errors.Is(err, gorm.ErrDuplicatedKey) {
			break
		}
	}
	if err != nil {
		log.Printf("Database error (boh queue batch save): %v", err)
		finish("boh", msgs, notSaved, savedCount, broken, abandonAll(len(msgs), true), &out)
		return out
	}

	// 6. Which counts changed something?
	for inserted := range maps.Values(result.Boh) {
		out.Counts[pick(inserted, "bohInserted", "bohUpdated")]++
	}
	for inserted := range maps.Values(result.StoreProducts) {
		out.Counts[pick(inserted, "storeProductsInserted", "storeProductsUpdated")]++
	}
	for i, it := range unique {
		_, rowChanged := result.Boh[dto.BohKey(it.store, it.product, it.date)]
		pair := dto.StoreProductKey(it.store, it.product)
		_, totalChanged := result.StoreProducts[pair]
		if rowChanged || (totalChanged && last[pair] == i) {
			savedCount[it.msg]++
			continue
		}
		out.Counts["unchanged"]++
		notSave(it, itemUnchanged, "already in the database with the same qty for this store, sku and date (nothing to update)")
	}

	// 7. OUTCOME per message
	finish("boh", msgs, notSaved, savedCount, broken, abandonAll(len(msgs), false), &out)
	return out
}

// lookup finds every store (by external_id) and product (by sku) of the batch, one query each.
func (p *BohProcessor) lookup(ctx context.Context, items []bohItem) (map[int64]*model.Store, map[string]*model.Product, error) {
	var externalIDs []int64
	var skus []string
	seenStore, seenSku := map[int64]bool{}, map[string]bool{}
	for _, it := range items {
		if id, err := strconv.ParseInt(it.storeID, 10, 64); err == nil && !seenStore[id] {
			seenStore[id] = true
			externalIDs = append(externalIDs, id)
		}
		if !seenSku[it.sku] {
			seenSku[it.sku] = true
			skus = append(skus, it.sku)
		}
	}
	stores, err := p.repo.FindStoresByExternalIDs(ctx, externalIDs)
	if err != nil {
		return nil, nil, err
	}
	products, err := p.repo.FindProductsBySkus(ctx, skus)
	return stores, products, err
}

// before reports whether a comes before b in the batch (message order, then position in the message).
func before(a, b bohItem) bool {
	if a.msg != b.msg {
		return a.msg < b.msg
	}
	return a.position < b.position
}

func pick(cond bool, yes, no string) string {
	if cond {
		return yes
	}
	return no
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
