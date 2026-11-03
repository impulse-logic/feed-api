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

const stockTakeExample = `{"storeId": "...", "sku": "...", "bosQty": "...", "fosQty": "..."}`

// StockTakeProcessor saves "stocktake" queue messages with EXACTLY the same steps as POST /stocktake.
// A body is ONE count or an ARRAY of counts:
//
//	[{"storeId": "112", "sku": "0000050147311", "bosQty": "0", "fosQty": "24"}]
type StockTakeProcessor struct {
	repo *repository.StockTakeRepository
}

func NewStockTakeProcessor(repo *repository.StockTakeRepository) *StockTakeProcessor {
	return &StockTakeProcessor{repo: repo}
}

// stockTakeItem is one count from one message.
type stockTakeItem struct {
	storeID, sku       string
	counts             dto.StockTakeCounts
	bosCount, fosCount float64
	raw                json.RawMessage
	msg                int
	position           int
}

// Handle runs one batch, count by count:
//
//  1. parse every body (object or array); CLEAN INPUT + VALIDATE => invalid; CONVERT COUNTS (dto.Counts)
//  2. de-duplicate by store + sku (the latest wins)
//  3. per count: FIND THE STORE, FIND THE PRODUCT (must be active), SAFETY CHECKS (department in
//     sync-up mode, no back-of-store location; open picklist stays DISABLED like the endpoint) => invalid,
//     then replace FOS + BOS location quantities and stock_takes in ONE transaction, the same repository
//     calls as the endpoint. Nothing changed => unchanged
//  4. message outcome (see finish)
func (p *StockTakeProcessor) Handle(ctx context.Context, msgs []*azservicebus.ReceivedMessage) BatchOutcome {
	out := BatchOutcome{Outcomes: make([]Outcome, len(msgs)), Counts: map[string]int64{
		"locationsUpdated": 0, "stockTakesInserted": 0, "stockTakesUpdated": 0,
		"skipped": 0, "duplicates": 0, "unchanged": 0,
	}}
	notSaved := make([][]InvalidItem, len(msgs))
	savedCount := make([]int, len(msgs))
	broken := make([]bool, len(msgs))

	notSave := func(it stockTakeItem, typ, reason string) {
		notSaved[it.msg] = append(notSaved[it.msg], InvalidItem{Item: it.position, ExternalID: it.sku,
			Name: "store " + it.storeID, Type: typ, Reason: reason, Raw: it.raw})
	}

	// 1. PARSE + VALIDATE + CONVERT
	var items []stockTakeItem
	for i, msg := range msgs {
		raws, err := splitBody(msg.Body, stockTakeExample)
		if err != nil {
			broken[i] = true
			out.Outcomes[i] = Outcome{Action: DeadLetter, Reason: "invalid message body", Description: err.Error()}
			continue
		}
		for n, raw := range raws {
			var req dto.StockTakeRequest
			it := stockTakeItem{raw: raw, msg: i, position: n + 1}
			if err := json.Unmarshal(raw, &req); err != nil {
				notSave(it, itemInvalid, "item is not a JSON object like "+stockTakeExample)
				continue
			}
			it.storeID, it.sku = string(req.StoreId), string(req.Sku)
			if reason, _ := validateItem(&req); reason != "" {
				notSave(it, itemInvalid, reason)
				continue
			}
			it.counts, it.bosCount, it.fosCount = req.Counts()
			items = append(items, it)
		}
	}

	// 2. DE-DUPLICATE by store + sku (the latest wins)
	var unique []stockTakeItem
	position := map[[2]string]int{}
	for _, it := range items {
		key := [2]string{it.storeID, it.sku}
		if j, seen := position[key]; seen {
			notSave(unique[j], itemDuplicate, fmt.Sprintf("store %s, sku %q is sent again at item %d of message seq %d; the later one is used",
				it.storeID, it.sku, it.position, sequence(msgs[it.msg])))
			unique[j] = it
			continue
		}
		position[key] = len(unique)
		unique = append(unique, it)
	}

	// 3. FIND + SAFETY CHECKS + SAVE, count by count (each count = one transaction, like POST /stocktake)
	findStore := storeLookup(ctx, p.repo.FindStoreByExternalID)
	findProduct := productLookup(ctx, p.repo.FindProductBySku)
	dbFailed := false
	for _, it := range unique {
		reason, err := p.check(ctx, it, findStore, findProduct)
		if err == nil && reason != "" {
			notSave(it, itemInvalid, reason)
			continue
		}
		var report *dto.StockTakeReport
		if err == nil {
			report, err = p.save(ctx, it, findStore, findProduct)
		}
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			notSave(it, itemInvalid, "This count was saved by another request at the same time, retried and still failing.")
			continue
		}
		if err != nil {
			log.Printf("Database error (stocktake queue, store %s sku %s): %v", it.storeID, it.sku, err)
			dbFailed = true
			break
		}
		if !addStockTakeCounts(out.Counts, report) {
			out.Counts["unchanged"]++
			notSave(it, itemUnchanged, "already in the database with the same counts (nothing to update)")
			continue
		}
		savedCount[it.msg]++
	}

	// 4. OUTCOME per message
	finish("stocktake", msgs, notSaved, savedCount, broken, abandonAll(len(msgs), dbFailed), &out)
	return out
}

// check runs STEP 4..6 (store, product, safety checks). A non-empty reason means the count is not saved.
func (p *StockTakeProcessor) check(ctx context.Context, it stockTakeItem, findStore storeFinder, findProduct productFinder) (string, error) {
	store, err := findStore(it.storeID)
	if err != nil || store == nil {
		return fmt.Sprintf("Store not found for this storeId (%s) (stores.external_id).", it.storeID), err
	}
	product, err := findProduct(it.sku)
	if err != nil {
		return "", err
	}
	if product == nil {
		return fmt.Sprintf("Product not found for this SKU (%s).", it.sku), nil
	}
	if product.Status != "A" {
		return fmt.Sprintf("Product is not active (status %s).", product.Status), nil
	}

	// 6a. SYNC-UP (ENABLED, same as the endpoint)
	inSyncUp, err := p.repo.IsInSyncUp(ctx, store.ID, product.CategoryID)
	if err != nil {
		return "", err
	}
	if inSyncUp {
		return "Product's department is in sync-up mode in this store.", nil
	}
	// 6b. OPEN PICKLIST (DISABLED, same as the endpoint)
	// 6c. BACK-OF-STORE LOCATION must exist in this store.
	hasBos, err := p.repo.HasBosLocation(ctx, store.ID, product.ID)
	if err != nil {
		return "", err
	}
	if !hasBos {
		return "Product has no back-of-store location in this store.", nil
	}
	return "", nil
}

// save calls the endpoint's repository Save (STEP 7). A unique-key race with another worker is retried.
func (p *StockTakeProcessor) save(ctx context.Context, it stockTakeItem, findStore storeFinder, findProduct productFinder) (*dto.StockTakeReport, error) {
	store, _ := findStore(it.storeID) // cached by check
	product, _ := findProduct(it.sku) // cached by check
	var err error
	for range conflictRetries {
		var report *dto.StockTakeReport
		report, err = p.repo.Save(ctx, store.ID, product.ID, it.bosCount, it.fosCount, it.counts.BosForStockTake, it.counts.FosForStockTake)
		if !errors.Is(err, gorm.ErrDuplicatedKey) {
			return report, err
		}
	}
	return nil, err
}

// addStockTakeCounts adds what one Save did to the counts. Returns false when nothing changed.
func addStockTakeCounts(counts map[string]int64, r *dto.StockTakeReport) bool {
	changed := false
	for _, loc := range append(append([]dto.LocationQtyReport{}, r.Fos.Locations...), r.Bos.Locations...) {
		if loc.Action != "unchanged" {
			counts["locationsUpdated"]++
			changed = true
		}
	}
	switch r.StockTake.Action {
	case "inserted":
		counts["stockTakesInserted"]++
		changed = true
	case "updated":
		counts["stockTakesUpdated"]++
		changed = true
	}
	return changed
}
