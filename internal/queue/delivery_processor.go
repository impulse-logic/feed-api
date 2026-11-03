package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"gorm.io/gorm"

	"feed-api/internal/dto"
	"feed-api/internal/model"
	"feed-api/internal/repository"
)

const deliveryExample = `{"asn": "...", "eta": "...", "deliveryUnitCode": "...", "sku": "...", "storeId": "...", ...}`

// DeliveryProcessor saves "deliveries" queue messages with EXACTLY the same steps as POST /deliveries.
// A body is ONE row or an ARRAY of rows:
//
//	[{"timestamp": "2024-11-26T00:00:00Z", "asn": "1", "eta": "2024-11-26T16:59:14Z", "deliveryUnitCode": "1",
//	  "deliveryUnitNumber": "1", "sku": "0006661348701", "description": "...", "expectedCases": "5",
//	  "expectedUnits": "0", "damagedCases": "0", "from": "BS03", "storeId": "112"}]
type DeliveryProcessor struct {
	repo *repository.DeliveryRepository
}

func NewDeliveryProcessor(repo *repository.DeliveryRepository) *DeliveryProcessor {
	return &DeliveryProcessor{repo: repo}
}

// deliveryItem is one row from one message.
type deliveryItem struct {
	row      dto.DeliveryRow
	storeID  string
	sku      string
	raw      json.RawMessage
	msg      int
	position int
}

// Handle runs one batch, row by row:
//
//  1. parse every body (object or array); CLEAN INPUT + VALIDATE (binding tags, then dto.Check:
//     asn 1..2147483647, eta a real date) => invalid; CONVERT VALUES (dto.Check)
//  2. de-duplicate by store + asn + deliveryUnitCode + sku (the latest wins)
//  3. per row: FIND THE STORE by external_id, FIND THE PRODUCT by sku (must be active), then save
//     delivery + pallet + delivery product in ONE transaction, the same repository calls as the
//     endpoint. A delivery that has already started is not changed (invalid). Nothing changed => unchanged
//  4. the endpoint's activity logs for skipped rows, ONE per event and store per batch
//     (store not found / product not found / delivery already started)
//  5. message outcome (see finish)
func (p *DeliveryProcessor) Handle(ctx context.Context, msgs []*azservicebus.ReceivedMessage) BatchOutcome {
	out := BatchOutcome{Outcomes: make([]Outcome, len(msgs)), Counts: map[string]int64{
		"deliveriesInserted": 0, "deliveriesUpdated": 0, "unitsInserted": 0,
		"productsInserted": 0, "productsUpdated": 0, "blocked": 0, "activityLogs": 0,
		"skipped": 0, "duplicates": 0, "unchanged": 0,
	}}
	notSaved := make([][]InvalidItem, len(msgs))
	savedCount := make([]int, len(msgs))
	broken := make([]bool, len(msgs))

	notSave := func(it deliveryItem, typ, reason string) {
		notSaved[it.msg] = append(notSaved[it.msg], InvalidItem{Item: it.position, ExternalID: it.sku,
			Name: fmt.Sprintf("store %s / asn %d / unit %s", it.storeID, it.row.Asn, it.row.DeliveryUnitCode),
			Type: typ, Reason: reason, Raw: it.raw})
	}

	// 1. PARSE + VALIDATE + CONVERT
	var items []deliveryItem
	for i, msg := range msgs {
		raws, err := splitBody(msg.Body, deliveryExample)
		if err != nil {
			broken[i] = true
			out.Outcomes[i] = Outcome{Action: DeadLetter, Reason: "invalid message body", Description: err.Error()}
			continue
		}
		for n, raw := range raws {
			var req dto.DeliveryRequest
			it := deliveryItem{raw: raw, msg: i, position: n + 1}
			if err := json.Unmarshal(raw, &req); err != nil {
				notSave(it, itemInvalid, "item is not a JSON object like "+deliveryExample)
				continue
			}
			reason, failed := validateItem(&req)
			row, extra := req.Check(failed)
			it.row, it.storeID, it.sku = row, string(req.StoreId), string(req.Sku)
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

	// 2. DE-DUPLICATE by store + asn + deliveryUnitCode + sku (the latest wins)
	var unique []deliveryItem
	position := map[string]int{}
	for _, it := range items {
		key := fmt.Sprintf("%s\x00%d\x00%s\x00%s", it.storeID, it.row.Asn, it.row.DeliveryUnitCode, it.sku)
		if j, seen := position[key]; seen {
			notSave(unique[j], itemDuplicate, fmt.Sprintf("store %s, asn %d, unit %s, sku %q is sent again at item %d of message seq %d; the later one is used",
				it.storeID, it.row.Asn, it.row.DeliveryUnitCode, it.sku, it.position, sequence(msgs[it.msg])))
			unique[j] = it
			continue
		}
		position[key] = len(unique)
		unique = append(unique, it)
	}

	// 3. FIND THE STORE + PRODUCT, then SAVE row by row (each row = one transaction, like POST /deliveries)
	findStore := storeLookup(ctx, p.repo.FindStoreByExternalID)
	findProduct := productLookup(ctx, p.repo.FindProductBySku)
	logs := newDeliveryLogs()
	dbFailed := false
	for _, it := range unique {
		store, err := findStore(it.storeID)
		if err != nil {
			log.Printf("Database error (deliveries queue store lookup): %v", err)
			dbFailed = true
			break
		}
		if store == nil {
			logs.storeNotFound(it.storeID)
			notSave(it, itemInvalid, fmt.Sprintf("Store not found for this storeId (%s) (stores.external_id). Row skipped and logged.", it.storeID))
			continue
		}

		product, err := findProduct(it.sku)
		if err != nil {
			log.Printf("Database error (deliveries queue product lookup): %v", err)
			dbFailed = true
			break
		}
		if product == nil || product.Status != "A" {
			logs.productNotFound(store, it)
			reason := fmt.Sprintf("Product not found for this SKU (%s). Row skipped and logged.", it.sku)
			if product != nil {
				reason = fmt.Sprintf("Product not found or not active (status %s). Row skipped and logged.", product.Status)
			}
			notSave(it, itemInvalid, reason)
			continue
		}

		report, err := p.save(ctx, store, product, it.row)
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			notSave(it, itemInvalid, "This row was saved by another request at the same time, retried and still failing.")
			continue
		}
		if err != nil {
			log.Printf("Database error (deliveries queue, asn %d sku %s): %v", it.row.Asn, it.sku, err)
			dbFailed = true
			break
		}
		if report.Blocked {
			out.Counts["blocked"]++
			logs.recordsSkipped(store, it)
			notSave(it, itemInvalid, fmt.Sprintf("Delivery has already started (status %s, delivery id %d). Row skipped.", report.BlockedStatus, report.BlockedDeliveryID))
			continue
		}
		if !addDeliveryCounts(out.Counts, report) {
			out.Counts["unchanged"]++
			notSave(it, itemUnchanged, "already in the database with the same data (nothing to update)")
			continue
		}
		savedCount[it.msg]++
	}

	// 4. ACTIVITY LOGS for the skipped rows (written even when the batch stopped on a DB error)
	n, err := logs.write(ctx, p.repo)
	out.Counts["activityLogs"] += int64(n)
	if err != nil {
		log.Printf("Database error (deliveries queue activity log): %v", err)
		dbFailed = true // rows are saved idempotently, so a retry is safe
	}

	// 5. OUTCOME per message
	finish("delivery", msgs, notSaved, savedCount, broken, abandonAll(len(msgs), dbFailed), &out)
	return out
}

// save calls the endpoint's repository Save. A unique-key race with another worker is retried.
func (p *DeliveryProcessor) save(ctx context.Context, store *model.Store, product *model.Product, row dto.DeliveryRow) (*dto.DeliveryReport, error) {
	var err error
	for range conflictRetries {
		var report *dto.DeliveryReport
		if report, err = p.repo.Save(ctx, store, product, row); !errors.Is(err, gorm.ErrDuplicatedKey) {
			return report, err
		}
	}
	return nil, err
}

// addDeliveryCounts adds what one Save did to the counts. Returns false when nothing changed.
func addDeliveryCounts(counts map[string]int64, r *dto.DeliveryReport) bool {
	changed := false
	switch r.Delivery.Action {
	case "inserted":
		counts["deliveriesInserted"]++
		changed = true
	case "updated":
		counts["deliveriesUpdated"]++
		changed = true
	}
	if r.DeliveryUnit.Action == "inserted" {
		counts["unitsInserted"]++
		changed = true
	}
	switch r.DeliveryProduct.Action {
	case "inserted":
		counts["productsInserted"]++
		changed = true
	case "updated":
		counts["productsUpdated"]++
		changed = true
	}
	return changed
}

// deliveryLogs collects the endpoint's activity logs for one batch: the endpoint writes one log per
// skipped row; a batch writes one per event (and store) with every skipped row in it, like the CLI.
type deliveryLogs struct {
	missingStores    []int64
	missingStoreRows int
	missingProducts  map[uint][]map[string]any // store id => products
	skipped          map[uint][]map[string]any // store id => deliveries
	order            []uint                    // store ids in first-seen order
}

func newDeliveryLogs() *deliveryLogs {
	return &deliveryLogs{missingProducts: map[uint][]map[string]any{}, skipped: map[uint][]map[string]any{}}
}

func (l *deliveryLogs) seen(storeID uint) {
	if _, a := l.missingProducts[storeID]; !a {
		if _, b := l.skipped[storeID]; !b {
			l.order = append(l.order, storeID)
		}
	}
}

func (l *deliveryLogs) storeNotFound(storeID string) {
	externalID, _ := strconv.ParseInt(storeID, 10, 64)
	l.missingStoreRows++
	for _, id := range l.missingStores {
		if id == externalID {
			return
		}
	}
	l.missingStores = append(l.missingStores, externalID)
}

func (l *deliveryLogs) productNotFound(store *model.Store, it deliveryItem) {
	l.seen(store.ID)
	externalID, _ := strconv.ParseInt(it.storeID, 10, 64)
	l.missingProducts[store.ID] = append(l.missingProducts[store.ID], map[string]any{
		"sku": it.sku, "asn": it.row.Asn, "deliveryUnit": it.row.DeliveryUnitCode, "storeId": externalID,
	})
}

func (l *deliveryLogs) recordsSkipped(store *model.Store, it deliveryItem) {
	l.seen(store.ID)
	l.skipped[store.ID] = append(l.skipped[store.ID], map[string]any{"asn": it.row.Asn, "deliveryUnitCode": it.row.DeliveryUnitCode})
}

// write saves the collected logs (same events and metadata shape as the endpoint) and returns how many.
func (l *deliveryLogs) write(ctx context.Context, repo *repository.DeliveryRepository) (int, error) {
	written := 0
	if len(l.missingStores) > 0 {
		if err := repo.LogActivity(ctx, dto.DeliveryEventStoreNotFound, nil, map[string]any{
			"stores": l.missingStores, "noOfProductsAffected": l.missingStoreRows,
		}); err != nil {
			return written, err
		}
		written++
	}
	for _, storeID := range l.order {
		id := storeID
		if products := l.missingProducts[id]; len(products) > 0 {
			if err := repo.LogActivity(ctx, dto.DeliveryEventProductNotFound, &id, map[string]any{"storeId": id, "products": products}); err != nil {
				return written, err
			}
			written++
		}
		if deliveries := l.skipped[id]; len(deliveries) > 0 {
			if err := repo.LogActivity(ctx, dto.DeliveryEventRecordsSkipped, &id, map[string]any{"storeId": id, "deliveries": deliveries}); err != nil {
				return written, err
			}
			written++
		}
	}
	return written, nil
}
