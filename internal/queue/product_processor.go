package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"
	"gorm.io/gorm"

	"feed-api/internal/dto"
	"feed-api/internal/model"
	"feed-api/internal/repository"
)

const productExample = `{"sku": "...", "name": "...", "level3Key": "...", ...}`

// ProductProcessor saves "products" queue messages with EXACTLY the same steps as POST /products.
// A body is ONE product or an ARRAY of products:
//
//	[{"level1Key": "", "level2Key": "", "level3Key": "100", "level4Key": "101", "level5Key": "10101",
//	  "sku": "0003640911312", "name": "Ctry Kitch Essence Mixed 355 ml", "brand": "COUNTRY KITCHEN",
//	  "status": "0", "vendorKey": "AA01", "buyQty": 24, "retailPrice": 5.29, ...}]
type ProductProcessor struct {
	repo *repository.ProductRepository
}

func NewProductProcessor(repo *repository.ProductRepository) *ProductProcessor {
	return &ProductProcessor{repo: repo}
}

// productItem is one product from one message.
type productItem struct {
	prepared dto.PreparedProduct
	raw      json.RawMessage // the item exactly as sent
	msg      int             // index of its message in the batch
	position int             // 1-based position inside the message body
}

// Handle runs one batch, product by product:
//
//  1. parse every body (object or array); CLEAN INPUT + VALIDATE every product (bad => invalid)
//  2. STATUS, NUMBERS, innerPack, MOQ + SHIFT RULE (dto.Prepare, the endpoint's steps 3-4)
//  3. de-duplicate by sku (the latest wins)
//  4. per product: FIND THE CATEGORY (not found => invalid), then save it in ONE transaction
//     (brand, product, inactive cleanup or BOS/FOS in every store), the same repository calls as
//     the endpoint; nothing changed => unchanged
//  5. message outcome (see finish): all saved => complete; otherwise only the not-saved products
//     (invalid, duplicate, unchanged) are dead-lettered with the reasons
func (p *ProductProcessor) Handle(ctx context.Context, msgs []*azservicebus.ReceivedMessage) BatchOutcome {
	out := BatchOutcome{Outcomes: make([]Outcome, len(msgs)), Counts: map[string]int64{
		"productsInserted": 0, "productsUpdated": 0, "active": 0, "inactive": 0, "shifted": 0,
		"brandsInserted": 0, "brandsUpdated": 0, "backRoomsCreated": 0, "bosAdded": 0,
		"fosLocationsCreated": 0, "fosAdded": 0, "picklistProductsRemoved": 0,
		"skipped": 0, "duplicates": 0, "unchanged": 0,
	}}
	notSaved := make([][]InvalidItem, len(msgs))
	savedCount := make([]int, len(msgs))
	broken := make([]bool, len(msgs))

	notSave := func(msg, position int, sku, name string, raw json.RawMessage, typ, reason string) {
		notSaved[msg] = append(notSaved[msg], InvalidItem{Item: position, ExternalID: sku, Name: name, Type: typ, Reason: reason, Raw: raw})
	}
	notSaveItem := func(it productItem, typ, reason string) {
		notSave(it.msg, it.position, it.prepared.Product.Sku, it.prepared.Product.Name, it.raw, typ, reason)
	}

	// 1. PARSE + VALIDATE, 2. PREPARE
	var items []productItem
	for i, msg := range msgs {
		raws, err := splitBody(msg.Body, productExample)
		if err != nil {
			broken[i] = true
			out.Outcomes[i] = Outcome{Action: DeadLetter, Reason: "invalid message body", Description: err.Error()}
			continue
		}
		for n, raw := range raws {
			req, reason := parseProduct(raw)
			if reason != "" {
				notSave(i, n+1, string(req.Sku), string(req.Name), raw, itemInvalid, reason)
				continue
			}
			items = append(items, productItem{prepared: req.ToRow().Prepare(), raw: raw, msg: i, position: n + 1})
		}
	}

	// 3. DE-DUPLICATE by sku (the latest wins, the earlier copy goes to the dead-letter queue)
	var unique []productItem
	position := map[string]int{}
	for _, it := range items {
		sku := it.prepared.Product.Sku
		if j, seen := position[sku]; seen {
			notSaveItem(unique[j], itemDuplicate, fmt.Sprintf("sku %q is sent again at item %d of message seq %d; the later one is used",
				sku, it.position, sequence(msgs[it.msg])))
			unique[j] = it
			continue
		}
		position[sku] = len(unique)
		unique = append(unique, it)
	}

	// 4. FIND THE CATEGORY + SAVE, product by product (each product = one transaction, like POST /products)
	dbFailed := false
	categories := map[[5]string]*model.Category{} // lookups cached for this batch
	for _, it := range unique {
		levels := it.prepared.Levels
		category, cached := categories[levels]
		if !cached {
			var err error
			if category, err = p.repo.FindCategory(ctx, levels); err != nil {
				log.Printf("Database error (products queue category): %v", err)
				dbFailed = true
				break
			}
			categories[levels] = category
		}
		if category == nil {
			notSaveItem(it, itemInvalid, fmt.Sprintf("Category not found for these level keys (%s). Send the product hierarchy first.",
				strings.Join(levels[:], "/")))
			continue
		}

		product := it.prepared.Product
		product.CategoryID = category.ID
		report, err := p.importProduct(ctx, product, category, it.prepared.Moq)
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			notSaveItem(it, itemInvalid, "Conflicting row (e.g. a brand with this name under another key, or the same sku saved at the same time). Nothing was saved.")
			continue
		}
		if err != nil {
			log.Printf("Database error (products queue, sku %s): %v", product.Sku, err)
			dbFailed = true
			break
		}
		if !addProductCounts(out.Counts, report) {
			out.Counts["unchanged"]++
			notSaveItem(it, itemUnchanged, "already in the database with the same data (nothing to update)")
			continue
		}
		if product.Status == "A" {
			out.Counts["active"]++
		} else {
			out.Counts["inactive"]++
		}
		if it.prepared.Shifted {
			out.Counts["shifted"]++
		}
		savedCount[it.msg]++
	}

	// 5. OUTCOME per message
	finish("product", msgs, notSaved, savedCount, broken, abandonAll(len(msgs), dbFailed), &out)
	return out
}

// importProduct calls the endpoint's repository Import. A unique-key race with another worker is
// retried; a real conflict (e.g. the brand name is used under another key) still fails after the retries.
func (p *ProductProcessor) importProduct(ctx context.Context, product model.Product, category *model.Category, moq float64) (*dto.ProductImportReport, error) {
	var err error
	for range conflictRetries {
		var report *dto.ProductImportReport
		if report, err = p.repo.Import(ctx, product, category, moq); !errors.Is(err, gorm.ErrDuplicatedKey) {
			return report, err
		}
	}
	return nil, err
}

// addProductCounts adds what one Import did to the counts. Returns false when nothing changed.
func addProductCounts(counts map[string]int64, r *dto.ProductImportReport) bool {
	changed := false
	switch r.Product.Action {
	case "inserted":
		counts["productsInserted"]++
		changed = true
	case "updated":
		counts["productsUpdated"]++
		changed = true
	}
	if r.Brand != nil {
		switch r.Brand.Action {
		case "inserted":
			counts["brandsInserted"]++
			changed = true
		case "updated":
			counts["brandsUpdated"]++
			changed = true
		}
	}
	if c := r.InactiveCleanup; c != nil {
		counts["picklistProductsRemoved"] += c.PicklistProductsRemoved
		if c.FosProductLocationsRemoved+c.EmptyFosLocationsRemoved+c.PicklistProductsRemoved > 0 {
			changed = true
		}
	}
	if l := r.Locations; l != nil {
		counts["backRoomsCreated"] += int64(l.BackRoomsCreated)
		counts["bosAdded"] += l.BosAdded
		counts["fosLocationsCreated"] += int64(l.FosLocationsCreated)
		counts["fosAdded"] += l.FosAdded
		if int64(l.BackRoomsCreated+l.FosLocationsCreated)+l.BosAdded+l.FosAdded > 0 {
			changed = true
		}
	}
	return changed
}

// parseProduct decodes and validates one product, the same as the endpoint's ShouldBindJSON.
// A non-empty reason means the product is invalid (Laravel-style message, e.g. "The buy qty field must be a number.").
func parseProduct(raw []byte) (dto.ProductRequest, string) {
	var req dto.ProductRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return req, "item is not a JSON object like " + productExample
	}
	if err := binding.Validator.ValidateStruct(&req); err != nil {
		var verrs validator.ValidationErrors
		if errors.As(err, &verrs) {
			return req, fieldError(verrs[0])
		}
		return req, "invalid product item"
	}
	return req, ""
}
