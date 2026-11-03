package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"
	"gorm.io/gorm"

	"feed-api/internal/dto"
	"feed-api/internal/model"
	"feed-api/internal/repository"
)

const productUpcExample = `{"upc": "...", "sku": "..."}`

// ProductUpcProcessor saves "product-upcs" queue messages with EXACTLY the same steps as
// POST /product-upcs. A body is ONE upc or an ARRAY of them:
//
//	[{"upc": "999000000002", "sku": "1250563665"}, {"upc": "999000000002", "sku": "078933354"}]
type ProductUpcProcessor struct {
	repo *repository.ProductUpcRepository
}

func NewProductUpcProcessor(repo *repository.ProductUpcRepository) *ProductUpcProcessor {
	return &ProductUpcProcessor{repo: repo}
}

// productUpcItem is one upc from one message.
type productUpcItem struct {
	upc, sku string
	raw      json.RawMessage // the item exactly as sent
	msg      int             // index of its message in the batch
	position int             // 1-based position inside the message body
}

// Handle runs one batch, item by item:
//
//  1. parse every body (object or array); CLEAN INPUT + VALIDATE every item (bad => invalid)
//  2. SKU LENGTH RULE (8..13, dto.CheckSkuLength) => invalid
//  3. de-duplicate by upc (a upc belongs to ONE product: the latest wins)
//  4. per item: FIND THE PRODUCT by sku (not found => invalid), then insert / move / keep the upc
//     in ONE transaction, the same repository calls as the endpoint; kept => unchanged
//  5. message outcome (see finish): all saved => complete; otherwise only the not-saved items
//     (invalid, duplicate, unchanged) are dead-lettered with the reasons
func (p *ProductUpcProcessor) Handle(ctx context.Context, msgs []*azservicebus.ReceivedMessage) BatchOutcome {
	out := BatchOutcome{Outcomes: make([]Outcome, len(msgs)), Counts: map[string]int64{
		"inserted": 0, "moved": 0, "skipped": 0, "duplicates": 0, "unchanged": 0,
	}}
	notSaved := make([][]InvalidItem, len(msgs))
	savedCount := make([]int, len(msgs))
	broken := make([]bool, len(msgs))

	notSave := func(it productUpcItem, typ, reason string) {
		notSaved[it.msg] = append(notSaved[it.msg], InvalidItem{Item: it.position, ExternalID: it.upc, Name: "sku " + it.sku,
			Type: typ, Reason: reason, Raw: it.raw})
	}

	// 1. PARSE + VALIDATE, 2. SKU LENGTH
	var items []productUpcItem
	for i, msg := range msgs {
		raws, err := splitBody(msg.Body, productUpcExample)
		if err != nil {
			broken[i] = true
			out.Outcomes[i] = Outcome{Action: DeadLetter, Reason: "invalid message body", Description: err.Error()}
			continue
		}
		for n, raw := range raws {
			req, reason := parseProductUpc(raw)
			it := productUpcItem{upc: string(req.Upc), sku: string(req.Sku), raw: raw, msg: i, position: n + 1}
			if reason == "" {
				reason = dto.CheckSkuLength(it.sku)
			}
			if reason != "" {
				notSave(it, itemInvalid, reason)
				continue
			}
			items = append(items, it)
		}
	}

	// 3. DE-DUPLICATE by upc (the latest wins, the earlier copy goes to the dead-letter queue)
	var unique []productUpcItem
	position := map[string]int{}
	for _, it := range items {
		if j, seen := position[it.upc]; seen {
			notSave(unique[j], itemDuplicate, fmt.Sprintf("upc %q is sent again at item %d of message seq %d (sku %s); the later one is used",
				it.upc, it.position, sequence(msgs[it.msg]), it.sku))
			unique[j] = it
			continue
		}
		position[it.upc] = len(unique)
		unique = append(unique, it)
	}

	// 4. FIND THE PRODUCT + SAVE, item by item (each item = one transaction, like POST /product-upcs)
	dbFailed := false
	products := map[string]*model.Product{} // lookups cached for this batch
	for _, it := range unique {
		product, cached := products[it.sku]
		if !cached {
			var err error
			if product, err = p.repo.FindProductBySku(ctx, it.sku); err != nil {
				log.Printf("Database error (product-upcs queue product lookup): %v", err)
				dbFailed = true
				break
			}
			products[it.sku] = product
		}
		if product == nil {
			notSave(it, itemInvalid, fmt.Sprintf("Product not found for this SKU (%s). Send the product feed first.", it.sku))
			continue
		}

		report, err := p.save(ctx, it.upc, product)
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			notSave(it, itemInvalid, "This UPC was saved by another request at the same time, retried and still failing.")
			continue
		}
		if err != nil {
			log.Printf("Database error (product-upcs queue, upc %s): %v", it.upc, err)
			dbFailed = true
			break
		}
		switch report.Action {
		case "inserted":
			out.Counts["inserted"]++
		case "updated":
			out.Counts["moved"]++
		default:
			out.Counts["unchanged"]++
			notSave(it, itemUnchanged, "already in the database for this sku (nothing to update)")
			continue
		}
		savedCount[it.msg]++
	}

	// 5. OUTCOME per message
	finish("product upc", msgs, notSaved, savedCount, broken, abandonAll(len(msgs), dbFailed), &out)
	return out
}

// save calls the endpoint's repository Save. Two writers inserting the same new upc at once is
// retried: the next try finds the row and gives unchanged or updated.
func (p *ProductUpcProcessor) save(ctx context.Context, upc string, product *model.Product) (*dto.ProductUpcReport, error) {
	var err error
	for range conflictRetries {
		var report *dto.ProductUpcReport
		if report, err = p.repo.Save(ctx, upc, product); !errors.Is(err, gorm.ErrDuplicatedKey) {
			return report, err
		}
	}
	return nil, err
}

// parseProductUpc decodes and validates one item, the same as the endpoint's ShouldBindJSON.
// A non-empty reason means the item is invalid (Laravel-style message, e.g. "The upc field is required.").
func parseProductUpc(raw []byte) (dto.ProductUpcRequest, string) {
	var req dto.ProductUpcRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return req, "item is not a JSON object like " + productUpcExample
	}
	if err := binding.Validator.ValidateStruct(&req); err != nil {
		var verrs validator.ValidationErrors
		if errors.As(err, &verrs) {
			return req, fieldError(verrs[0])
		}
		return req, "invalid product upc item"
	}
	return req, ""
}
