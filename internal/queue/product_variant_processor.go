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

const productVariantExample = `{"parentSku": "...", "variantSku": "..."}`

// ProductVariantProcessor saves "product-variants" queue messages with EXACTLY the same steps as
// POST /product-variants. A body is ONE link or an ARRAY of links:
//
//	[{"parentSku": "0000000003145", "variantSku": "0020408700000"}, {"parentSku": "0000000003145", "variantSku": "4087"}]
type ProductVariantProcessor struct {
	repo *repository.ProductVariantRepository
}

func NewProductVariantProcessor(repo *repository.ProductVariantRepository) *ProductVariantProcessor {
	return &ProductVariantProcessor{repo: repo}
}

// productVariantItem is one link from one message.
type productVariantItem struct {
	parentSku, variant string
	raw                json.RawMessage
	msg                int
	position           int
}

// Handle runs one batch, link by link:
//
//  1. parse every body (object or array); CLEAN INPUT + VALIDATE (both required, max 255) => invalid
//  2. de-duplicate by parentSku + variantSku
//  3. per link: FIND THE PARENT PRODUCT by sku (must be active) => invalid, then insert the
//     (product, variant) link if it does not exist, the same repository call as the endpoint.
//     Already linked => unchanged
//  4. message outcome (see finish)
func (p *ProductVariantProcessor) Handle(ctx context.Context, msgs []*azservicebus.ReceivedMessage) BatchOutcome {
	out := BatchOutcome{Outcomes: make([]Outcome, len(msgs)), Counts: map[string]int64{
		"linked": 0, "skipped": 0, "duplicates": 0, "unchanged": 0,
	}}
	notSaved := make([][]InvalidItem, len(msgs))
	savedCount := make([]int, len(msgs))
	broken := make([]bool, len(msgs))

	notSave := func(it productVariantItem, typ, reason string) {
		notSaved[it.msg] = append(notSaved[it.msg], InvalidItem{Item: it.position, ExternalID: it.variant,
			Name: "parent " + it.parentSku, Type: typ, Reason: reason, Raw: it.raw})
	}

	// 1. PARSE + VALIDATE
	var items []productVariantItem
	for i, msg := range msgs {
		raws, err := splitBody(msg.Body, productVariantExample)
		if err != nil {
			broken[i] = true
			out.Outcomes[i] = Outcome{Action: DeadLetter, Reason: "invalid message body", Description: err.Error()}
			continue
		}
		for n, raw := range raws {
			var req dto.ProductVariantRequest
			it := productVariantItem{raw: raw, msg: i, position: n + 1}
			if err := json.Unmarshal(raw, &req); err != nil {
				notSave(it, itemInvalid, "item is not a JSON object like "+productVariantExample)
				continue
			}
			it.parentSku, it.variant = string(req.ParentSku), string(req.VariantSku)
			if reason, _ := validateItem(&req); reason != "" {
				notSave(it, itemInvalid, reason)
				continue
			}
			items = append(items, it)
		}
	}

	// 2. DE-DUPLICATE by parentSku + variantSku (the same link twice)
	var unique []productVariantItem
	position := map[[2]string]int{}
	for _, it := range items {
		key := [2]string{it.parentSku, it.variant}
		if j, seen := position[key]; seen {
			notSave(unique[j], itemDuplicate, fmt.Sprintf("parent %q, variant %q is sent again at item %d of message seq %d; the later one is used",
				it.parentSku, it.variant, it.position, sequence(msgs[it.msg])))
			unique[j] = it
			continue
		}
		position[key] = len(unique)
		unique = append(unique, it)
	}

	// 3. FIND THE PARENT + LINK, link by link (each link = one transaction, like POST /product-variants)
	findProduct := productLookup(ctx, p.repo.FindProductBySku)
	dbFailed := false
	for _, it := range unique {
		product, err := findProduct(it.parentSku)
		if err != nil {
			log.Printf("Database error (product-variants queue parent lookup): %v", err)
			dbFailed = true
			break
		}
		if product == nil {
			notSave(it, itemInvalid, fmt.Sprintf("Parent product not found for this SKU (%s).", it.parentSku))
			continue
		}
		if product.Status != "A" {
			notSave(it, itemInvalid, fmt.Sprintf("Parent product is not active (status %s).", product.Status))
			continue
		}

		report, err := p.link(ctx, product.ID, it.variant)
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			notSave(it, itemInvalid, "This link was saved by another request at the same time, retried and still failing.")
			continue
		}
		if err != nil {
			log.Printf("Database error (product-variants queue, parent %s variant %s): %v", it.parentSku, it.variant, err)
			dbFailed = true
			break
		}
		if report.Action != "inserted" {
			out.Counts["unchanged"]++
			notSave(it, itemUnchanged, "this variant is already linked to this parent product (nothing to update)")
			continue
		}
		out.Counts["linked"]++
		savedCount[it.msg]++
	}

	// 4. OUTCOME per message
	finish("product variant", msgs, notSaved, savedCount, broken, abandonAll(len(msgs), dbFailed), &out)
	return out
}

// link calls the endpoint's repository Link. Two writers inserting the same new link at once is
// retried: the next try finds it and gives "unchanged".
func (p *ProductVariantProcessor) link(ctx context.Context, productID uint, variant string) (*dto.VariantRowReport, error) {
	var err error
	for range conflictRetries {
		var report *dto.VariantRowReport
		if report, err = p.repo.Link(ctx, productID, variant); !errors.Is(err, gorm.ErrDuplicatedKey) {
			return report, err
		}
	}
	return nil, err
}
