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
	"feed-api/internal/repository"
)

// Vendors saved per transaction. A 2,500-item message => 3 transactions.
const upsertChunk = 1000

const vendorExample = `{"external_id": "...", "name": "..."}`

// VendorProcessor saves "vendor" queue messages. A body is ONE vendor or an ARRAY of vendors:
//
//	{"external_id": "1011", "name": "vendor1"}
//	[{"external_id": "H0166", "name": "KENNETH HAREWOOD"}, {"external_id": "H0167", "name": "DAWN HUNTE"}]
type VendorProcessor struct {
	repo *repository.VendorRepository
}

func NewVendorProcessor(repo *repository.VendorRepository) *VendorProcessor {
	return &VendorProcessor{repo: repo}
}

// vendorItem is one vendor from one message.
type vendorItem struct {
	vendor   dto.VendorMessage
	raw      json.RawMessage // the item exactly as sent
	msg      int             // index of its message in the batch
	position int             // 1-based position inside the message body
}

// Handle runs one batch, item by item:
//
//  1. parse every body (object or array) and validate every item (bad => invalid)
//  2. de-duplicate by external_id (the latest wins)
//  3. name clashes, found BEFORE saving (vendors.name is unique):
//     a. the name already belongs to another external_id in the DB      => invalid
//     b. the same name is used by two external_ids in this batch        => the first wins, others invalid
//  4. save the valid vendors in chunks of 1,000 (one transaction each)
//  5. message outcome (see finish): all saved => complete; otherwise only the not-saved items
//     (invalid, duplicate, unchanged) are dead-lettered with the reasons
func (p *VendorProcessor) Handle(ctx context.Context, msgs []*azservicebus.ReceivedMessage) BatchOutcome {
	out := BatchOutcome{Outcomes: make([]Outcome, len(msgs)), Counts: map[string]int64{"skipped": 0, "duplicates": 0}}
	notSaved := make([][]InvalidItem, len(msgs))
	savedCount := make([]int, len(msgs)) // items inserted or updated
	broken := make([]bool, len(msgs))

	skip := func(it vendorItem, typ, reason string) {
		notSaved[it.msg] = append(notSaved[it.msg], InvalidItem{Item: it.position, ExternalID: string(it.vendor.ExternalID),
			Name: string(it.vendor.Name), Type: typ, Reason: reason, Raw: it.raw})
		savedCount[it.msg]--
	}

	// 1. PARSE + VALIDATE
	var items []vendorItem
	for i, msg := range msgs {
		raws, err := splitBody(msg.Body, vendorExample)
		if err != nil {
			broken[i] = true
			out.Outcomes[i] = Outcome{Action: DeadLetter, Reason: "invalid message body", Description: err.Error()}
			continue
		}
		for n, raw := range raws {
			vendor, reason := parseVendor(raw)
			if reason != "" {
				notSaved[i] = append(notSaved[i], InvalidItem{Item: n + 1, ExternalID: string(vendor.ExternalID),
					Name: string(vendor.Name), Type: itemInvalid, Reason: reason, Raw: raw})
				continue
			}
			items = append(items, vendorItem{vendor: vendor, raw: raw, msg: i, position: n + 1})
			savedCount[i]++
		}
	}

	// 2. DE-DUPLICATE by external_id (the latest wins, the earlier copy goes to the dead-letter queue)
	var unique []vendorItem
	position := map[string]int{}
	for _, it := range items {
		key := string(it.vendor.ExternalID)
		if j, seen := position[key]; seen {
			skip(unique[j], itemDuplicate, fmt.Sprintf("external_id %q is sent again at item %d of message seq %d; the later one is used",
				key, it.position, sequence(msgs[it.msg])))
			unique[j] = it
			continue
		}
		position[key] = len(unique)
		unique = append(unique, it)
	}

	// 3a. NAME ALREADY USED IN THE DB by another external_id
	dbFailed := false
	if len(unique) > 0 {
		clashes, err := p.repo.FindNameClashes(ctx, vendorsOf(unique))
		if err != nil {
			log.Printf("Database error (vendor queue name check): %v", err)
			dbFailed = true
		}
		kept := unique[:0]
		for _, it := range unique {
			if usedBy, clash := clashes[string(it.vendor.ExternalID)]; clash {
				skip(it, itemInvalid, fmt.Sprintf("name %q is already used by vendor external_id %s", it.vendor.Name, usedBy))
				continue
			}
			kept = append(kept, it)
		}
		unique = kept
	}

	// 3b. SAME NAME FOR TWO external_ids IN THIS BATCH (the first one wins)
	owner := map[string]string{}
	kept := unique[:0]
	for _, it := range unique {
		name := string(it.vendor.Name)
		if first, taken := owner[name]; taken {
			skip(it, itemInvalid, fmt.Sprintf("name %q is also used by external_id %s in the same batch", name, first))
			continue
		}
		owner[name] = string(it.vendor.ExternalID)
		kept = append(kept, it)
	}
	unique = kept

	// 4. SAVE in chunks
	for start := 0; start < len(unique) && !dbFailed; start += upsertChunk {
		chunk := unique[start:min(start+upsertChunk, len(unique))]
		dbFailed = !p.saveChunk(ctx, chunk, out.Counts, skip)
	}

	// 5. OUTCOME per message
	finish("vendor", msgs, notSaved, savedCount, broken, abandonAll(len(msgs), dbFailed), &out)
	return out
}

// saveChunk saves one chunk in ONE transaction. Items that were not inserted or updated are
// marked "unchanged". A unique-name race (another writer took the name after step 3) => that chunk
// is saved one vendor at a time. Returns false on a temporary DB error.
func (p *VendorProcessor) saveChunk(ctx context.Context, chunk []vendorItem, counts map[string]int64, skip func(vendorItem, string, string)) bool {
	markUnchanged := func(it vendorItem, result *dto.VendorBatchResult) {
		if !result.Written[string(it.vendor.ExternalID)] {
			skip(it, itemUnchanged, "already in the database with the same name (nothing to update)")
		}
	}

	result, err := p.repo.UpsertBatch(ctx, vendorsOf(chunk))
	if err == nil {
		addCounts(counts, result)
		for _, it := range chunk {
			markUnchanged(it, result)
		}
		return true
	}
	if !errors.Is(err, gorm.ErrDuplicatedKey) {
		log.Printf("Database error (vendor queue chunk): %v", err)
		return false
	}
	for _, it := range chunk {
		result, err := p.repo.UpsertBatch(ctx, []dto.VendorMessage{it.vendor})
		switch {
		case err == nil:
			addCounts(counts, result)
			markUnchanged(it, result)
		case errors.Is(err, gorm.ErrDuplicatedKey):
			skip(it, itemInvalid, fmt.Sprintf("name %q is already used by another vendor", it.vendor.Name))
		default:
			log.Printf("Database error (vendor queue, external_id %s): %v", it.vendor.ExternalID, err)
			return false
		}
	}
	return true
}

// parseVendor decodes and validates one item. A non-empty reason means the item is invalid.
func parseVendor(raw []byte) (dto.VendorMessage, string) {
	var vendor dto.VendorMessage
	if err := json.Unmarshal(raw, &vendor); err != nil {
		return vendor, `item is not a JSON object like {"external_id": "...", "name": "..."}`
	}
	if err := binding.Validator.ValidateStruct(&vendor); err != nil {
		var verrs validator.ValidationErrors
		if errors.As(err, &verrs) {
			fe := verrs[0]
			field := map[string]string{"Name": "name", "ExternalID": "external_id"}[fe.Field()]
			if fe.Tag() == "max" {
				return vendor, fmt.Sprintf("%s must not be greater than %s characters", field, fe.Param())
			}
			return vendor, fmt.Sprintf("%s is required", field)
		}
		return vendor, "invalid vendor item"
	}
	return vendor, ""
}

func vendorsOf(items []vendorItem) []dto.VendorMessage {
	vendors := make([]dto.VendorMessage, len(items))
	for i, it := range items {
		vendors[i] = it.vendor
	}
	return vendors
}

func addCounts(counts map[string]int64, r *dto.VendorBatchResult) {
	counts["inserted"] += r.Inserted
	counts["updated"] += r.Updated
	counts["unchanged"] += r.Unchanged
	counts["storeLinksAdded"] += r.StoreLinksAdded
}
