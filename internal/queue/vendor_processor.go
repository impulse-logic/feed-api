package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"
	"gorm.io/gorm"

	"feed-api/internal/dto"
	"feed-api/internal/repository"
)

const (
	// Vendors saved per transaction. A 2,500-item message => 3 transactions.
	upsertChunk = 1000
	// Max length of the dead-letter description (one line per invalid item).
	maxDescription = 4000
	// Max size of the invalidItems application property on a dead-lettered message.
	maxInvalidItemsJSON = 30000
)

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

// InvalidItem is one vendor that was NOT saved. The list is attached to the dead-lettered message.
type InvalidItem struct {
	Item       int             `json:"item"`
	ExternalID string          `json:"external_id"`
	Name       string          `json:"name"`
	Reason     string          `json:"reason"`
	Raw        json.RawMessage `json:"-"` // the item exactly as sent (body of the invalid-items message)
}

// Handle runs one batch, item by item:
//
//  1. parse every body (object or array) and validate every item (bad => invalid)
//  2. de-duplicate by external_id (the latest wins)
//  3. name clashes, found BEFORE saving (vendors.name is unique):
//     a. the name already belongs to another external_id in the DB      => invalid
//     b. the same name is used by two external_ids in this batch        => the first wins, others invalid
//  4. save the valid vendors in chunks of 1,000 (one transaction each)
//  5. message outcome:
//     all items saved         => complete (deleted)
//     some items invalid      => valid ones saved; a NEW message with only the invalid items is sent
//     and dead-lettered with the reasons; the original is deleted (see Resend)
//     body not JSON           => dead-lettered
//     temporary DB error      => abandon (retried; saved items come back as "unchanged")
func (p *VendorProcessor) Handle(ctx context.Context, msgs []*azservicebus.ReceivedMessage) BatchOutcome {
	out := BatchOutcome{Outcomes: make([]Outcome, len(msgs)), Counts: map[string]int64{}}
	invalid := make([][]InvalidItem, len(msgs))
	validCount := make([]int, len(msgs))
	broken := make([]bool, len(msgs))

	markInvalid := func(it vendorItem, reason string) {
		invalid[it.msg] = append(invalid[it.msg], InvalidItem{it.position, string(it.vendor.ExternalID), string(it.vendor.Name), reason, it.raw})
		validCount[it.msg]--
	}

	// 1. PARSE + VALIDATE
	var items []vendorItem
	for i, msg := range msgs {
		raws, err := splitBody(msg.Body)
		if err != nil {
			broken[i] = true
			out.Outcomes[i] = Outcome{Action: DeadLetter, Reason: "invalid message body", Description: err.Error()}
			continue
		}
		for n, raw := range raws {
			vendor, reason := parseVendor(raw)
			if reason != "" {
				invalid[i] = append(invalid[i], InvalidItem{n + 1, string(vendor.ExternalID), string(vendor.Name), reason, raw})
				continue
			}
			items = append(items, vendorItem{vendor: vendor, raw: raw, msg: i, position: n + 1})
			validCount[i]++
		}
	}

	// 2. DE-DUPLICATE by external_id (the latest wins)
	var unique []vendorItem
	position := map[string]int{}
	for _, it := range items {
		key := string(it.vendor.ExternalID)
		if j, seen := position[key]; seen {
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
				markInvalid(it, fmt.Sprintf("name %q is already used by vendor external_id %s", it.vendor.Name, usedBy))
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
			markInvalid(it, fmt.Sprintf("name %q is also used by external_id %s in the same batch", name, first))
			continue
		}
		owner[name] = string(it.vendor.ExternalID)
		kept = append(kept, it)
	}
	unique = kept

	// 4. SAVE in chunks
	for start := 0; start < len(unique) && !dbFailed; start += upsertChunk {
		chunk := unique[start:min(start+upsertChunk, len(unique))]
		dbFailed = !p.saveChunk(ctx, chunk, out.Counts, markInvalid)
	}

	// 5. OUTCOME per message
	skipped := 0
	for i, msg := range msgs {
		slices.SortFunc(invalid[i], func(a, b InvalidItem) int { return a.Item - b.Item })
		skipped += len(invalid[i])
		for _, bad := range invalid[i] {
			line := fmt.Sprintf("seq %d item %d (%s): %s", sequence(msg), bad.Item, bad.ExternalID, bad.Reason)
			out.Errors = append(out.Errors, line)
			log.Printf("Vendor queue skipped %s", line)
		}
		switch {
		case broken[i]:
			// already dead-lettered in step 1
		case dbFailed:
			out.Outcomes[i] = Outcome{Action: Abandon, Reason: "database error, will retry"}
		case len(invalid[i]) > 0:
			out.Outcomes[i] = invalidOutcome(msg, invalid[i], validCount[i])
		default:
			out.Outcomes[i] = Outcome{Action: Complete}
		}
	}
	out.Counts["skipped"] += int64(skipped)
	return out
}

// saveChunk saves one chunk in ONE transaction. A unique-name race (another writer took the name
// after step 3) => that chunk is saved one vendor at a time. Returns false on a temporary DB error.
func (p *VendorProcessor) saveChunk(ctx context.Context, chunk []vendorItem, counts map[string]int64, markInvalid func(vendorItem, string)) bool {
	result, err := p.repo.UpsertBatch(ctx, vendorsOf(chunk))
	if err == nil {
		addCounts(counts, result)
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
		case errors.Is(err, gorm.ErrDuplicatedKey):
			markInvalid(it, fmt.Sprintf("name %q is already used by another vendor", it.vendor.Name))
		default:
			log.Printf("Database error (vendor queue, external_id %s): %v", it.vendor.ExternalID, err)
			return false
		}
	}
	return true
}

// invalidOutcome handles a message whose valid items were saved but some items were invalid.
// Resend: a new message with ONLY the invalid items (exactly as sent) is sent to the queue and
// dead-lettered on receive; the original is deleted. If that send fails, the original is
// dead-lettered whole. Both carry DeadLetterReason (summary), DeadLetterErrorDescription (one line
// per item) and the invalidItems property (JSON list with the reasons).
func invalidOutcome(msg *azservicebus.ReceivedMessage, invalid []InvalidItem, valid int) Outcome {
	lines := make([]string, len(invalid))
	for i, bad := range invalid {
		lines[i] = fmt.Sprintf("item %d (%s): %s", bad.Item, bad.ExternalID, bad.Reason)
	}
	description := strings.Join(lines, "; ")
	if len(description) > maxDescription {
		description = description[:maxDescription-3] + "..."
	}

	list := invalid
	itemsJSON, _ := json.Marshal(list)
	for len(itemsJSON) > maxInvalidItemsJSON && len(list) > 1 {
		list = list[:len(list)/2]
		itemsJSON, _ = json.Marshal(list)
	}

	raws := make([]json.RawMessage, len(invalid))
	for i, bad := range invalid {
		raws[i] = bad.Raw
	}
	body, _ := json.Marshal(raws)

	return Outcome{
		Action:      DeadLetter,
		Reason:      fmt.Sprintf("%d invalid items (%d valid items were saved)", len(invalid), max(valid, 0)),
		Description: description,
		Properties:  map[string]any{"invalidItems": string(itemsJSON), "invalidItemsTotal": len(invalid)},
		Resend: &Resend{
			Body: body,
			Properties: map[string]any{
				PropInvalidItemsOnly:      true,
				PropDeadLetterReason:      fmt.Sprintf("%d invalid vendor items from message seq %d (%d valid items were saved)", len(invalid), sequence(msg), max(valid, 0)),
				PropDeadLetterDescription: description,
				"originalSequence":        sequence(msg),
				"originalMessageId":       msg.MessageID,
				"invalidItems":            string(itemsJSON),
				"invalidItemsTotal":       len(invalid),
			},
		},
	}
}

// splitBody returns the items of a body: an array => its elements, an object => itself.
func splitBody(body []byte) ([]json.RawMessage, error) {
	trimmed := bytes.TrimSpace(body)
	switch {
	case len(trimmed) > 0 && trimmed[0] == '[':
		var raws []json.RawMessage
		if err := json.Unmarshal(trimmed, &raws); err != nil {
			return nil, errors.New("body is not a valid JSON array")
		}
		return raws, nil
	case len(trimmed) > 0 && trimmed[0] == '{':
		return []json.RawMessage{trimmed}, nil
	default:
		return nil, errors.New(`body must be a JSON object {"external_id": "...", "name": "..."} or an array of them`)
	}
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
