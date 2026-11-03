package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"

	"feed-api/internal/model"
)

// Shared by every processor: item types, the dead-letter split and the per-message outcome.

const (
	// Max length of the dead-letter description (one line per invalid item).
	maxDescription = 4000
	// Max size of the invalidItems application property on a dead-lettered message.
	maxInvalidItemsJSON = 30000
)

// Why an item was not saved (InvalidItem.Type).
const (
	itemInvalid   = "invalid"   // validation error or a conflict with existing data
	itemJunk      = "junk"      // a row the feed rules ignore (e.g. contains OLD-OLD or NULL)
	itemDuplicate = "duplicate" // the same item is sent again later; the later one is used
	itemUnchanged = "unchanged" // already in the DB with the same data, nothing to update
)

var itemTypes = []string{itemInvalid, itemJunk, itemDuplicate, itemUnchanged}

// InvalidItem is one item that was NOT inserted or updated. The list goes to the dead-letter queue.
type InvalidItem struct {
	Item       int             `json:"item"`
	ExternalID string          `json:"external_id"`
	Name       string          `json:"name"`
	Type       string          `json:"type"`
	Reason     string          `json:"reason"`
	Raw        json.RawMessage `json:"-"` // the item exactly as sent (body of the dead-letter message)
}

// finish sets the outcome of every message once all items are handled. label names the items
// ("vendor", "product hierarchy"); notSaved, saved, broken and abandon are per message.
//
//	body not JSON (broken)  => dead-lettered (outcome already set)
//	abandon (temporary DB error) => abandoned (retried; saved items come back as "unchanged")
//	some items not saved    => Resend + dead-letter (see notSavedOutcome)
//	everything saved        => complete (deleted)
func finish(label string, msgs []*azservicebus.ReceivedMessage, notSaved [][]InvalidItem, saved []int, broken, abandon []bool, out *BatchOutcome) {
	prefix := strings.ToUpper(label[:1]) + label[1:] + " queue"
	for i, msg := range msgs {
		// invalid first, then junk, duplicate, unchanged (so the important ones are never cut off)
		slices.SortFunc(notSaved[i], func(a, b InvalidItem) int {
			if ra, rb := slices.Index(itemTypes, a.Type), slices.Index(itemTypes, b.Type); ra != rb {
				return ra - rb
			}
			return a.Item - b.Item
		})
		unchanged := 0
		for _, bad := range notSaved[i] {
			line := fmt.Sprintf("seq %d item %d (%s) [%s]: %s", sequence(msg), bad.Item, bad.ExternalID, bad.Type, bad.Reason)
			out.Errors = append(out.Errors, line)
			switch bad.Type {
			case itemInvalid:
				out.Counts["skipped"]++
			case itemJunk:
				out.Counts["junk"]++
			case itemDuplicate:
				out.Counts["duplicates"]++
			default:
				unchanged++
				continue
			}
			log.Printf("%s skipped %s", prefix, line)
		}
		if unchanged > 0 {
			log.Printf("%s: seq %d has %d unchanged items (already in the DB with the same data)", prefix, sequence(msg), unchanged)
		}
		switch {
		case broken[i]:
			// already dead-lettered while parsing
		case abandon[i]:
			out.Outcomes[i] = Outcome{Action: Abandon, Reason: "database error, will retry"}
		case len(notSaved[i]) > 0:
			out.Outcomes[i] = notSavedOutcome(label, msg, notSaved[i], saved[i])
		default:
			out.Outcomes[i] = Outcome{Action: Complete}
		}
	}
}

// notSavedOutcome handles a message where some items were not inserted or updated
// (invalid, junk, duplicate or unchanged). Resend: a new message with ONLY those items (exactly as sent)
// is sent to the queue and dead-lettered on receive; the original is deleted. If that send fails,
// the original is dead-lettered whole. Both carry DeadLetterReason (summary),
// DeadLetterErrorDescription (one line per item) and the invalidItems property (JSON list).
func notSavedOutcome(label string, msg *azservicebus.ReceivedMessage, invalid []InvalidItem, saved int) Outcome {
	lines := make([]string, len(invalid))
	perType := map[string]int{}
	for i, bad := range invalid {
		lines[i] = fmt.Sprintf("item %d (%s) [%s]: %s", bad.Item, bad.ExternalID, bad.Type, bad.Reason)
		perType[bad.Type]++
	}
	var parts []string
	for _, typ := range itemTypes {
		if perType[typ] > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", perType[typ], typ))
		}
	}
	breakdown := strings.Join(parts, ", ")
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
		Reason:      fmt.Sprintf("%d items not saved (%s); %d items saved", len(invalid), breakdown, max(saved, 0)),
		Description: description,
		Properties:  map[string]any{"invalidItems": string(itemsJSON), "invalidItemsTotal": len(invalid)},
		Resend: &Resend{
			Body: body,
			Properties: map[string]any{
				PropInvalidItemsOnly:      true,
				PropDeadLetterReason:      fmt.Sprintf("%d %s items not saved from message seq %d (%s); %d items saved", len(invalid), label, sequence(msg), breakdown, max(saved, 0)),
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
// example is shown in the error, e.g. {"external_id": "...", "name": "..."}.
func splitBody(body []byte, example string) ([]json.RawMessage, error) {
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
		return nil, fmt.Errorf("body must be a JSON object %s or an array of them", example)
	}
}

// fieldError is the endpoints' Laravel-style message for one failed binding tag
// (the same text as handler.validationMessages), e.g. "The level3 key field is required.".
func fieldError(fe validator.FieldError) string {
	label := fieldLabel(fe.Field())
	switch fe.Tag() {
	case "required":
		return fmt.Sprintf("The %s field is required.", label)
	case "max":
		return fmt.Sprintf("The %s field must not be greater than %s characters.", label, fe.Param())
	case "oneof":
		return fmt.Sprintf("The selected %s is invalid.", label)
	case "numeric":
		return fmt.Sprintf("The %s field must be a number.", label)
	case "number":
		return fmt.Sprintf("The %s field must be an integer.", label)
	case "datetime":
		return fmt.Sprintf("The %s field must match the format Y-m-d.", label)
	}
	return fmt.Sprintf("The %s field is invalid.", label)
}

// fieldLabel turns "Level3Key" into "level3 key" (Laravel's attribute names).
func fieldLabel(field string) string {
	var b strings.Builder
	for i, r := range field {
		if unicode.IsUpper(r) {
			if i > 0 {
				b.WriteRune(' ')
			}
			r = unicode.ToLower(r)
		}
		b.WriteRune(r)
	}
	return b.String()
}

// abandonAll is finish's abandon list when one DB error stops the whole batch.
func abandonAll(n int, dbFailed bool) []bool {
	abandon := make([]bool, n)
	for i := range abandon {
		abandon[i] = dbFailed
	}
	return abandon
}

// validateItem runs the binding tags on a decoded item, the same as the endpoints' ShouldBindJSON.
// It returns the first message (Laravel-style) and the JSON names of the fields that failed, so the
// extra dto rules (Check) skip them, as the handlers do.
func validateItem(v any) (first string, failed map[string]bool) {
	failed = map[string]bool{}
	err := binding.Validator.ValidateStruct(v)
	if err == nil {
		return "", failed
	}
	var verrs validator.ValidationErrors
	if !errors.As(err, &verrs) {
		return "invalid item", failed
	}
	for _, fe := range verrs {
		failed[strings.ToLower(fe.Field()[:1])+fe.Field()[1:]] = true
	}
	return fieldError(verrs[0]), failed
}

// storeFinder and productFinder are the cached lookups made by storeLookup and productLookup.
type (
	storeFinder   = func(storeID string) (*model.Store, error)
	productFinder = func(sku string) (*model.Product, error)
)

// storeLookup finds stores by external_id once per batch. find is the repository's FindStoreByExternalID.
// A storeId that is too large for a number can match no store (nil, nil).
func storeLookup(ctx context.Context, find func(context.Context, int64) (*model.Store, error)) storeFinder {
	cache := map[string]*model.Store{}
	return func(storeID string) (*model.Store, error) {
		if store, ok := cache[storeID]; ok {
			return store, nil
		}
		var store *model.Store
		if externalID, err := strconv.ParseInt(storeID, 10, 64); err == nil {
			if store, err = find(ctx, externalID); err != nil {
				return nil, err
			}
		}
		cache[storeID] = store
		return store, nil
	}
}

// productLookup finds products by sku once per batch. find is the repository's FindProductBySku.
func productLookup(ctx context.Context, find func(context.Context, string) (*model.Product, error)) productFinder {
	cache := map[string]*model.Product{}
	return func(sku string) (*model.Product, error) {
		if product, ok := cache[sku]; ok {
			return product, nil
		}
		product, err := find(ctx, sku)
		if err != nil {
			return nil, err
		}
		cache[sku] = product
		return product, nil
	}
}
