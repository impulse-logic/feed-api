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
	"feed-api/internal/repository"
)

const hierarchyExample = `{"level3Key": "...", "level3Name": "...", ...}`

// Times one row is retried when another worker inserted the same department/category at the same moment.
const conflictRetries = 3

// ProductHierarchyProcessor saves "product-hierarchies" queue messages with EXACTLY the same steps as
// POST /product-hierarchies. A body is ONE row or an ARRAY of rows:
//
//	[{"level1Key": "", "level1Name": "", "level2Key": "", "level2Name": "",
//	  "level3Key": "100", "level3Name": "GROCERY (FOOD)", "level4Key": "105", "level4Name": "OIL",
//	  "level5Key": "10505", "level5Name": "OLIVE OIL", "level6Key": ""}]
type ProductHierarchyProcessor struct {
	repo *repository.ProductHierarchyRepository
}

func NewProductHierarchyProcessor(repo *repository.ProductHierarchyRepository) *ProductHierarchyProcessor {
	return &ProductHierarchyProcessor{repo: repo}
}

// hierarchyItem is one row from one message.
type hierarchyItem struct {
	row      dto.ProductHierarchyRow // after the shift
	shifted  bool
	raw      json.RawMessage // the item exactly as sent
	msg      int             // index of its message in the batch
	position int             // 1-based position inside the message body
}

// Handle runs one batch, row by row:
//
//  1. parse every body (object or array); NORMALIZE + VALIDATE every row (bad => invalid)
//  2. JUNK CHECK (OLD-OLD / NULL => junk) and SHIFT RULE (shifted row without level 3 => invalid)
//  3. de-duplicate by the 5 level keys (the latest wins)
//  4. save each row in its own transaction (departments, parents, category, store links),
//     the same repository call as the endpoint; nothing changed => unchanged
//  5. message outcome (see finish): all saved => complete; otherwise only the not-saved rows
//     (invalid, junk, duplicate, unchanged) are dead-lettered with the reasons
func (p *ProductHierarchyProcessor) Handle(ctx context.Context, msgs []*azservicebus.ReceivedMessage) BatchOutcome {
	out := BatchOutcome{Outcomes: make([]Outcome, len(msgs)), Counts: map[string]int64{
		"rowsSaved": 0, "shifted": 0, "skipped": 0, "junk": 0, "duplicates": 0, "unchanged": 0,
		"departmentsInserted": 0, "departmentsUpdated": 0, "parentsChanged": 0,
		"categoriesInserted": 0, "categoriesUpdated": 0, "storeLinksAdded": 0,
	}}
	notSaved := make([][]InvalidItem, len(msgs))
	savedCount := make([]int, len(msgs))
	broken := make([]bool, len(msgs))

	notSave := func(msg, position int, row dto.ProductHierarchyRow, raw json.RawMessage, typ, reason string) {
		key, name := rowLabel(row)
		notSaved[msg] = append(notSaved[msg], InvalidItem{Item: position, ExternalID: key, Name: name, Type: typ, Reason: reason, Raw: raw})
	}

	// 1. PARSE + VALIDATE, 2. JUNK CHECK + SHIFT RULE
	var items []hierarchyItem
	for i, msg := range msgs {
		raws, err := splitBody(msg.Body, hierarchyExample)
		if err != nil {
			broken[i] = true
			out.Outcomes[i] = Outcome{Action: DeadLetter, Reason: "invalid message body", Description: err.Error()}
			continue
		}
		for n, raw := range raws {
			req, reason := parseHierarchy(raw)
			if reason != "" {
				notSave(i, n+1, req.ToRow(), raw, itemInvalid, reason)
				continue
			}
			row, shifted, skip, shiftErr := req.ToRow().Prepare()
			switch {
			case skip != "":
				notSave(i, n+1, req.ToRow(), raw, itemJunk, skip)
			case shiftErr != "":
				notSave(i, n+1, req.ToRow(), raw, itemInvalid, shiftErr)
			default:
				items = append(items, hierarchyItem{row: row, shifted: shifted, raw: raw, msg: i, position: n + 1})
			}
		}
	}

	// 3. DE-DUPLICATE by the 5 level keys (the latest wins, the earlier copy goes to the dead-letter queue)
	var unique []hierarchyItem
	position := map[string]int{}
	for _, it := range items {
		r := it.row
		key := strings.Join([]string{r.Level1Key, r.Level2Key, r.Level3Key, r.Level4Key, r.Level5Key}, "\x00")
		if j, seen := position[key]; seen {
			old := unique[j]
			label, _ := rowLabel(r)
			notSave(old.msg, old.position, old.row, old.raw, itemDuplicate,
				fmt.Sprintf("the same level keys (%s) are sent again at item %d of message seq %d; the later one is used",
					label, it.position, sequence(msgs[it.msg])))
			unique[j] = it
			continue
		}
		position[key] = len(unique)
		unique = append(unique, it)
	}

	// 4. SAVE row by row (each row = one transaction, the same as POST /product-hierarchies)
	dbFailed := false
	for _, it := range unique {
		report, err := p.save(ctx, it.row)
		switch {
		case errors.Is(err, gorm.ErrDuplicatedKey):
			notSave(it.msg, it.position, it.row, it.raw, itemInvalid, "conflicting row was saved at the same time (unique key), retried and still failing")
			continue
		case err != nil:
			log.Printf("Database error (product hierarchy queue): %v", err)
			dbFailed = true
		}
		if dbFailed {
			break
		}
		if !addHierarchyCounts(out.Counts, report) {
			out.Counts["unchanged"]++
			notSave(it.msg, it.position, it.row, it.raw, itemUnchanged, "already in the database with the same data (nothing to update)")
			continue
		}
		out.Counts["rowsSaved"]++
		if it.shifted {
			out.Counts["shifted"]++
		}
		savedCount[it.msg]++
	}

	// 5. OUTCOME per message
	finish("product hierarchy", msgs, notSaved, savedCount, broken, abandonAll(len(msgs), dbFailed), &out)
	return out
}

// save calls the endpoint's repository Save. A unique-key race with another worker is retried:
// the next try finds the row the other worker inserted.
func (p *ProductHierarchyProcessor) save(ctx context.Context, row dto.ProductHierarchyRow) (*dto.ProductHierarchyReport, error) {
	var err error
	for range conflictRetries {
		var report *dto.ProductHierarchyReport
		if report, err = p.repo.Save(ctx, row); !errors.Is(err, gorm.ErrDuplicatedKey) {
			return report, err
		}
	}
	return nil, err
}

// addHierarchyCounts adds what one Save did to the counts. Returns false when nothing changed.
func addHierarchyCounts(counts map[string]int64, r *dto.ProductHierarchyReport) bool {
	changed := false
	for _, d := range r.Departments {
		switch d.Action {
		case "inserted":
			counts["departmentsInserted"]++
			changed = true
		case "updated":
			counts["departmentsUpdated"]++
			changed = true
		}
	}
	if len(r.Parents) > 0 {
		counts["parentsChanged"] += int64(len(r.Parents))
		changed = true
	}
	if r.Category != nil {
		switch r.Category.Action {
		case "inserted":
			counts["categoriesInserted"]++
			changed = true
		case "updated":
			counts["categoriesUpdated"]++
			changed = true
		}
	}
	if r.StoreLinksAdded > 0 {
		counts["storeLinksAdded"] += r.StoreLinksAdded
		changed = true
	}
	return changed
}

// parseHierarchy decodes and validates one row, the same as the endpoint's ShouldBindJSON.
// A non-empty reason means the row is invalid (Laravel-style message, e.g. "The level3 key field is required.").
func parseHierarchy(raw []byte) (dto.ProductHierarchyRequest, string) {
	var req dto.ProductHierarchyRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return req, "item is not a JSON object like " + hierarchyExample
	}
	if err := binding.Validator.ValidateStruct(&req); err != nil {
		var verrs validator.ValidationErrors
		if errors.As(err, &verrs) {
			return req, fieldError(verrs[0])
		}
		return req, "invalid product hierarchy item"
	}
	return req, ""
}

// rowLabel names a row in the dead-letter list: its level keys joined by "/" and its deepest name.
// 100/105/10505, OLIVE OIL
func rowLabel(row dto.ProductHierarchyRow) (key, name string) {
	var keys []string
	for level := 1; level <= 5; level++ {
		k, n := row.Level(level)
		if k != "" {
			keys = append(keys, k)
		}
		if n != "" {
			name = n
		}
	}
	return strings.Join(keys, "/"), name
}
