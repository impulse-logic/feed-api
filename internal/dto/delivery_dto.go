package dto

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

// Activity-log events for skipped feed rows (same names as the CLI).
const (
	DeliveryEventStoreNotFound   = "DELIVERY_STORE_NOT_FOUND_SKIPPED"
	DeliveryEventProductNotFound = "DELIVERY_PRODUCTS_NOT_FOUND_SKIPPED"
	DeliveryEventRecordsSkipped  = "DELIVERY_FEED_RECORDS_SKIPPED"
)

// maxAsn: deliveries.asn is a Postgres integer.
const maxAsn = 2147483647

// =============================================================================
// INPUT
// =============================================================================

// DeliveryRequest is one deliveries feed row (old file line:
// Timestamp|ASN|ETA|DU|DU_Number|SKU|Description|Expected_Cases|Expected_Units|Damaged|From|Store_ID).
// FlexString does "STEP 1 — CLEAN INPUT" (trim; missing / null / array / object -> "").
// The binding tags are part of "STEP 2 — VALIDATE" (asn range and eta are checked in the handler).
// Fields are named like their JSON keys, because validation errors use the field name ("storeId").
type DeliveryRequest struct {
	Timestamp          FlexString `json:"timestamp"` // accepted, ignored
	Asn                FlexString `json:"asn" binding:"required,number"`
	Eta                FlexString `json:"eta" binding:"required"`
	DeliveryUnitCode   FlexString `json:"deliveryUnitCode" binding:"required,max=255"`
	DeliveryUnitNumber FlexString `json:"deliveryUnitNumber"` // accepted, ignored
	Sku                FlexString `json:"sku" binding:"required,max=255"`
	Description        FlexString `json:"description" binding:"max=255"`
	ExpectedCases      FlexString `json:"expectedCases" binding:"omitempty,numeric"`
	ExpectedUnits      FlexString `json:"expectedUnits" binding:"omitempty,numeric"`
	DamagedCases       FlexString `json:"damagedCases" binding:"omitempty,numeric"`
	From               FlexString `json:"from" binding:"max=255"`
	StoreId            FlexString `json:"storeId" binding:"required,number"`
}

// DeliveryRow is the converted row after STEP 3 (what the repository saves).
type DeliveryRow struct {
	Asn              int64
	Eta              time.Time // UTC
	From             string
	DeliveryUnitCode string
	Description      string
	ExpectedCases    int
	ExpectedUnits    int
	DamagedCases     int
}

// =============================================================================
// OUTPUT
// =============================================================================

// DeliveryReport is what the DB transaction did (STEP 6).
// When the delivery has already started, Blocked is true and nothing was written
// (the Blocked* fields are never sent to the client).
type DeliveryReport struct {
	Delivery        DeliveryRowReport        `json:"delivery"`
	DeliveryUnit    DeliveryUnitReport       `json:"deliveryUnit"`
	DeliveryProduct DeliveryProductRowReport `json:"deliveryProduct"`

	Blocked           bool   `json:"-"`
	BlockedDeliveryID uint   `json:"-"`
	BlockedStatus     string `json:"-"`
}

// DeliveryRowReport is the result of STEP 6a. Action is "inserted", "updated" or "unchanged".
type DeliveryRowReport struct {
	ID      uint                   `json:"id"`
	Asn     int64                  `json:"asn"`
	Action  string                 `json:"action"`
	Changes map[string]FieldChange `json:"changes"`
}

// DeliveryUnitReport is the result of STEP 6b. Action is "inserted" or "unchanged".
type DeliveryUnitReport struct {
	ID     uint   `json:"id"`
	Code   string `json:"code"`
	Action string `json:"action"`
}

// DeliveryProductRowReport is the result of STEP 6c. Action is "inserted", "updated" or "unchanged".
type DeliveryProductRowReport struct {
	ID      uint                   `json:"id"`
	Action  string                 `json:"action"`
	Changes map[string]FieldChange `json:"changes"`
}

// DeliveryResult is the full success response.
// The embedded report's fields appear at the top level, like PHP's `[...] + $result`.
type DeliveryResult struct {
	Status  string             `json:"status"`
	Store   DeliveryStoreRef   `json:"store"`
	Product DeliveryProductRef `json:"product"`
	DeliveryReport
	PostProcessing any `json:"postProcessing"`
}

type DeliveryStoreRef struct {
	ExternalID int64 `json:"externalId"`
	ID         uint  `json:"id"`
}

type DeliveryProductRef struct {
	ID  uint   `json:"id"`
	Sku string `json:"sku"`
}

// Check runs the extra rules after the binding tags (used by POST /deliveries and the deliveries queue).
// failed lists fields that already failed binding; they are not checked again.
//
//	asn: 1..2147483647        eta: a real date (see ParseTime)
//
// Then STEP 3, CONVERT VALUES (old file transform: roundUp, defaultIfFalsy 0):
// quantities round UP (2.1 => 3, -1.5 => -1), empty => 0, negatives KEPT. ETA is stored in UTC.
// deliveryUnitNumber and timestamp are accepted but never stored.
func (r DeliveryRequest) Check(failed map[string]bool) (DeliveryRow, []FieldError) {
	var errs []FieldError
	asn, asnErr := strconv.ParseInt(string(r.Asn), 10, 64)
	if !failed["asn"] && (asnErr != nil || asn < 1 || asn > maxAsn) {
		errs = append(errs, FieldError{"asn", fmt.Sprintf("The asn field must be between 1 and %d.", maxAsn)})
	}
	eta, etaOK := ParseTime(string(r.Eta))
	if !failed["eta"] && !etaOK {
		errs = append(errs, FieldError{"eta", "The eta field must be a valid date."})
	}

	roundUp := func(v FlexString) int { return int(math.Ceil(toFloat(string(v)))) }
	return DeliveryRow{
		Asn:              asn,
		Eta:              eta.UTC(),
		From:             string(r.From),
		DeliveryUnitCode: string(r.DeliveryUnitCode),
		Description:      string(r.Description),
		ExpectedCases:    roundUp(r.ExpectedCases),
		ExpectedUnits:    roundUp(r.ExpectedUnits),
		DamagedCases:     roundUp(r.DamagedCases),
	}, errs
}
