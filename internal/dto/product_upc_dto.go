package dto

import (
	"fmt"
	"unicode/utf8"
)

// STEP 3: allowed SKU length (13 so the usual 13-digit SKUs are accepted).
const (
	skuMinLength = 8
	skuMaxLength = 13
)

// =============================================================================
// INPUT
// =============================================================================

// ProductUpcRequest is one product_upc feed row (old file line: UPC|SKU).
// FlexString does "STEP 1 — CLEAN INPUT" (trim; missing / null / array / object -> "").
// Send both as JSON strings: a number with a leading zero (078933354) is invalid JSON (400).
// The binding tags are "STEP 2 — VALIDATE".
type ProductUpcRequest struct {
	Upc FlexString `json:"upc" binding:"required,max=255"`
	Sku FlexString `json:"sku" binding:"required,max=255"`
}

// CheckSkuLength is STEP 3 (used by POST /product-upcs and the product-upcs queue): the SKU must be
// 8..13 characters. Returns "" when it is fine. The SKU is used as sent: no zeros are added or removed.
func CheckSkuLength(sku string) string {
	if n := utf8.RuneCountInString(sku); n < skuMinLength || n > skuMaxLength {
		return fmt.Sprintf("SKU must be %d to %d characters (got %d).", skuMinLength, skuMaxLength, n)
	}
	return ""
}

// =============================================================================
// OUTPUT
// =============================================================================

// ProductUpcReport is what the DB transaction did (steps 5..6).
// Action is "inserted", "updated" or "unchanged". Before is set only when updated.
type ProductUpcReport struct {
	Action string     `json:"action"`
	UpcID  uint       `json:"upcId"`
	Before *UpcBefore `json:"before"`
}

// UpcBefore is the product that owned the UPC before it was moved.
type UpcBefore struct {
	ProductID uint   `json:"productId"`
	Sku       string `json:"sku"`
}

// ProductUpcResult is the full success response.
// The embedded report's fields appear at the top level, like PHP's `[...] + $result`.
type ProductUpcResult struct {
	Status    string `json:"status"`
	Upc       string `json:"upc"`
	Sku       string `json:"sku"`
	ProductID uint   `json:"productId"`
	ProductUpcReport
}
