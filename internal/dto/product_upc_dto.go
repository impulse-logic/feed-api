package dto

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
