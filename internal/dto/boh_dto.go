package dto

import (
	"math"
	"strconv"
)

// =============================================================================
// INPUT
// =============================================================================

// BohRequest is one BOH feed row (old file line: Store_ID|Product_ID|BOH_Qty|BOH_Date;
// Product_ID holds the SKU). FlexString does "STEP 1 — CLEAN INPUT" (trim; missing / null /
// array / object -> ""). The binding tags are "STEP 2 — VALIDATE":
// storeId whole number, sku max 255, qty numeric, date exactly YYYY-MM-DD (a real date).
type BohRequest struct {
	StoreId FlexString `json:"storeId" binding:"required,number"` // named like the JSON key: validation errors use it ("storeId")
	Sku     FlexString `json:"sku" binding:"required,max=255"`
	Qty     FlexString `json:"qty" binding:"required,numeric"`
	Date    FlexString `json:"date" binding:"required,datetime=2006-01-02"`
}

// BohQty is STEP 3, the QUANTITY RULE (used by POST /boh and the boh queue; old file transform:
// roundUp, then negativesAsZero). Round UP to a whole number (56.2 => 57, -2.5 => -2), then negative => 0.
// qty was already validated as numeric.
func BohQty(qty string) int {
	f, _ := strconv.ParseFloat(qty, 64)
	return max(int(math.Ceil(f)), 0)
}

// =============================================================================
// OUTPUT
// =============================================================================

// BohReport is what the DB transaction did (steps 6..7).
// ActivityLog is "not POS mode" while POS mode is disabled.
type BohReport struct {
	StoreQty     int                `json:"storeQty"`
	Boh          BohRowReport       `json:"boh"`
	StoreProduct StoreProductReport `json:"storeProduct"`
	ActivityLog  any                `json:"activityLog"`
}

// BohRowReport is the result of STEP 7a. Before is set only when updated.
type BohRowReport struct {
	ID     uint       `json:"id"`
	Action string     `json:"action"`
	Before *BohBefore `json:"before"`
}

type BohBefore struct {
	Qty int `json:"qty"`
}

// StoreProductReport is the result of STEP 7b.
// Before is the old total_qty (current one when unchanged), or null when inserted.
type StoreProductReport struct {
	Action string `json:"action"`
	Before *int   `json:"before"`
}

// BohResult is the full success response.
// The embedded report's fields appear at the top level, like PHP's `[...] + $result`.
type BohResult struct {
	Status      string     `json:"status"`
	AppMode     string     `json:"appMode"`
	Store       BohStore   `json:"store"`
	Product     BohProduct `json:"product"`
	Date        string     `json:"date"`
	QtyReceived string     `json:"qtyReceived"`
	QtyStored   int        `json:"qtyStored"`
	BohReport
}

type BohStore struct {
	ExternalID int64  `json:"externalId"`
	ID         uint   `json:"id"`
	Name       string `json:"name"`
}

type BohProduct struct {
	ID  uint   `json:"id"`
	Sku string `json:"sku"`
}
