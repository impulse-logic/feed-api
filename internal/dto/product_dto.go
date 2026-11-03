package dto

import (
	"math"
	"strconv"

	"feed-api/internal/model"
)

// =============================================================================
// INPUT
// =============================================================================

// ProductRequest is one product feed row (one line of the old products file).
// FlexString does "STEP 1 — CLEAN INPUT" while decoding (trim, numbers -> text,
// missing / null / array / object -> ""). The binding tags are "STEP 2 — VALIDATE".
type ProductRequest struct {
	Level1Key      FlexString `json:"level1Key" binding:"max=255"`
	Level2Key      FlexString `json:"level2Key" binding:"max=255"`
	Level3Key      FlexString `json:"level3Key" binding:"max=255"`
	Level4Key      FlexString `json:"level4Key" binding:"max=255"`
	Level5Key      FlexString `json:"level5Key" binding:"max=255"`
	Level6Key      FlexString `json:"level6Key"` // ignored
	Sku            FlexString `json:"sku" binding:"required,max=255"`
	Name           FlexString `json:"name"`
	Brand          FlexString `json:"brand"`
	Status         FlexString `json:"status" binding:"omitempty,oneof=0 1"`
	VendorKey      FlexString `json:"vendorKey"`
	UomBuy         FlexString `json:"uomBuy"`
	BuyQty         FlexString `json:"buyQty" binding:"omitempty,numeric"`
	UomSell        FlexString `json:"uomSell"`
	InnerPack      FlexString `json:"innerPack" binding:"omitempty,numeric"`
	MasterPack     FlexString `json:"masterPack" binding:"omitempty,numeric"`
	Image          FlexString `json:"image"`
	PrimaryUpc     FlexString `json:"primaryUpc"`
	RetailPrice    FlexString `json:"retailPrice" binding:"omitempty,numeric"`
	UnitLandedCost FlexString `json:"unitLandedCost" binding:"omitempty,numeric"`
	SkuType        FlexString `json:"skuType"`
	ParentSku      FlexString `json:"parentSku"`
	QtyPerParent   FlexString `json:"qtyPerParent" binding:"omitempty,numeric"`
	ReferenceSku   FlexString `json:"referenceSku"`
	ShelfDays      FlexString `json:"shelfDays" binding:"omitempty,numeric"`
	Moq            FlexString `json:"moq" binding:"omitempty,numeric"` // store_products.moq, empty => 1
}

// ProductRow is the clean row after STEP 1 + 2 (plain strings).
type ProductRow struct {
	Levels         [5]string // level1Key..level5Key
	Sku            string
	Name           string
	Brand          string
	Status         string
	VendorKey      string
	UomBuy         string
	BuyQty         string
	UomSell        string
	InnerPack      string
	MasterPack     string
	Image          string
	PrimaryUpc     string
	RetailPrice    string
	UnitLandedCost string
	SkuType        string
	ParentSku      string
	QtyPerParent   string
	ReferenceSku   string
	ShelfDays      string
	Moq            string
}

// ToRow converts the decoded request into a plain-string row (level6Key is dropped).
func (r ProductRequest) ToRow() ProductRow {
	return ProductRow{
		Levels:         [5]string{string(r.Level1Key), string(r.Level2Key), string(r.Level3Key), string(r.Level4Key), string(r.Level5Key)},
		Sku:            string(r.Sku),
		Name:           string(r.Name),
		Brand:          string(r.Brand),
		Status:         string(r.Status),
		VendorKey:      string(r.VendorKey),
		UomBuy:         string(r.UomBuy),
		BuyQty:         string(r.BuyQty),
		UomSell:        string(r.UomSell),
		InnerPack:      string(r.InnerPack),
		MasterPack:     string(r.MasterPack),
		Image:          string(r.Image),
		PrimaryUpc:     string(r.PrimaryUpc),
		RetailPrice:    string(r.RetailPrice),
		UnitLandedCost: string(r.UnitLandedCost),
		SkuType:        string(r.SkuType),
		ParentSku:      string(r.ParentSku),
		QtyPerParent:   string(r.QtyPerParent),
		ReferenceSku:   string(r.ReferenceSku),
		ShelfDays:      string(r.ShelfDays),
		Moq:            string(r.Moq),
	}
}

// =============================================================================
// OUTPUT
// =============================================================================

// ProductImportReport is what the DB transaction did (steps 6..8).
// Brand is null when no brand was written. InactiveCleanup is set only for
// status I, Locations only for status A.
type ProductImportReport struct {
	Brand           *BrandReport           `json:"brand"`
	Product         ProductReport          `json:"product"`
	InactiveCleanup *InactiveCleanupReport `json:"inactiveCleanup"`
	Locations       *LocationsReport       `json:"locations"`
}

// ProductImportResult is the full success response.
// The embedded report's fields appear at the top level, like PHP's `[...] + $result`.
type ProductImportResult struct {
	Status        string    `json:"status"`
	Shifted       bool      `json:"shifted"`
	LevelsUsed    [5]string `json:"levelsUsed"`
	CategoryID    uint      `json:"categoryId"`
	ProductStatus string    `json:"productStatus"`
	ProductImportReport
}

// BrandReport is the result of STEP 6. Action is "inserted", "updated" or "unchanged".
// Before holds the old name, only when updated.
type BrandReport struct {
	ID         uint    `json:"id"`
	ExternalID string  `json:"external_id"`
	Action     string  `json:"action"`
	Before     *string `json:"before"`
}

// ProductReport is the result of STEP 7. Changes lists only the mapped columns that changed.
type ProductReport struct {
	ID      uint                   `json:"id"`
	Sku     string                 `json:"sku"`
	Action  string                 `json:"action"`
	Changes map[string]FieldChange `json:"changes"`
}

// InactiveCleanupReport is the result of STEP 8a.
type InactiveCleanupReport struct {
	FosRemovalEnabled          bool  `json:"fosRemovalEnabled"`
	FosProductLocationsRemoved int64 `json:"fosProductLocationsRemoved"`
	EmptyFosLocationsRemoved   int64 `json:"emptyFosLocationsRemoved"`
	PicklistProductsRemoved    int64 `json:"picklistProductsRemoved"`
}

// LocationsReport is the result of STEP 8b.
type LocationsReport struct {
	StoresChecked         int   `json:"storesChecked"`
	StoresAlreadyComplete int   `json:"storesAlreadyComplete"`
	BackRoomsCreated      int   `json:"backRoomsCreated"`
	BosAdded              int64 `json:"bosAdded"`
	FosLocationsCreated   int   `json:"fosLocationsCreated"`
	FosAdded              int64 `json:"fosAdded"`
}

// =============================================================================
// RULES (used by POST /products and the products queue)
// =============================================================================

// PreparedProduct is a validated row after STEP 3 + 4. Product has everything except CategoryID
// (STEP 5 finds it) and BrandID (the repository sets it).
type PreparedProduct struct {
	Product model.Product
	Levels  [5]string
	Shifted bool
	Moq     float64
}

// Prepare runs STEP 3 (status, numbers, innerPack, moq) and STEP 4 (shift rule).
func (r ProductRow) Prepare() PreparedProduct {
	// 3a. STATUS
	// CURRENT RULE (ENABLED): "1" => A (active); anything else, including "0" and empty => I.
	status := "I"
	if r.Status == "1" {
		status = "A"
	}
	// OLD FILE RULE (DISABLED): the old file mapped "0" => A, anything else => I.
	// Keep it commented while the external team sends 1 = active. If they keep the
	// old meaning (0 = active), replace the CURRENT RULE above with:
	//
	//	status := "I"
	//	if r.Status == "0" {
	//		status = "A"
	//	}

	// 3b. NUMBERS: empty => 0. Whole numbers drop the decimal part ("12.7" => 12).
	// 3c. innerPack: 0 or empty => 1.
	innerPack := toInt(r.InnerPack)
	if innerPack == 0 {
		innerPack = 1
	}

	// 3d. MOQ (store_products.moq, written in STEP 8b): missing or empty => 1, otherwise the number sent.
	moq := 1.0
	if r.Moq != "" {
		moq = toFloat(r.Moq)
	}

	// 4. SHIFT RULE
	// Only level3Key is checked. If it is a shift key: L1 stays, L2 = L3, L3 = L4, L4 = L5, L5 = "".
	// The original L2 is dropped (same rule as the hierarchy feed, so the keys match the category).
	levels := r.Levels
	shifted := false
	if IsShiftKey(levels[2]) {
		levels = [5]string{levels[0], levels[2], levels[3], levels[4], ""}
		shifted = true
	}

	return PreparedProduct{
		Product: model.Product{
			Sku:            r.Sku,
			Name:           r.Name,
			Brand:          r.Brand,
			Status:         status,
			VendorKey:      r.VendorKey,
			UomBuy:         r.UomBuy,
			BuyQty:         toFloat(r.BuyQty),
			UomSell:        r.UomSell,
			InnerPack:      innerPack,
			MasterPack:     toInt(r.MasterPack),
			Image:          r.Image,
			PrimaryUpc:     r.PrimaryUpc,
			RetailPrice:    toFloat(r.RetailPrice),
			UnitLandedCost: toFloat(r.UnitLandedCost),
			SkuType:        r.SkuType,
			ParentSku:      r.ParentSku,
			QtyPerParent:   toFloat(r.QtyPerParent),
			ReferenceSku:   r.ReferenceSku,
			ShelfDays:      toInt(r.ShelfDays),
		},
		Levels:  levels,
		Shifted: shifted,
		Moq:     moq,
	}
}

// toFloat: decimals, empty => 0 (the value was already validated as numeric).
func toFloat(v string) float64 {
	if v == "" {
		return 0
	}
	f, _ := strconv.ParseFloat(v, 64)
	return f
}

// toInt: whole numbers, empty => 0, decimal part dropped like PHP's (int) cast ("12.7" => 12).
func toInt(v string) int {
	return int(math.Trunc(toFloat(v)))
}
