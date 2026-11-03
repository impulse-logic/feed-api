package dto

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

// =============================================================================
// INPUT
// =============================================================================

// PosRequest is one POS feed line (old file line:
// TimeStamp|Store_ID|Register|Operator|Transaction_type|Transaction|Seq_number|Sku|Qty|
// Reg_Price_each|Sold_Price_each|Is_Price_override|Discount_code).
// FlexString does "STEP 1 — CLEAN INPUT" (trim; missing / null / array / object -> "").
// The binding tags are part of "STEP 2 — VALIDATE" (timestamp, register != 0 and the
// integer ranges are checked in the handler).
// Fields are named like their JSON keys, because validation errors use the field name ("storeId").
type PosRequest struct {
	Timestamp        FlexString `json:"timestamp" binding:"required"`
	StoreId          FlexString `json:"storeId" binding:"required,number"`
	Register         FlexString `json:"register" binding:"required,number"`
	Operator         FlexString `json:"operator" binding:"omitempty,number"`
	TransactionType  FlexString `json:"transactionType" binding:"max=255"`
	Transaction      FlexString `json:"transaction" binding:"omitempty,number"`
	SeqNumber        FlexString `json:"seqNumber" binding:"omitempty,number"`
	Sku              FlexString `json:"sku" binding:"required,max=255"`
	Qty              FlexString `json:"qty" binding:"omitempty,numeric"`
	RegularPriceEach FlexString `json:"regularPriceEach" binding:"omitempty,numeric"`
	SoldPriceEach    FlexString `json:"soldPriceEach" binding:"omitempty,numeric"`
	IsPriceOverride  FlexString `json:"isPriceOverride" binding:"max=10"`
	DiscountCode     FlexString `json:"discountCode" binding:"omitempty,numeric"`
}

// PosSale holds the converted pos columns (STEP 3), saved in STEP 8.
type PosSale struct {
	TransactedAt     time.Time // UTC
	Register         int
	Operator         int
	TransactionType  string
	Transaction      int
	SeqNumber        int
	Sku              string // stored exactly as sent (can be a variant code)
	Qty              int
	RegularPriceEach float64
	SoldPriceEach    float64
	IsPriceOverride  bool
	DiscountCode     int
}

// =============================================================================
// OUTPUT
// =============================================================================

// PosReport is what the DB transaction did (steps 6..9).
type PosReport struct {
	Deduction             PosDeduction      `json:"deduction"`
	Picklists             PosPicklistReport `json:"picklists"`
	PosRecord             PosRecordReport   `json:"posRecord"`
	EmptyPicklistsRemoved int               `json:"emptyPicklistsRemoved"`
}

// PosDeduction is the result of STEP 6. Reason is set when nothing was deducted.
type PosDeduction struct {
	Applied bool      `json:"applied"`
	Reason  string    `json:"reason,omitempty"`
	Before  *PosStock `json:"before,omitempty"`
	After   *PosStock `json:"after,omitempty"`
}

type PosStock struct {
	TotalQty         int `json:"totalQty"`
	DeductionCounter int `json:"deductionCounter"`
}

// PosPicklistReport is the result of STEP 7. Every list is [] when empty.
type PosPicklistReport struct {
	Result           string            `json:"result"`
	FilteredOut      []FilteredProduct `json:"filteredOut"`
	ToppedUp         []ToppedUpItem    `json:"toppedUp"`
	CreatedPicklists []CreatedPicklist `json:"createdPicklists"`
	AddedToPicklists []AddedToPicklist `json:"addedToPicklists"`
	CountersReset    []CounterReset    `json:"countersReset"`
}

// NewPosPicklistReport returns a report whose lists print as [] instead of null.
func NewPosPicklistReport() PosPicklistReport {
	return PosPicklistReport{
		FilteredOut:      []FilteredProduct{},
		ToppedUp:         []ToppedUpItem{},
		CreatedPicklists: []CreatedPicklist{},
		AddedToPicklists: []AddedToPicklist{},
		CountersReset:    []CounterReset{},
	}
}

type FilteredProduct struct {
	ProductID uint   `json:"productId"`
	Reason    string `json:"reason"`
}

// ToppedUpItem is one NEW-picklist item topped up in 7.9. Skipped is set when it was not.
type ToppedUpItem struct {
	PicklistProductID uint   `json:"picklistProductId"`
	PicklistID        uint   `json:"picklistId"`
	ProductID         uint   `json:"productId"`
	AddedCases        int    `json:"addedCases,omitempty"`
	AddedQty          int    `json:"addedQty,omitempty"`
	Skipped           string `json:"skipped,omitempty"`
}

// CreatedPicklist is a picklist created by the engine (7.10), including overflow copies.
type CreatedPicklist struct {
	ID                uint   `json:"id"`
	Name              string `json:"name"`
	Sequence          int    `json:"sequence"`
	DepartmentID      *uint  `json:"department_id"`
	ReplenishmentType string `json:"replenishment_type"`
	TypeName          string `json:"typeName"`
}

type AddedToPicklist struct {
	PicklistProductID uint `json:"picklistProductId"`
	PicklistID        uint `json:"picklistId"`
	ProductID         uint `json:"productId"`
	Cases             int  `json:"cases"`
	Qty               int  `json:"qty"`
}

type CounterReset struct {
	ProductID   uint `json:"productId"`
	By          int  `json:"by"`
	PickedCases int  `json:"pickedCases"`
}

// PosRecordReport is the result of STEP 8. Action is "inserted", "updated" or "skipped".
type PosRecordReport struct {
	ID        uint     `json:"id,omitempty"`
	Action    string   `json:"action"`
	Reason    string   `json:"reason,omitempty"`
	QtyBefore *float64 `json:"qtyBefore,omitempty"`
	QtyAfter  *float64 `json:"qtyAfter,omitempty"`
}

// PosResult is the full success response.
// The embedded report's fields appear at the top level, like PHP's `[...] + $result`.
type PosResult struct {
	Status          string         `json:"status"`
	Store           PosStoreRef    `json:"store"`
	SaleProduct     PosSaleProduct `json:"saleProduct"`
	ProductsChecked []uint         `json:"productsChecked"`
	QtyStored       int            `json:"qtyStored"`
	PosReport
}

type PosStoreRef struct {
	ExternalID int64 `json:"externalId"`
	ID         uint  `json:"id"`
}

// PosSaleProduct is the product the sale is booked on. ID and Sku are null when
// the code matched only a primary_upc (MatchedBy "primary_upc only").
type PosSaleProduct struct {
	ID        *uint   `json:"id"`
	Sku       *string `json:"sku"`
	MatchedBy string  `json:"matchedBy"`
}

// Check runs the extra rules after the binding tags (used by POST /pos and the pos queue).
// failed lists fields that already failed binding; only the first problem per field is kept.
//
//	timestamp: a real date (see ParseTime)
//	register / operator / transaction / seqNumber: fit the integer DB columns; register must not be 0
//
// Then STEP 3, CONVERT VALUES (old file transform + import parsing):
//
//	qty: round UP (1.2 => 2), negative => 0, empty => 0
//	transaction / seqNumber: empty or 0 => 1;  operator: empty => 0
//	prices: decimals, empty => 0;  discountCode: decimal part cut (5.9 => 5)
//	isPriceOverride: "true" or "1" => true, anything else => false;  timestamp => UTC
func (r PosRequest) Check(failed map[string]bool) (PosSale, []FieldError) {
	var errs []FieldError
	add := func(field, msg string) {
		if failed[field] {
			return
		}
		failed[field] = true
		errs = append(errs, FieldError{field, msg})
	}
	if failed == nil {
		failed = map[string]bool{}
	}

	transactedAt, ok := ParseTime(string(r.Timestamp))
	if !ok {
		add("timestamp", "The timestamp field must be a valid date.")
	}
	int32Field := func(v FlexString, field, label string) int {
		if v == "" {
			return 0
		}
		n, err := strconv.ParseInt(string(v), 10, 32)
		if err != nil {
			add(field, fmt.Sprintf("The %s field must be between %d and %d.", label, math.MinInt32, math.MaxInt32))
			return 0
		}
		return int(n)
	}
	register := int32Field(r.Register, "register", "register")
	operator := int32Field(r.Operator, "operator", "operator")
	transaction := int32Field(r.Transaction, "transaction", "transaction")
	seqNumber := int32Field(r.SeqNumber, "seqNumber", "seq number")
	if r.Register != "" && register == 0 {
		add("register", "The selected register is invalid.")
	}

	qty := max(int(math.Ceil(toFloat(string(r.Qty)))), 0)
	if transaction == 0 {
		transaction = 1
	}
	if seqNumber == 0 {
		seqNumber = 1
	}
	isPriceOverride := string(r.IsPriceOverride)
	return PosSale{
		TransactedAt:     transactedAt.UTC(),
		Register:         register,
		Operator:         operator,
		TransactionType:  string(r.TransactionType),
		Transaction:      transaction,
		SeqNumber:        seqNumber,
		Sku:              string(r.Sku),
		Qty:              qty,
		RegularPriceEach: toFloat(string(r.RegularPriceEach)),
		SoldPriceEach:    toFloat(string(r.SoldPriceEach)),
		IsPriceOverride:  isPriceOverride == "true" || isPriceOverride == "1",
		DiscountCode:     int(math.Trunc(toFloat(string(r.DiscountCode)))),
	}, errs
}
