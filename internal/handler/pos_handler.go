package handler

import (
	"errors"
	"log"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"gorm.io/gorm"

	"feed-api/internal/dto"
	"feed-api/internal/repository"
)

type PosHandler struct {
	repo *repository.PosRepository
}

func NewPosHandler(repo *repository.PosRepository) *PosHandler {
	return &PosHandler{repo: repo}
}

// Store imports ONE POS sale line (TimeStamp|Store_ID|Register|Operator|Transaction_type|Transaction|
// Seq_number|Sku|Qty|Reg_Price_each|Sold_Price_each|Is_Price_override|Discount_code).
// Steps 6..9 run in one transaction; a failed picklist safety check => 409 and nothing is saved.
func (h *PosHandler) Store(c *gin.Context) {
	var req dto.PosRequest
	ctx := c.Request.Context()

	// 1. CLEAN INPUT + 2. VALIDATE
	// dto.FlexString trims every value while decoding; missing / null / array / object become "".
	// Binding tags: timestamp / storeId / register / sku required; storeId, register, operator,
	// transaction, seqNumber whole numbers; qty, prices, discountCode numeric if sent; text max 255.
	// Extra rules below: timestamp must be a real date, register must not be 0, and the integer
	// columns must fit the DB. Any failure => 422, nothing is saved (the CLI dropped bad rows silently).
	errs := map[string][]string{}
	firstError := ""
	if err := c.ShouldBindJSON(&req); err != nil {
		var verrs validator.ValidationErrors
		if !errors.As(err, &verrs) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON body"})
			return
		}
		errs = validationMessages(verrs)
		firstError = firstMessage(verrs, errs)
	}
	addError := func(field, msg string) {
		if _, failed := errs[field]; failed {
			return // keep only the first problem per field
		}
		errs[field] = []string{msg}
		if firstError == "" {
			firstError = msg
		}
	}

	// timestamp a real date, integer columns fit, register not 0 + 3. CONVERT VALUES
	// (dto.Check, shared with the pos queue):
	//   qty: round UP (1.2 => 2), negative => 0, empty => 0
	//   transaction / seqNumber: empty or 0 => 1;  operator: empty => 0
	//   prices: decimals, empty => 0;  discountCode: decimal part cut (5.9 => 5)
	//   isPriceOverride: "true" or "1" => true, anything else => false;  timestamp => UTC
	failed := map[string]bool{}
	for field := range errs {
		failed[field] = true
	}
	sale, extra := req.Check(failed)
	for _, fe := range extra {
		addError(fe.Field, fe.Message)
	}
	if len(errs) > 0 {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": firstError, "errors": errs})
		return
	}
	qty := sale.Qty

	// 4. FIND THE STORE by stores.external_id (not stores.id). Not found => 422.
	storeNotFound := gin.H{"error": "Store not found for this storeId (stores.external_id).", "storeId": string(req.StoreId)}
	externalID, err := strconv.ParseInt(string(req.StoreId), 10, 64)
	if err != nil {
		// Digits only (validated), but too large for a number => no store can match.
		c.JSON(http.StatusUnprocessableEntity, storeNotFound)
		return
	}
	store, err := h.repo.FindStoreByExternalID(ctx, externalID)
	if err != nil {
		h.serverError(c, err)
		return
	}
	if store == nil {
		c.JSON(http.StatusUnprocessableEntity, storeNotFound)
		return
	}

	// 5. FIND ACTIVE PRODUCTS by sku OR primary_upc OR variant code. None => 422.
	products, err := h.repo.FindProducts(ctx, sale.Sku)
	if err != nil {
		h.serverError(c, err)
		return
	}
	if len(products.All) == 0 {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "No active product found for this SKU / UPC / variant.", "sku": sale.Sku})
		return
	}

	// 6..9. ONE TRANSACTION: deduct stock, replenishment picklists, pos record, remove empty picklists
	report, err := h.repo.Import(ctx, store.ID, products, sale)
	if err != nil {
		var guard *repository.PicklistGuardError
		switch {
		case errors.As(err, &guard):
			c.JSON(http.StatusConflict, gin.H{"error": guard.Message + " Nothing was saved."})
		case errors.Is(err, gorm.ErrDuplicatedKey):
			c.JSON(http.StatusConflict, gin.H{"error": "This sale was saved by another request at the same time. Please retry."})
		default:
			h.serverError(c, err)
		}
		return
	}

	// 10. Return what happened (spec 1.4)
	saleProduct := dto.PosSaleProduct{MatchedBy: products.MatchedBy}
	if products.Sale != nil {
		saleProduct.ID, saleProduct.Sku = &products.Sale.ID, &products.Sale.Sku
	}
	checked := make([]uint, len(products.All))
	for i, p := range products.All {
		checked[i] = p.ID
	}
	c.JSON(http.StatusOK, dto.PosResult{
		Status:          "processed",
		Store:           dto.PosStoreRef{ExternalID: store.ExternalID, ID: store.ID},
		SaleProduct:     saleProduct,
		ProductsChecked: checked,
		QtyStored:       qty,
		PosReport:       *report,
	})
}

// serverError logs the real error and sends a generic 500 (never the DB error text).
func (h *PosHandler) serverError(c *gin.Context, err error) {
	log.Printf("Database error: %v", err)
	c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to import POS sale"})
}
