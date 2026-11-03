package handler

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"gorm.io/gorm"

	"feed-api/internal/dto"
	"feed-api/internal/repository"
)

// appModeDefault: POS mode is DISABLED (business decision), so every request runs in default mode.
// POS mode changes STEP 6 and enables STEP 7c; both are commented out in boh_repository.go.
const appModeDefault = "default"

type BohHandler struct {
	repo *repository.BohRepository
}

func NewBohHandler(repo *repository.BohRepository) *BohHandler {
	return &BohHandler{repo: repo}
}

// Store imports ONE BOH feed row (old file line: Store_ID|Product_ID|BOH_Qty|BOH_Date).
func (h *BohHandler) Store(c *gin.Context) {
	var req dto.BohRequest

	// 1. CLEAN INPUT + 2. VALIDATE
	// dto.FlexString trims every value while decoding; missing / null / array / object become "".
	// Then the binding tags check: storeId whole number, sku required + max 255,
	// qty numeric, date exactly YYYY-MM-DD. Any failure => 422, nothing is saved.
	if err := c.ShouldBindJSON(&req); err != nil {
		var verrs validator.ValidationErrors
		if errors.As(err, &verrs) {
			errs := validationMessages(verrs)
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": firstMessage(verrs, errs), "errors": errs})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON body"})
		return
	}
	storeIDText, sku, qtyText, date := string(req.StoreId), string(req.Sku), string(req.Qty), string(req.Date)

	// 3. QUANTITY RULE (old file transform: roundUp, then negativesAsZero)
	// Round UP to a whole number (56.2 => 57, -2.5 => -2), then negative => 0.
	qty := dto.BohQty(qtyText) // shared with the boh queue

	// 4. FIND THE STORE by stores.external_id (not stores.id)
	// Not found => 422, nothing saved (the CLI silently skipped the row).
	externalID, err := strconv.ParseInt(storeIDText, 10, 64)
	if err != nil {
		// Digits only (validated), but too large for a number => no store can match.
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Store not found for this storeId (stores.external_id).", "storeId": storeIDText})
		return
	}
	store, err := h.repo.FindStoreByExternalID(c.Request.Context(), externalID)
	if err != nil {
		log.Printf("Database error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to import BOH"})
		return
	}
	if store == nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Store not found for this storeId (stores.external_id).", "storeId": storeIDText})
		return
	}

	// 5. FIND THE PRODUCT by exact sku. It must be ACTIVE (status A).
	// Not found => 422. Found but not active => 422 (the CLI also skipped inactive products).
	product, err := h.repo.FindProductBySku(c.Request.Context(), sku)
	if err != nil {
		log.Printf("Database error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to import BOH"})
		return
	}
	if product == nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Product not found for this SKU.", "sku": sku})
		return
	}
	if product.Status != "A" {
		c.JSON(http.StatusUnprocessableEntity, gin.H{
			"error": fmt.Sprintf("Product is not active (status %s). BOH is only stored for active products.", product.Status),
			"sku":   sku,
		})
		return
	}

	// 6..7. ONE TRANSACTION: store_qty, boh row, store_products.total_qty
	report, err := h.repo.Save(c.Request.Context(), store, product, qty, date)
	if err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			c.JSON(http.StatusConflict, gin.H{"error": "This count was saved by another request at the same time. Please retry."})
			return
		}
		log.Printf("Database error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to import BOH"})
		return
	}

	// 8. Return what happened (spec 1.5)
	c.JSON(http.StatusOK, dto.BohResult{
		Status:      "processed",
		AppMode:     appModeDefault,
		Store:       dto.BohStore{ExternalID: store.ExternalID, ID: store.ID, Name: store.Name},
		Product:     dto.BohProduct{ID: product.ID, Sku: product.Sku},
		Date:        date,
		QtyReceived: qtyText,
		QtyStored:   qty,
		BohReport:   *report,
	})
}
