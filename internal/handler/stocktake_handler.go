package handler

import (
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"gorm.io/gorm"

	"feed-api/internal/dto"
	"feed-api/internal/repository"
)

type StockTakeHandler struct {
	repo *repository.StockTakeRepository
}

func NewStockTakeHandler(repo *repository.StockTakeRepository) *StockTakeHandler {
	return &StockTakeHandler{repo: repo}
}

// Store imports ONE stocktake feed row (old file line: Store_ID|SKU|BOS_QTY|FOS_QTY).
// store_products is NOT updated (same as the CLI).
func (h *StockTakeHandler) Store(c *gin.Context) {
	var req dto.StockTakeRequest
	ctx := c.Request.Context()

	// 1. CLEAN INPUT + 2. VALIDATE
	// dto.FlexString trims every value while decoding; missing / null / array / object become "".
	// Then the binding tags check: storeId whole number, sku required + max 255,
	// bosQty and fosQty required + numeric ("0" is allowed). Any failure => 422, nothing is saved.
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
	storeIDText, sku := string(req.StoreId), string(req.Sku)

	// 3. CONVERT COUNTS
	// Locations (7a/7b): count < 1 => clear, otherwise round DOWN (5.7 => 5).
	// stock_takes (7c):  round UP (5.7 => 6); negatives are kept (-3 stays -3), same as the CLI.
	bosCount, fosCount := toFloat(string(req.BosQty)), toFloat(string(req.FosQty))
	counts := dto.StockTakeCounts{
		BosReceived:     string(req.BosQty),
		FosReceived:     string(req.FosQty),
		BosForLocations: countForLocations(bosCount),
		FosForLocations: countForLocations(fosCount),
		BosForStockTake: int(math.Ceil(bosCount)),
		FosForStockTake: int(math.Ceil(fosCount)),
	}

	// 4. FIND THE STORE by stores.external_id (not stores.id). Not found => 422.
	storeNotFound := gin.H{"error": "Store not found for this storeId (stores.external_id).", "storeId": storeIDText}
	externalID, err := strconv.ParseInt(storeIDText, 10, 64)
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

	// 5. FIND THE PRODUCT by exact sku. Not found => 422. Not active (status != A) => 422.
	product, err := h.repo.FindProductBySku(ctx, sku)
	if err != nil {
		h.serverError(c, err)
		return
	}
	if product == nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Product not found for this SKU.", "sku": sku})
		return
	}
	if product.Status != "A" {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": fmt.Sprintf("Product is not active (status %s).", product.Status), "sku": sku})
		return
	}

	// 6. SAFETY CHECKS (all use the INTERNAL store id). Any failure => 409, nothing is changed.

	// 6a. SYNC-UP (ENABLED: the CLI's intended rule)
	// Blocked when the product's department (category level_3_name = department name) is in
	// sync-up mode for this store. The CLI's own version never triggers (it compares the wrong keys).
	// To match the CLI exactly, comment out this whole block.
	inSyncUp, err := h.repo.IsInSyncUp(ctx, store.ID, product.CategoryID)
	if err != nil {
		h.serverError(c, err)
		return
	}
	if inSyncUp {
		c.JSON(http.StatusConflict, gin.H{"error": "Product's department is in sync-up mode in this store."})
		return
	}

	// 6b. OPEN PICKLIST (DISABLED): a count is no longer blocked by an open picklist.
	// When enabled, it blocks when the product is on any picklist of this store whose status != C.
	// To enable it again, uncomment this block.
	// onOpenPicklist, err := h.repo.IsOnOpenPicklist(ctx, store.ID, product.ID)
	// if err != nil {
	// 	h.serverError(c, err)
	// 	return
	// }
	// if onOpenPicklist {
	// 	c.JSON(http.StatusConflict, gin.H{"error": "Product is on an open picklist in this store. Complete the picklist first."})
	// 	return
	// }

	// 6c. BACK-OF-STORE LOCATION must exist in this store.
	hasBos, err := h.repo.HasBosLocation(ctx, store.ID, product.ID)
	if err != nil {
		h.serverError(c, err)
		return
	}
	if !hasBos {
		c.JSON(http.StatusConflict, gin.H{"error": "Product has no back-of-store location in this store."})
		return
	}

	// 7. ONE TRANSACTION: replace FOS (7a) and BOS (7b) location quantities, then stock_takes (7c)
	report, err := h.repo.Save(ctx, store.ID, product.ID, bosCount, fosCount, counts.BosForStockTake, counts.FosForStockTake)
	if err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			c.JSON(http.StatusConflict, gin.H{"error": "This count was saved by another request at the same time. Please retry."})
			return
		}
		h.serverError(c, err)
		return
	}

	// 8. Return what happened (spec 1.5)
	c.JSON(http.StatusOK, dto.StockTakeResult{
		Status:          "processed",
		Store:           dto.StockTakeStoreRef{ExternalID: store.ExternalID, ID: store.ID},
		Product:         dto.StockTakeProductRef{ID: product.ID, Sku: product.Sku},
		Counts:          counts,
		StockTakeReport: *report,
	})
}

// serverError logs the real error and sends a generic 500 (never the DB error text).
func (h *StockTakeHandler) serverError(c *gin.Context, err error) {
	log.Printf("Database error: %v", err)
	c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to import stocktake"})
}

// countForLocations: below 1 => 0 (the locations are cleared), otherwise rounded down.
func countForLocations(count float64) int {
	if count < 1 {
		return 0
	}
	return int(math.Floor(count))
}
