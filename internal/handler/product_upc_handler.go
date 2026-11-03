package handler

import (
	"errors"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"gorm.io/gorm"

	"feed-api/internal/dto"
	"feed-api/internal/repository"
)

type ProductUpcHandler struct {
	repo *repository.ProductUpcRepository
}

func NewProductUpcHandler(repo *repository.ProductUpcRepository) *ProductUpcHandler {
	return &ProductUpcHandler{repo: repo}
}

// Store imports ONE product_upc feed row (old file line: UPC|SKU). Only product_upcs is written.
func (h *ProductUpcHandler) Store(c *gin.Context) {
	var req dto.ProductUpcRequest

	// 1. CLEAN INPUT + 2. VALIDATE
	// dto.FlexString trims upc and sku while decoding; missing / null / array / object become "".
	// Then the binding tags check: both required, max 255. Any failure => 422, nothing is saved.
	// (An empty UPC is rejected here; the CLI could save "" as a UPC.)
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
	upc, sku := string(req.Upc), string(req.Sku)

	// 3. SKU LENGTH RULE
	// The SKU must be 8..13 characters (dto.CheckSkuLength, shared with the queue), otherwise 422 (the CLI allowed 8..12 and silently skipped the row).
	// The SKU is used as sent: no zeros are added or removed. To disable the rule, comment out this if-block.
	if msg := dto.CheckSkuLength(sku); msg != "" {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": msg})
		return
	}

	// 4. FIND THE PRODUCT (exact sku match, status not checked)
	// Not found => 422, nothing saved: the product feed must run first.
	product, err := h.repo.FindProductBySku(c.Request.Context(), sku)
	if err != nil {
		log.Printf("Database error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to import product UPC"})
		return
	}
	if product == nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{
			"error": "Product not found for this SKU. Send the product feed first.",
			"sku":   sku,
		})
		return
	}

	// 5..6. ONE TRANSACTION: find the UPC (locked), then insert / move / keep
	report, err := h.repo.Save(c.Request.Context(), upc, product)
	if err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			// Two requests inserted the same new UPC at the same moment. A retry gives unchanged or updated.
			c.JSON(http.StatusConflict, gin.H{"error": "This UPC was saved by another request at the same time. Please retry."})
			return
		}
		log.Printf("Database error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to import product UPC"})
		return
	}

	// 7. Return what happened (spec 1.5)
	c.JSON(http.StatusOK, dto.ProductUpcResult{
		Status:           "processed",
		Upc:              upc,
		Sku:              sku,
		ProductID:        product.ID,
		ProductUpcReport: *report,
	})
}
