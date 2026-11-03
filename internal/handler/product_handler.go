package handler

import (
	"errors"
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

type ProductHandler struct {
	repo *repository.ProductRepository
}

func NewProductHandler(repo *repository.ProductRepository) *ProductHandler {
	return &ProductHandler{repo: repo}
}

// Store imports ONE product feed row (spec 1.4, steps 1..8).
func (h *ProductHandler) Store(c *gin.Context) {
	var req dto.ProductRequest

	// 1. CLEAN INPUT + 2. VALIDATE
	// dto.FlexString trims every value and turns numbers into text while decoding;
	// missing / null / array / object become "". Then the binding tags check:
	// sku required + max 255, status empty or 0/1, the 7 number fields + moq numeric, level keys max 255.
	// Any failure => 422, nothing is saved.
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
	// 3. STATUS, NUMBERS, innerPack, MOQ + 4. SHIFT RULE (dto.Prepare, shared with the products queue)
	// status "1" => A, anything else => I. Empty numbers => 0, whole numbers drop decimals.
	// innerPack 0 => 1. moq empty => 1. level3Key in the shift keys => L2 = L3, L3 = L4, L4 = L5, L5 = "".
	prepared := req.ToRow().Prepare()
	levels, shifted, status := prepared.Levels, prepared.Shifted, prepared.Product.Status

	// 5. FIND THE CATEGORY (all 5 keys must match exactly)
	// Not found => 422, nothing saved: the hierarchy feed must run first.
	category, err := h.repo.FindCategory(c.Request.Context(), levels)
	if err != nil {
		log.Printf("Database error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to import product"})
		return
	}
	if category == nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{
			"error":      "Category not found for these level keys. Send the product hierarchy first.",
			"levelsUsed": levels,
			"shifted":    shifted,
		})
		return
	}

	// 6..8. SAVE IN ONE TRANSACTION (brand, product, inactive cleanup or BOS/FOS locations)
	// brand_id and the fixed values are set by the repository.
	product := prepared.Product
	product.CategoryID = category.ID

	report, err := h.repo.Import(c.Request.Context(), product, category, prepared.Moq)
	if err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			c.JSON(http.StatusConflict, gin.H{"error": "Conflicting row (e.g. a brand with this name under another key, or the same sku saved at the same time). Nothing was saved."})
			return
		}
		log.Printf("Database error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to import product"})
		return
	}

	// 9. Return what happened (spec 1.5)
	c.JSON(http.StatusOK, dto.ProductImportResult{
		Status:              "processed",
		Shifted:             shifted,
		LevelsUsed:          levels,
		CategoryID:          category.ID,
		ProductStatus:       status,
		ProductImportReport: *report,
	})
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
