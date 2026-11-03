package handler

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"unicode"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"gorm.io/gorm"

	"feed-api/internal/dto"
	"feed-api/internal/repository"
)

type ProductHierarchyHandler struct {
	repo *repository.ProductHierarchyRepository
}

func NewProductHierarchyHandler(repo *repository.ProductHierarchyRepository) *ProductHierarchyHandler {
	return &ProductHierarchyHandler{repo: repo}
}

func (h *ProductHierarchyHandler) Store(c *gin.Context) {
	var req dto.ProductHierarchyRequest

	// 1. NORMALIZE + VALIDATE
	// dto.FlexString trims every value and turns numbers into text while decoding;
	// missing / null fields become "". Then the binding tags check: level 3 required, max 255.
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
	// 2. JUNK CHECK + 3. SHIFT RULE (dto.Prepare, shared with the product-hierarchies queue)
	// If any value contains OLD-OLD or NULL, nothing is saved.
	// level3Key in the shift keys => L3→L2, L4→L3, L5→L4, L5="", L6="". The original level 2 is dropped.
	row, shifted, skip, shiftErr := req.ToRow().Prepare()
	if skip != "" {
		c.JSON(http.StatusOK, gin.H{"status": "skipped", "reason": skip})
		return
	}
	if shiftErr != "" {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": shiftErr})
		return
	}

	// 4. SAVE EVERYTHING IN ONE TRANSACTION (departments, parents, category, store links)
	report, err := h.repo.Save(c.Request.Context(), row)
	if err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			c.JSON(http.StatusConflict, gin.H{"error": "Conflicting row was saved at the same time. Please retry."})
			return
		}
		log.Printf("Database error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save product hierarchy"})
		return
	}

	// 5. Return what happened
	c.JSON(http.StatusOK, dto.ProductHierarchyResult{
		Status:                 "processed",
		Shifted:                shifted,
		Row:                    row,
		ProductHierarchyReport: *report,
	})
}

// validationMessages builds Laravel-style errors: {"level3Key": ["The level3 key field is required."]}.
func validationMessages(verrs validator.ValidationErrors) map[string][]string {
	errs := map[string][]string{}
	for _, fe := range verrs {
		jsonName := strings.ToLower(fe.Field()[:1]) + fe.Field()[1:] // Level3Key -> level3Key
		label := fieldLabel(fe.Field())                              // Level3Key -> level3 key

		msg := fmt.Sprintf("The %s field is invalid.", label)
		switch fe.Tag() {
		case "required":
			msg = fmt.Sprintf("The %s field is required.", label)
		case "max":
			msg = fmt.Sprintf("The %s field must not be greater than %s characters.", label, fe.Param())
		case "oneof":
			msg = fmt.Sprintf("The selected %s is invalid.", label)
		case "numeric":
			msg = fmt.Sprintf("The %s field must be a number.", label)
		case "number":
			msg = fmt.Sprintf("The %s field must be an integer.", label)
		case "datetime":
			msg = fmt.Sprintf("The %s field must match the format Y-m-d.", label)
		}
		errs[jsonName] = append(errs[jsonName], msg)
	}
	return errs
}

// firstMessage returns the first error message (in field order) for the "error" key.
func firstMessage(verrs validator.ValidationErrors, errs map[string][]string) string {
	first := verrs[0].Field()
	return errs[strings.ToLower(first[:1])+first[1:]][0]
}

// fieldLabel turns "Level3Key" into "level3 key", the same as Laravel's attribute names.
func fieldLabel(field string) string {
	var b strings.Builder
	for i, r := range field {
		if unicode.IsUpper(r) {
			if i > 0 {
				b.WriteRune(' ')
			}
			r = unicode.ToLower(r)
		}
		b.WriteRune(r)
	}
	return b.String()
}
