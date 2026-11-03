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
	"feed-api/internal/model"
	"feed-api/internal/repository"
)

type DeliveryHandler struct {
	repo *repository.DeliveryRepository
}

func NewDeliveryHandler(repo *repository.DeliveryRepository) *DeliveryHandler {
	return &DeliveryHandler{repo: repo}
}

// Store imports ONE deliveries feed row
// (Timestamp|ASN|ETA|DU|DU_Number|SKU|Description|Expected_Cases|Expected_Units|Damaged|From|Store_ID).
func (h *DeliveryHandler) Store(c *gin.Context) {
	var req dto.DeliveryRequest
	ctx := c.Request.Context()

	// 1. CLEAN INPUT + 2. VALIDATE
	// dto.FlexString trims every value while decoding; missing / null / array / object become "".
	// Binding tags: storeId and asn whole numbers, eta / deliveryUnitCode / sku required,
	// text max 255, quantities numeric if sent. Then two extra rules below: asn 1..2147483647
	// and eta must be a real date. Any failure => 422, nothing saved, NO activity log.
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
		errs[field] = append(errs[field], msg)
		if firstError == "" {
			firstError = msg
		}
	}

	// asn 1..2147483647 and eta a real date + 3. CONVERT VALUES (dto.Check, shared with the deliveries queue):
	// quantities round UP (2.1 => 3, -1.5 => -1), empty => 0, negatives KEPT. ETA stored in UTC.
	failed := map[string]bool{}
	for field := range errs {
		failed[field] = true
	}
	row, extra := req.Check(failed)
	for _, fe := range extra {
		addError(fe.Field, fe.Message)
	}
	if len(errs) > 0 {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": firstError, "errors": errs})
		return
	}

	asn, sku := row.Asn, string(req.Sku)

	// 4. FIND THE STORE by stores.external_id (the feed's Store_ID)
	// Not found => activity log (store_id NULL) + 422.
	externalStoreID, err := strconv.ParseInt(string(req.StoreId), 10, 64)
	var store *model.Store
	if err == nil {
		store, err = h.repo.FindStoreByExternalID(ctx, externalStoreID)
		if err != nil {
			h.serverError(c, err)
			return
		}
	}
	if store == nil {
		if err := h.repo.LogActivity(ctx, dto.DeliveryEventStoreNotFound, nil, map[string]any{
			"stores":               []int64{externalStoreID},
			"noOfProductsAffected": 1,
		}); err != nil {
			h.serverError(c, err)
			return
		}
		c.JSON(http.StatusUnprocessableEntity, gin.H{
			"error":   "Store not found for this storeId (stores.external_id). Row skipped and logged.",
			"storeId": externalStoreID,
		})
		return
	}

	// 5. FIND THE PRODUCT by exact sku. It must exist AND be ACTIVE (status A).
	// Otherwise => activity log (store_id = internal id) + 422.
	product, err := h.repo.FindProductBySku(ctx, sku)
	if err != nil {
		h.serverError(c, err)
		return
	}
	if product == nil || product.Status != "A" {
		if err := h.repo.LogActivity(ctx, dto.DeliveryEventProductNotFound, &store.ID, map[string]any{
			"storeId": store.ID,
			"products": []map[string]any{{
				"sku":          sku,
				"asn":          asn,
				"deliveryUnit": row.DeliveryUnitCode,
				"storeId":      externalStoreID,
			}},
		}); err != nil {
			h.serverError(c, err)
			return
		}
		msg := "Product not found for this SKU. Row skipped and logged."
		if product != nil {
			msg = fmt.Sprintf("Product not found or not active (status %s). Row skipped and logged.", product.Status)
		}
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": msg, "sku": sku})
		return
	}

	// 6. ONE TRANSACTION: delivery (6a), pallet (6b), delivery product (6c)
	report, err := h.repo.Save(ctx, store, product, row)
	if err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			c.JSON(http.StatusConflict, gin.H{"error": "This row was saved by another request at the same time. Please retry."})
			return
		}
		h.serverError(c, err)
		return
	}

	// 6a (continued). DELIVERY ALREADY STARTED (status is not planned)
	// Nothing was changed. Log it OUTSIDE the transaction so the log survives, then 409.
	if report.Blocked {
		if err := h.repo.LogActivity(ctx, dto.DeliveryEventRecordsSkipped, &store.ID, map[string]any{
			"storeId":    store.ID,
			"deliveries": []map[string]any{{"asn": asn, "deliveryUnitCode": row.DeliveryUnitCode}},
		}); err != nil {
			h.serverError(c, err)
			return
		}
		c.JSON(http.StatusConflict, gin.H{
			"error":      fmt.Sprintf("Delivery has already started (status %s). Row skipped.", report.BlockedStatus),
			"deliveryId": report.BlockedDeliveryID,
		})
		return
	}

	// 7. POST-FEED PROCESSING is DISABLED (business decision, no env flag).
	// It would finish the delivery after this row, so later rows for the same ASN would get 409.
	// The code is in delivery_repository.go (PostProcess), commented out.

	// 8. Return what happened (spec 1.5)
	c.JSON(http.StatusOK, dto.DeliveryResult{
		Status:         "processed",
		Store:          dto.DeliveryStoreRef{ExternalID: store.ExternalID, ID: store.ID},
		Product:        dto.DeliveryProductRef{ID: product.ID, Sku: product.Sku},
		DeliveryReport: *report,
		PostProcessing: "disabled",
	})
}

// serverError logs the real error and sends a generic 500 (never the DB error text).
func (h *DeliveryHandler) serverError(c *gin.Context, err error) {
	log.Printf("Database error: %v", err)
	c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to import delivery"})
}
