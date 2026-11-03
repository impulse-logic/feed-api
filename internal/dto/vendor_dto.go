package dto

import "feed-api/internal/model"

type CreateVendorRequest struct {
	Name       string `json:"name" binding:"required"`
	ExternalID string `json:"external_id" binding:"required"`
}

// VendorUpsertResult is what POST /vendors did.
// Action is "inserted", "updated" or "unchanged". Before is set only when updated.
type VendorUpsertResult struct {
	Vendor          model.Vendor  `json:"vendor"`
	Action          string        `json:"action"`
	Before          *VendorBefore `json:"before"`
	StoreLinksAdded int64         `json:"storeLinksAdded"`
}

type VendorBefore struct {
	Name string `json:"name"`
}

// VendorMessage is one message body from the Service Bus "vendor" queue:
// {"external_id": "1011", "name": "vendor1"}. FlexString trims both values while decoding.
type VendorMessage struct {
	Name       FlexString `json:"name" binding:"required,max=255"`
	ExternalID FlexString `json:"external_id" binding:"required,max=255"`
}

// VendorBatchResult is what VendorRepository.UpsertBatch did for one batch.
type VendorBatchResult struct {
	Inserted        int64
	Updated         int64
	Unchanged       int64
	StoreLinksAdded int64
	// Written holds the external_ids that were inserted or updated; the others were unchanged.
	Written map[string]bool
}
