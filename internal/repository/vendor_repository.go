package repository

import (
	"context"
	"encoding/json"
	"time"

	"gorm.io/gorm"

	"feed-api/internal/dto"
	"feed-api/internal/model"
)

type VendorRepository struct {
	db *gorm.DB
}

func NewVendorRepository(db *gorm.DB) *VendorRepository {
	return &VendorRepository{db: db}
}

// Upsert saves one vendor and links it to every store, all in ONE transaction.
// If any step fails, nothing is saved.
func (r *VendorRepository) Upsert(ctx context.Context, name, externalID string) (*dto.VendorUpsertResult, error) {
	result := &dto.VendorUpsertResult{}

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// -----------------------------------------------------------------
		// STEP 1 — VENDOR (upsert keyed by external_id, only name is updated)
		//   not found       => INSERT
		//   name differs    => UPDATE name
		//   same            => nothing
		// FOR UPDATE locks the row so two requests can't edit it at once.
		// -----------------------------------------------------------------
		var vendor model.Vendor
		res := tx.Clauses(forUpdate).Where("external_id = ?", externalID).Limit(1).Find(&vendor)
		if res.Error != nil {
			return res.Error
		}

		switch {
		case res.RowsAffected == 0:
			vendor = model.Vendor{Name: name, ExternalID: externalID, CreatedAt: time.Now()}
			if err := tx.Create(&vendor).Error; err != nil {
				return err
			}
			result.Action = "inserted"

		case vendor.Name != name:
			result.Before = &dto.VendorBefore{Name: vendor.Name}
			if err := tx.Model(&vendor).Update("name", name).Error; err != nil {
				return err
			}
			result.Action = "updated"

		default:
			result.Action = "unchanged"
		}
		result.Vendor = vendor

		// -----------------------------------------------------------------
		// STEP 2 — STORE LINKS (store_vendors)
		// For every store: if a (store, vendor) row does not exist yet, create it
		// with picklist_creation = false. Existing rows are never changed
		// (ON CONFLICT DO NOTHING). picklist_creation is set explicitly because
		// the DB default is true.
		// -----------------------------------------------------------------
		links := tx.Exec(`
			INSERT INTO store_vendors (store_id, vendor_id, picklist_creation, created_at)
			SELECT s.id, ?, false, NOW()
			FROM stores s
			ON CONFLICT (store_id, vendor_id) DO NOTHING`, vendor.ID)
		if links.Error != nil {
			return links.Error
		}
		result.StoreLinksAdded = links.RowsAffected
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// FindNameClashes returns, for each vendor whose name already belongs to a DIFFERENT
// external_id in the DB: external_id => external_id of the vendor that has the name.
// vendors.name is unique, so these vendors can't be saved.
func (r *VendorRepository) FindNameClashes(ctx context.Context, vendors []dto.VendorMessage) (map[string]string, error) {
	payload, err := json.Marshal(vendors)
	if err != nil {
		return nil, err
	}
	var rows []struct {
		ExternalID string
		UsedBy     string
	}
	if err := r.db.WithContext(ctx).Raw(`
		SELECT t.external_id AS external_id, v.external_id AS used_by
		FROM jsonb_to_recordset(?::jsonb) AS t(name text, external_id text)
		JOIN vendors v ON v.name = t.name AND v.external_id <> t.external_id`, string(payload)).Scan(&rows).Error; err != nil {
		return nil, err
	}
	clashes := make(map[string]string, len(rows))
	for _, row := range rows {
		clashes[row.ExternalID] = row.UsedBy
	}
	return clashes, nil
}

// UpsertBatch saves MANY vendors (one queue batch) with the same rules as Upsert,
// in ONE transaction and 2 SQL statements. If any step fails, nothing is saved.
// external_id must be unique inside vendors (the caller de-duplicates).
// The rows are sent as one JSON parameter, so the batch size does not change the SQL.
func (r *VendorRepository) UpsertBatch(ctx context.Context, vendors []dto.VendorMessage) (*dto.VendorBatchResult, error) {
	payload, err := json.Marshal(vendors)
	if err != nil {
		return nil, err
	}
	result := &dto.VendorBatchResult{Written: map[string]bool{}}

	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// -----------------------------------------------------------------
		// STEP 1 — VENDORS (upsert keyed by external_id, only name is updated)
		//   not found    => INSERT          (returned, inserted = true)
		//   name differs => UPDATE name     (returned, inserted = false)
		//   same         => nothing         (not returned)
		// Sorted by external_id so parallel batches lock rows in the same order (no deadlocks).
		// -----------------------------------------------------------------
		var rows []struct {
			ExternalID string
			Inserted   bool
		}
		if err := tx.Raw(`
			INSERT INTO vendors (name, external_id, created_at)
			SELECT t.name, t.external_id, NOW()
			FROM jsonb_to_recordset(?::jsonb) AS t(name text, external_id text)
			ORDER BY t.external_id
			ON CONFLICT (external_id) DO UPDATE SET name = EXCLUDED.name
			WHERE vendors.name IS DISTINCT FROM EXCLUDED.name
			RETURNING external_id, (xmax = 0) AS inserted`, string(payload)).Scan(&rows).Error; err != nil {
			// The error comes while reading the RETURNING rows, after GORM's TranslateError ran,
			// so translate it here (a unique name clash => gorm.ErrDuplicatedKey).
			if translator, ok := tx.Dialector.(gorm.ErrorTranslator); ok {
				return translator.Translate(err)
			}
			return err
		}
		for _, row := range rows {
			result.Written[row.ExternalID] = true
			if row.Inserted {
				result.Inserted++
			} else {
				result.Updated++
			}
		}
		result.Unchanged = int64(len(vendors) - len(rows))

		// -----------------------------------------------------------------
		// STEP 2 — STORE LINKS (store_vendors), same as Upsert STEP 2:
		// every (store, vendor) pair that does not exist yet, picklist_creation = false.
		// -----------------------------------------------------------------
		links := tx.Exec(`
			INSERT INTO store_vendors (store_id, vendor_id, picklist_creation, created_at)
			SELECT s.id, v.id, false, NOW()
			FROM vendors v CROSS JOIN stores s
			WHERE v.external_id IN (SELECT t.external_id FROM jsonb_to_recordset(?::jsonb) AS t(external_id text))
			ORDER BY v.id, s.id
			ON CONFLICT (store_id, vendor_id) DO NOTHING`, string(payload))
		if links.Error != nil {
			return links.Error
		}
		result.StoreLinksAdded = links.RowsAffected
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
