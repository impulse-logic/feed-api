package repository

import (
	"context"
	"math"
	"time"

	"gorm.io/gorm"

	"feed-api/internal/dto"
	"feed-api/internal/model"
)

// STEP 6b: the only picklist status that does NOT block a count (completed).
const picklistCompleted = "C"

type StockTakeRepository struct {
	db *gorm.DB
}

func NewStockTakeRepository(db *gorm.DB) *StockTakeRepository {
	return &StockTakeRepository{db: db}
}

// -----------------------------------------------------------------------------
// STEP 4 — FIND THE STORE by stores.external_id (the feed's Store_ID).
//
//	found     => the store (id, external_id); stores.id is used for everything else
//	not found => nil, nil (the handler returns 422)
//
// -----------------------------------------------------------------------------
func (r *StockTakeRepository) FindStoreByExternalID(ctx context.Context, externalID int64) (*model.Store, error) {
	var store model.Store
	res := r.db.WithContext(ctx).Select("id", "external_id").Where("external_id = ?", externalID).Limit(1).Find(&store)
	if res.Error != nil {
		return nil, res.Error
	}
	if res.RowsAffected == 0 {
		return nil, nil
	}
	return &store, nil
}

// -----------------------------------------------------------------------------
// STEP 5 — FIND THE PRODUCT by exact sku (id, sku, status, category_id).
// The handler checks status = A; category_id is used by the sync-up check.
//
//	not found => nil, nil (the handler returns 422)
//
// -----------------------------------------------------------------------------
func (r *StockTakeRepository) FindProductBySku(ctx context.Context, sku string) (*model.Product, error) {
	var product model.Product
	res := r.db.WithContext(ctx).Select("id", "sku", "status", "category_id").Where("sku = ?", sku).Limit(1).Find(&product)
	if res.Error != nil {
		return nil, res.Error
	}
	if res.RowsAffected == 0 {
		return nil, nil
	}
	return &product, nil
}

// -----------------------------------------------------------------------------
// STEP 6 — SAFETY CHECKS (all use the INTERNAL store id)
// -----------------------------------------------------------------------------

// IsInSyncUp — 6a: true when the product's department (category level_3_name = department name)
// has sync_up_mode = true for this store.
func (r *StockTakeRepository) IsInSyncUp(ctx context.Context, storeID, categoryID uint) (bool, error) {
	var found bool
	err := r.db.WithContext(ctx).Raw(`
		SELECT EXISTS (
			SELECT 1 FROM store_departments sd
			JOIN departments d ON d.id = sd.department_id
			JOIN categories c ON c.level_3_name = d.name
			WHERE sd.store_id = ? AND sd.sync_up_mode = true AND c.id = ?
		)`, storeID, categoryID).Scan(&found).Error
	return found, err
}

// IsOnOpenPicklist — 6b: true when the product is on any picklist of this store that is not completed.
// Not called while 6b is disabled in the handler.
func (r *StockTakeRepository) IsOnOpenPicklist(ctx context.Context, storeID, productID uint) (bool, error) {
	var found bool
	err := r.db.WithContext(ctx).Raw(`
		SELECT EXISTS (
			SELECT 1 FROM picklist_products pp
			JOIN picklists p ON p.id = pp.picklist_id
			WHERE p.store_id = ? AND pp.product_id = ? AND p.status <> ?
		)`, storeID, productID, picklistCompleted).Scan(&found).Error
	return found, err
}

// HasBosLocation — 6c: true when the product has a back-of-store location (is_back = true) in this store.
func (r *StockTakeRepository) HasBosLocation(ctx context.Context, storeID, productID uint) (bool, error) {
	var found bool
	err := r.db.WithContext(ctx).Raw(`
		SELECT EXISTS (
			SELECT 1 FROM product_location pl
			JOIN store_location sl ON sl.id = pl.location_id
			WHERE pl.product_id = ? AND sl.store_id = ? AND sl.is_back = true
		)`, productID, storeID).Scan(&found).Error
	return found, err
}

// Save runs STEP 7a..7c inside ONE transaction. If any step returns an error,
// GORM rolls everything back. store_products is NOT updated (same as the CLI).
func (r *StockTakeRepository) Save(ctx context.Context, storeID, productID uint, bosCount, fosCount float64, bosForStockTake, fosForStockTake int) (*dto.StockTakeReport, error) {
	report := &dto.StockTakeReport{}

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error

		// STEP 7a — front-of-store locations.
		if report.Fos, err = applyCountToLocations(tx, storeID, productID, false, fosCount); err != nil {
			return err
		}

		// STEP 7b — back-of-store locations.
		if report.Bos, err = applyCountToLocations(tx, storeID, productID, true, bosCount); err != nil {
			return err
		}

		// STEP 7c — stock_takes row.
		return saveStockTake(tx, storeID, productID, bosForStockTake, fosForStockTake, report)
	})
	if err != nil {
		return nil, err
	}
	return report, nil
}

// -----------------------------------------------------------------------------
// STEP 7a / 7b — REPLACE the quantities of this product's FOS or BOS locations in this store.
// Locations are ordered by product_location.id and locked, so the "+1 remainder" always goes
// to the same locations.
//
//	no locations => nothing (the count is only saved in stock_takes)
//	count < 1    => every location: case 0, units 0, qty 0 (cleared)
//	count >= 1   => total = floor(count), split evenly:
//	                base = total / n, remainder = total % n, the first `remainder` locations get +1
//	                each location: case 0, units = qty   (7 over 3 => 3, 2, 2)
//
// Values are REPLACED, not added. A location already holding (case 0, units = qty = new)
// is "unchanged"; a NULL case or units counts as different.
// -----------------------------------------------------------------------------
func applyCountToLocations(tx *gorm.DB, storeID, productID uint, isBack bool, count float64) (dto.LocationsCountReport, error) {
	// "case" is a reserved word, so it is quoted.
	var locations []struct {
		ID    uint
		Case  *int
		Units *int
		Qty   int
	}
	if err := tx.Raw(`
		SELECT pl.id, pl."case", pl.units, pl.qty
		FROM product_location pl
		JOIN store_location sl ON sl.id = pl.location_id
		WHERE pl.product_id = ? AND sl.store_id = ? AND sl.is_back = ?
		ORDER BY pl.id
		FOR UPDATE OF pl`, productID, storeID, isBack).Scan(&locations).Error; err != nil {
		return dto.LocationsCountReport{}, err
	}

	n := len(locations)
	result := dto.LocationsCountReport{LocationCount: n, Cleared: count < 1, Locations: []dto.LocationQtyReport{}}
	if n == 0 {
		return result, nil
	}

	// Target qty per location (all 0 when cleared).
	targets := make([]int, n)
	if !result.Cleared {
		total := int(math.Floor(count))
		base, remainder := total/n, total%n
		for i := range targets {
			targets[i] = base
			if i < remainder {
				targets[i]++
			}
		}
	}

	for i, loc := range locations {
		newQty := targets[i]
		same := loc.Case != nil && *loc.Case == 0 && loc.Units != nil && *loc.Units == newQty && loc.Qty == newQty

		action := "unchanged"
		if !same {
			if err := tx.Exec(`UPDATE product_location SET "case" = 0, units = ?, qty = ? WHERE id = ?`,
				newQty, newQty, loc.ID).Error; err != nil {
				return dto.LocationsCountReport{}, err
			}
			action = "updated"
		}

		zero, units := 0, newQty
		result.Locations = append(result.Locations, dto.LocationQtyReport{
			ID:     loc.ID,
			Action: action,
			Before: dto.LocationQty{Case: loc.Case, Units: loc.Units, Qty: loc.Qty},
			After:  dto.LocationQty{Case: &zero, Units: &units, Qty: newQty},
		})
	}
	return result, nil
}

// -----------------------------------------------------------------------------
// STEP 7c — STOCK_TAKES, unique on (store_id, product_id), row locked FOR UPDATE.
//
//	not found     => INSERT bos_qty, fos_qty (rounded up), created_at = now => "inserted"
//	values differ => UPDATE bos_qty, fos_qty (created_at keeps its first value, same as the CLI)
//	                                                                        => "updated" + before
//	same          => nothing                                                => "unchanged"
//
// -----------------------------------------------------------------------------
func saveStockTake(tx *gorm.DB, storeID, productID uint, bosQty, fosQty int, report *dto.StockTakeReport) error {
	var existing model.StockTake
	res := tx.Clauses(forUpdate).Select("id", "bos_qty", "fos_qty").
		Where("store_id = ? AND product_id = ?", storeID, productID).Limit(1).Find(&existing)

	switch {
	case res.Error != nil:
		return res.Error

	case res.RowsAffected == 0:
		row := model.StockTake{StoreID: storeID, ProductID: productID, BosQty: bosQty, FosQty: fosQty, CreatedAt: time.Now()}
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
		report.StockTake = dto.StockTakeRowReport{ID: row.ID, Action: "inserted"}

	case existing.BosQty != bosQty || existing.FosQty != fosQty:
		if err := tx.Model(&model.StockTake{}).Where("id = ?", existing.ID).
			Updates(map[string]any{"bos_qty": bosQty, "fos_qty": fosQty}).Error; err != nil {
			return err
		}
		report.StockTake = dto.StockTakeRowReport{
			ID:     existing.ID,
			Action: "updated",
			Before: &dto.StockTakeBefore{BosQty: existing.BosQty, FosQty: existing.FosQty},
		}

	default:
		report.StockTake = dto.StockTakeRowReport{ID: existing.ID, Action: "unchanged"}
	}
	return nil
}
