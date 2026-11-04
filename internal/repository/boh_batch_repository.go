package repository

import (
	"context"
	"encoding/json"
	"slices"
	"time"

	"gorm.io/gorm"

	"feed-api/internal/dto"
	"feed-api/internal/model"
)

// Batch versions of the BOH steps, used by the boh queue: a whole batch of counts in a fixed number
// of queries instead of ~7 round trips per count. Same rules as Save (STEP 4..7b); POST /boh keeps Save.

// lookupChunk keeps every IN (...) list well below Postgres' 65,535 parameter limit.
const lookupChunk = 5000

// FindStoresByExternalIDs is STEP 4 for a batch: stores by stores.external_id (not stores.id).
// Missing ids are not in the map. When two stores share an external_id, the lowest id is used.
func (r *BohRepository) FindStoresByExternalIDs(ctx context.Context, externalIDs []int64) (map[int64]*model.Store, error) {
	found := map[int64]*model.Store{}
	for chunk := range slices.Chunk(externalIDs, lookupChunk) {
		var stores []model.Store
		if err := r.db.WithContext(ctx).Where("external_id IN ?", chunk).Order("id").Find(&stores).Error; err != nil {
			return nil, err
		}
		for i := range stores {
			if _, seen := found[stores[i].ExternalID]; !seen {
				found[stores[i].ExternalID] = &stores[i]
			}
		}
	}
	return found, nil
}

// FindProductsBySkus is STEP 5 for a batch: products by exact sku (id, sku, status). The caller checks status A.
func (r *BohRepository) FindProductsBySkus(ctx context.Context, skus []string) (map[string]*model.Product, error) {
	found := map[string]*model.Product{}
	for chunk := range slices.Chunk(skus, lookupChunk) {
		var products []model.Product
		if err := r.db.WithContext(ctx).Select("id", "sku", "status").Where("sku IN ?", chunk).Find(&products).Error; err != nil {
			return nil, err
		}
		for i := range products {
			found[products[i].Sku] = &products[i]
		}
	}
	return found, nil
}

// SaveBatch runs STEP 6..7b for a whole batch inside ONE transaction:
//
//	6   store_qty per store + product = SUM(product_location.qty) over all its locations, read BEFORE
//	    any write (BOH never changes product_location), 0 if none
//	7a  boh: one upsert on (store_id, product_id, date). Not found => INSERT qty, store_qty, date,
//	    created_at; qty differs => UPDATE qty only; same qty => nothing (not returned)
//	7b  store_products: one upsert of totals (the LAST count per store + product, chosen by the caller).
//	    Not found => INSERT store_id, product_id, total_qty; differs => UPDATE total_qty; same => nothing
//
// rows must have unique (store, product, date) and totals unique (store, product). Both are sorted
// by key before writing, so parallel batches lock rows in the same order (no deadlocks).
func (r *BohRepository) SaveBatch(ctx context.Context, rows []dto.BohBatchRow, totals []dto.BohTotal) (*dto.BohBatchResult, error) {
	result := &dto.BohBatchResult{Boh: map[string]bool{}, StoreProducts: map[string]bool{}}
	rows, totals = slices.Clone(rows), slices.Clone(totals)
	slices.SortFunc(rows, func(a, b dto.BohBatchRow) int {
		return cmpKeys(a.StoreID, b.StoreID, a.ProductID, b.ProductID, a.Date, b.Date)
	})
	slices.SortFunc(totals, func(a, b dto.BohTotal) int {
		return cmpKeys(a.StoreID, b.StoreID, a.ProductID, b.ProductID, "", "")
	})

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// STEP 6 — store_qty for every store + product of the batch, in one query.
		pairs, err := json.Marshal(totals)
		if err != nil {
			return err
		}
		var sums []struct {
			StoreID   uint
			ProductID uint
			StoreQty  int
		}
		if err := tx.Raw(`
			SELECT sl.store_id, pl.product_id, COALESCE(SUM(pl.qty), 0) AS store_qty
			FROM product_location pl
			JOIN store_location sl ON sl.id = pl.location_id
			WHERE (pl.product_id, sl.store_id) IN (
				SELECT t.product_id, t.store_id FROM jsonb_to_recordset(?::jsonb) AS t(store_id bigint, product_id bigint))
			GROUP BY sl.store_id, pl.product_id`, string(pairs)).Scan(&sums).Error; err != nil {
			return err
		}
		storeQty := map[string]int{}
		for _, s := range sums {
			storeQty[dto.StoreProductKey(s.StoreID, s.ProductID)] = s.StoreQty
		}
		for i := range rows {
			rows[i].StoreQty = storeQty[dto.StoreProductKey(rows[i].StoreID, rows[i].ProductID)]
		}

		// STEP 7a — boh rows.
		payload, err := json.Marshal(rows)
		if err != nil {
			return err
		}
		var written []struct {
			StoreID   uint
			ProductID uint
			Date      string
			Inserted  bool
		}
		if err := tx.Raw(`
			INSERT INTO boh (store_id, product_id, qty, store_qty, date, created_at)
			SELECT t.store_id, t.product_id, t.qty, t.store_qty, t.date::date, ?
			FROM jsonb_to_recordset(?::jsonb) AS t(store_id bigint, product_id bigint, qty int, store_qty int, date text)
			ORDER BY t.store_id, t.product_id, t.date
			ON CONFLICT (store_id, product_id, date) DO UPDATE SET qty = EXCLUDED.qty
			WHERE boh.qty IS DISTINCT FROM EXCLUDED.qty
			RETURNING store_id, product_id, date::text AS date, (xmax = 0) AS inserted`, time.Now(), string(payload)).Scan(&written).Error; err != nil {
			return translate(tx, err)
		}
		for _, w := range written {
			result.Boh[dto.BohKey(w.StoreID, w.ProductID, w.Date)] = w.Inserted
		}

		// STEP 7b — store_products.total_qty (the last count per store + product).
		var totalsWritten []struct {
			StoreID   uint
			ProductID uint
			Inserted  bool
		}
		if err := tx.Raw(`
			INSERT INTO store_products (store_id, product_id, total_qty)
			SELECT t.store_id, t.product_id, t.total_qty
			FROM jsonb_to_recordset(?::jsonb) AS t(store_id bigint, product_id bigint, total_qty int)
			ORDER BY t.store_id, t.product_id
			ON CONFLICT (store_id, product_id) DO UPDATE SET total_qty = EXCLUDED.total_qty
			WHERE store_products.total_qty IS DISTINCT FROM EXCLUDED.total_qty
			RETURNING store_id, product_id, (xmax = 0) AS inserted`, string(pairs)).Scan(&totalsWritten).Error; err != nil {
			return translate(tx, err)
		}
		for _, w := range totalsWritten {
			result.StoreProducts[dto.StoreProductKey(w.StoreID, w.ProductID)] = w.Inserted
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// translate maps an error read from RETURNING rows (after GORM's TranslateError ran) to the GORM
// errors, so a unique clash becomes gorm.ErrDuplicatedKey.
func translate(tx *gorm.DB, err error) error {
	if translator, ok := tx.Dialector.(gorm.ErrorTranslator); ok {
		return translator.Translate(err)
	}
	return err
}

func cmpKeys(storeA, storeB, productA, productB uint, dateA, dateB string) int {
	switch {
	case storeA != storeB:
		return cmpUint(storeA, storeB)
	case productA != productB:
		return cmpUint(productA, productB)
	case dateA < dateB:
		return -1
	case dateA > dateB:
		return 1
	}
	return 0
}

func cmpUint(a, b uint) int {
	if a < b {
		return -1
	}
	return 1
}
