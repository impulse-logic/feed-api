package repository

import (
	"context"
	"slices"
	"time"

	"gorm.io/gorm"

	"feed-api/internal/dto"
	"feed-api/internal/model"
)

// PicklistGuardError is a failed safety check inside the picklist engine (STEP 7.10).
// Returning it from the transaction rolls EVERYTHING back (the sale too); the handler answers 409.
type PicklistGuardError struct {
	Message string
}

func (e *PicklistGuardError) Error() string { return e.Message }

// PosProducts is the result of STEP 5.
type PosProducts struct {
	All       []model.Product // every active product found, ordered by id (all go through STEP 7)
	Sale      *model.Product  // the product the sale is booked on; nil = matched only by primary_upc
	MatchedBy string          // "sku", "variant" or "primary_upc only"
}

type PosRepository struct {
	db *gorm.DB
}

func NewPosRepository(db *gorm.DB) *PosRepository {
	return &PosRepository{db: db}
}

// -----------------------------------------------------------------------------
// STEP 4 — FIND THE STORE by stores.external_id (the feed's Store_ID).
// Everything after this uses the INTERNAL stores.id.
//
//	not found => nil, nil (the handler returns 422)
//
// -----------------------------------------------------------------------------
func (r *PosRepository) FindStoreByExternalID(ctx context.Context, externalID int64) (*model.Store, error) {
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
// STEP 5 — FIND ACTIVE PRODUCTS (status A) whose sku OR primary_upc OR one of whose
// variants equals the sent code, ordered by id.
//
//	none found => All is empty (the handler returns 422)
//	sale product = first sku match, else the first product that owns the variant,
//	               else none ("primary_upc only": the sale is not deducted and not stored, same as the CLI)
//
// -----------------------------------------------------------------------------
func (r *PosRepository) FindProducts(ctx context.Context, code string) (*PosProducts, error) {
	found, err := r.FindProductsByCodes(ctx, []string{code})
	if err != nil {
		return nil, err
	}
	if found[code] == nil {
		return &PosProducts{}, nil
	}
	return found[code], nil
}

// FindProductsByCodes is STEP 5 for many codes in ONE query (the pos queue sends a whole batch).
// sku and variant use their btree indexes. primary_upc has only a trigram (gin) index, which is ~8x
// slower for exact matches than scanning the active products, so appending an empty string keeps the planner off it.
// A code with no active product is not in the map. The rules per code are the same as FindProducts.
func (r *PosRepository) FindProductsByCodes(ctx context.Context, codes []string) (map[string]*PosProducts, error) {
	var rows []codeMatch
	for chunk := range slices.Chunk(slices.Compact(slices.Sorted(slices.Values(codes))), lookupChunk) {
		var part []codeMatch
		if err := r.db.WithContext(ctx).Raw(`
			SELECT p.sku AS code, p.id, p.sku, 'sku' AS matched FROM products p WHERE p.status = 'A' AND p.sku IN ?
			UNION ALL
			SELECT p.primary_upc, p.id, p.sku, 'primary_upc' FROM products p WHERE p.status = 'A' AND (p.primary_upc || '') IN ?
			UNION ALL
			SELECT v.variant, p.id, p.sku, 'variant' FROM product_variant v JOIN products p ON p.id = v.product_id
			WHERE p.status = 'A' AND v.variant IN ?
			ORDER BY 1, 2`, chunk, chunk, chunk).Scan(&part).Error; err != nil {
			return nil, err
		}
		rows = append(rows, part...)
	}

	// Every active product found per code, ordered by id (rows are ordered by code, then id).
	found := map[string]*PosProducts{}
	for _, row := range rows {
		f := found[row.Code]
		if f == nil {
			f = &PosProducts{MatchedBy: "primary_upc only"}
			found[row.Code] = f
		}
		if n := len(f.All); n == 0 || f.All[n-1].ID != row.ID {
			f.All = append(f.All, model.Product{ID: row.ID, Sku: row.Sku})
		}
	}
	// Sale product: the sku match, else the lowest-id variant owner, else none ("primary_upc only").
	for _, row := range rows {
		f := found[row.Code]
		if row.Matched == "sku" || (row.Matched == "variant" && f.MatchedBy != "sku" && f.MatchedBy != "variant") {
			i := slices.IndexFunc(f.All, func(p model.Product) bool { return p.ID == row.ID })
			f.Sale, f.MatchedBy = &f.All[i], row.Matched
		}
	}
	return found, nil
}

// codeMatch is one active product found for a code by FindProductsByCodes.
type codeMatch struct {
	Code    string
	ID      uint
	Sku     string
	Matched string // "sku", "primary_upc" or "variant"
}

// Import runs STEP 6..9 for one sale line inside ONE transaction, in this order:
// the picklist step reads the counters the deduction has just changed.
// A *PicklistGuardError (or any other error) rolls everything back.
func (r *PosRepository) Import(ctx context.Context, storeID uint, products *PosProducts, sale dto.PosSale) (*dto.PosReport, error) {
	report := &dto.PosReport{}

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error

		// STEP 6 — deduct stock
		if report.Deduction, err = deductStock(tx, storeID, products.Sale, sale.Qty); err != nil {
			return err
		}

		// STEP 7 — replenishment picklists
		if report.Picklists, err = runPicklistStep(tx, storeID, productIDs(products.All)); err != nil {
			return err
		}

		// STEP 8 — pos record
		if report.PosRecord, err = savePosRecord(tx, storeID, products.Sale, sale); err != nil {
			return err
		}

		// STEP 9 — remove empty picklists of this store
		report.EmptyPicklistsRemoved, err = removeEmptyPicklists(tx, storeID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return report, nil
}

// -----------------------------------------------------------------------------
// STEP 6 — DEDUCT STOCK (POS rule, used in every app mode; the CLI hard-codes it)
//
//	no sale product (primary_upc only) => skip
//	qty 0                               => skip
//	no store_products row               => skip
//	otherwise                           => total_qty = total_qty < qty ? 0 : total_qty - qty  (never below 0)
//	                                       deduction_counter += qty
//
// Only store_products changes; shelf quantities (product_location.qty) are never touched by a sale.
// -----------------------------------------------------------------------------
func deductStock(tx *gorm.DB, storeID uint, sale *model.Product, qty int) (dto.PosDeduction, error) {
	if sale == nil {
		return dto.PosDeduction{Reason: "Code matched a product only by primary_upc; sale not deducted (same as the CLI)."}, nil
	}
	if qty <= 0 {
		return dto.PosDeduction{Reason: "qty is 0 after rounding; nothing to deduct."}, nil
	}

	var rows []struct {
		ID               uint
		TotalQty         int
		DeductionCounter int
	}
	if err := tx.Raw(`SELECT id, total_qty, deduction_counter FROM store_products
		WHERE store_id = ? AND product_id = ? LIMIT 1 FOR UPDATE`, storeID, sale.ID).Scan(&rows).Error; err != nil {
		return dto.PosDeduction{}, err
	}
	if len(rows) == 0 {
		return dto.PosDeduction{Reason: "Product has no store_products row in this store."}, nil
	}
	sp := rows[0]

	totalAfter := sp.TotalQty - qty
	if sp.TotalQty < qty {
		totalAfter = 0
	}
	counterAfter := sp.DeductionCounter + qty

	if err := tx.Exec(`UPDATE store_products SET total_qty = ?, deduction_counter = ? WHERE id = ?`,
		totalAfter, counterAfter, sp.ID).Error; err != nil {
		return dto.PosDeduction{}, err
	}

	return dto.PosDeduction{
		Applied: true,
		Before:  &dto.PosStock{TotalQty: sp.TotalQty, DeductionCounter: sp.DeductionCounter},
		After:   &dto.PosStock{TotalQty: totalAfter, DeductionCounter: counterAfter},
	}, nil
}

// -----------------------------------------------------------------------------
// STEP 8 — POS RECORD, unique on (store_id, register, product_id, transaction), row locked.
//
//	no sale product => skip (primary_upc only, same as the CLI)
//	not found       => INSERT every column, created_at = now  => "inserted"
//	found           => qty = qty + new qty, nothing else      => "updated"
//	                   (resending a row counts the sale twice: the sender must never resend)
//
// -----------------------------------------------------------------------------
func savePosRecord(tx *gorm.DB, storeID uint, sale *model.Product, s dto.PosSale) (dto.PosRecordReport, error) {
	if sale == nil {
		return dto.PosRecordReport{Action: "skipped", Reason: "Code matched a product only by primary_upc (same as the CLI)."}, nil
	}

	var existing model.Pos
	res := tx.Clauses(forUpdate).Select("id", "qty").
		Where(map[string]any{"store_id": storeID, "register": s.Register, "product_id": sale.ID, "transaction": s.Transaction}).
		Limit(1).Find(&existing)
	if res.Error != nil {
		return dto.PosRecordReport{}, res.Error
	}

	if res.RowsAffected == 0 {
		row := model.Pos{
			StoreID: storeID, ProductID: sale.ID, TransactedAt: s.TransactedAt,
			Register: s.Register, Operator: s.Operator, TransactionType: s.TransactionType,
			Transaction: s.Transaction, SeqNumber: s.SeqNumber, Sku: s.Sku, Qty: float64(s.Qty),
			RegularPriceEach: s.RegularPriceEach, SoldPriceEach: s.SoldPriceEach,
			IsPriceOverride: s.IsPriceOverride, DiscountCode: float64(s.DiscountCode), CreatedAt: time.Now(),
		}
		if err := tx.Create(&row).Error; err != nil {
			return dto.PosRecordReport{}, err
		}
		return dto.PosRecordReport{ID: row.ID, Action: "inserted"}, nil
	}

	if err := tx.Model(&model.Pos{}).Where("id = ?", existing.ID).
		Update("qty", gorm.Expr("qty + ?", s.Qty)).Error; err != nil {
		return dto.PosRecordReport{}, err
	}
	before, after := existing.Qty, existing.Qty+float64(s.Qty)
	return dto.PosRecordReport{ID: existing.ID, Action: "updated", QtyBefore: &before, QtyAfter: &after}, nil
}

// -----------------------------------------------------------------------------
// STEP 9 — REMOVE EMPTY PICKLISTS of this store (picklists with no items).
//
//	none        => nothing (0)
//	otherwise   => remember the delivery ids of ALL this store's picklists, then delete for the
//	               empty ones: picklist_product_assignations -> picklist_products -> picklist_users
//	               -> picklist_groups -> picklist_types -> store_picklists -> picklists.
//	               Every remembered delivery left with no picklist gets preparation_end_at = now.
//
// -----------------------------------------------------------------------------
func removeEmptyPicklists(tx *gorm.DB, storeID uint) (int, error) {
	var emptyIDs []uint
	if err := tx.Raw(`SELECT pl.id FROM picklists pl
		WHERE pl.store_id = ? AND NOT EXISTS (SELECT 1 FROM picklist_products pp WHERE pp.picklist_id = pl.id)`,
		storeID).Scan(&emptyIDs).Error; err != nil {
		return 0, err
	}
	if len(emptyIDs) == 0 {
		return 0, nil
	}

	var deliveryIDs []uint
	if err := tx.Raw(`SELECT DISTINCT delivery_id FROM picklists WHERE store_id = ? AND delivery_id IS NOT NULL`,
		storeID).Scan(&deliveryIDs).Error; err != nil {
		return 0, err
	}

	deletes := []string{
		`DELETE FROM picklist_product_assignations WHERE picklist_product_id IN (SELECT id FROM picklist_products WHERE picklist_id IN ?)`,
		`DELETE FROM picklist_products WHERE picklist_id IN ?`,
		`DELETE FROM picklist_users WHERE picklist_id IN ?`,
		`DELETE FROM picklist_groups WHERE picklist_type_id IN (SELECT id FROM picklist_types WHERE picklist_id IN ?)`,
		`DELETE FROM picklist_types WHERE picklist_id IN ?`,
	}
	for _, sql := range deletes {
		if err := tx.Exec(sql, emptyIDs).Error; err != nil {
			return 0, err
		}
	}
	if err := tx.Exec(`DELETE FROM store_picklists WHERE store_id = ? AND picklist_id IN ?`, storeID, emptyIDs).Error; err != nil {
		return 0, err
	}
	if err := tx.Exec(`DELETE FROM picklists WHERE store_id = ? AND id IN ?`, storeID, emptyIDs).Error; err != nil {
		return 0, err
	}

	if len(deliveryIDs) > 0 {
		if err := tx.Exec(`UPDATE deliveries SET preparation_end_at = ?
			WHERE id IN ? AND NOT EXISTS (SELECT 1 FROM picklists pl WHERE pl.delivery_id = deliveries.id)`,
			time.Now(), deliveryIDs).Error; err != nil {
			return 0, err
		}
	}
	return len(emptyIDs), nil
}

// productIDs returns the ids of the products, in the same order.
func productIDs(products []model.Product) []uint {
	ids := make([]uint, len(products))
	for i, p := range products {
		ids[i] = p.ID
	}
	return ids
}
