package repository

import (
	"fmt"
	"math"
	"slices"
	"time"

	"gorm.io/gorm"

	"feed-api/internal/dto"
	"feed-api/internal/model"
)

// Picklist values used by the POS picklist strategy (same as the CLI).
const (
	maxItemsPerPicklist     = 25
	picklistStatusNew       = "N"
	picklistStatusCompleted = "C"
	picklistTypeAdvanced    = "advanced"
	replenishmentFillToMax  = "filltomax"
	picklistItemOpen        = "O"
	picklistItemFinished    = "F"
	departmentActive        = "A"
	departmentInactive      = "I"
)

// =============================================================================
// STEP 7 — REPLENISHMENT PICKLISTS (POS picklist strategy, used in every app mode)
//
// The picklist products from STEP 5 go through these filters in order:
//
//	7.1 no product has a BOS location, or none has a FOS location => stop
//	7.2 keep products with store_products.total_qty > 0 (after STEP 6); none => stop
//	7.3/7.4 keep products with stock AND a BOS AND a FOS location in this store
//	7.5 drop products whose department is INACTIVE in this store
//	7.6 drop products whose department is active and in SYNC-UP mode
//	7.7 keep products whose deduction_counter >= trigger (moq x inner_pack); none => "no picklist needed"
//	7.8 product on a NEW picklist => top-up (7.9); on an ongoing one => nothing; neither => new item (7.10)
//
// =============================================================================
func runPicklistStep(tx *gorm.DB, storeID uint, ids []uint) (dto.PosPicklistReport, error) {
	report := dto.NewPosPicklistReport()
	filter := func(pid uint, reason string) {
		report.FilteredOut = append(report.FilteredOut, dto.FilteredProduct{ProductID: pid, Reason: reason})
	}

	// ---- 7.1 Locations: stop if NO product has BOS, or NO product has FOS ----
	var withBos, withFos []uint
	var locations []struct {
		ProductID uint
		Bos, Fos  bool
	}
	if err := tx.Raw(`SELECT l.product_id, COALESCE(bool_or(sl.is_back), false) AS bos, COALESCE(bool_or(NOT sl.is_back), false) AS fos
		FROM product_location l JOIN store_location sl ON sl.id = l.location_id
		WHERE l.product_id IN ? AND sl.store_id = ?
		GROUP BY l.product_id`, ids, storeID).Scan(&locations).Error; err != nil {
		return report, err
	}
	for _, l := range locations {
		if l.Bos {
			withBos = append(withBos, l.ProductID)
		}
		if l.Fos {
			withFos = append(withFos, l.ProductID)
		}
	}
	switch {
	case len(withBos) == 0 && len(withFos) == 0:
		report.Result = "skipped: no valid BACK OF STORE and FRONT OF STORE locations"
		return report, nil
	case len(withBos) == 0:
		report.Result = "skipped: no valid BACK OF STORE locations"
		return report, nil
	case len(withFos) == 0:
		report.Result = "skipped: no valid FRONT OF STORE locations"
		return report, nil
	}

	// ---- 7.2 Stock: products with store_products.total_qty > 0 (after STEP 6) ----
	var withQty []uint
	if err := tx.Raw(`SELECT product_id FROM store_products WHERE store_id = ? AND product_id IN ? AND total_qty > 0`,
		storeID, ids).Scan(&withQty).Error; err != nil {
		return report, err
	}
	if len(withQty) == 0 {
		report.Result = "skipped: no in-store quantity"
		return report, nil
	}

	// ---- 7.3 / 7.4 Keep products with stock AND a BOS AND a FOS location ----
	var candidates []uint
	for _, pid := range ids {
		switch {
		case !slices.Contains(withQty, pid):
			filter(pid, "total_qty is 0")
		case !slices.Contains(withBos, pid):
			filter(pid, "no back-of-store location")
		case !slices.Contains(withFos, pid):
			filter(pid, "no front-of-store location")
		default:
			candidates = append(candidates, pid)
		}
	}

	// ---- 7.5 Drop products whose department is INACTIVE in this store ----
	if len(candidates) > 0 {
		inactive, err := productsInDepartmentStatus(tx, storeID, candidates, departmentInactive)
		if err != nil {
			return report, err
		}
		for _, pid := range inactive {
			filter(pid, "department inactive in this store")
		}
		candidates = without(candidates, inactive)
	}

	// ---- 7.6 Drop products whose department is in SYNC-UP mode ----
	if len(candidates) > 0 {
		syncUp, err := productsInSyncUp(tx, storeID, candidates)
		if err != nil {
			return report, err
		}
		for _, pid := range syncUp {
			filter(pid, "department in sync-up mode")
		}
		candidates = without(candidates, syncUp)
	}

	// ---- 7.7 Trigger: deduction_counter >= moq x inner_pack (moq = ceil(moq) if > 0, else 1) ----
	var triggered []uint
	type counter struct {
		ProductID        uint
		Moq              *float64
		DeductionCounter int
		InnerPack        int
	}
	counters := map[uint]counter{}
	if len(candidates) > 0 {
		var rows []counter
		if err := tx.Raw(`SELECT sp.product_id, sp.moq, sp.deduction_counter, p.inner_pack
			FROM store_products sp JOIN products p ON p.id = sp.product_id
			WHERE sp.store_id = ? AND sp.product_id IN ?`, storeID, candidates).Scan(&rows).Error; err != nil {
			return report, err
		}
		for _, row := range rows {
			counters[row.ProductID] = row
		}
	}
	for _, pid := range candidates {
		sp := counters[pid]
		moq := 1
		if sp.Moq != nil && *sp.Moq > 0 {
			moq = int(math.Ceil(*sp.Moq))
		}
		trigger := moq * sp.InnerPack

		if sp.DeductionCounter >= trigger {
			triggered = append(triggered, pid)
		} else {
			filter(pid, fmt.Sprintf("deduction_counter %d < trigger %d (moq %d x inner_pack %d)",
				sp.DeductionCounter, trigger, moq, sp.InnerPack))
		}
	}
	if len(triggered) == 0 {
		report.Result = "no picklist needed"
		return report, nil
	}

	// ---- 7.8 Where are the triggered products now? ----
	// Items on a NEW picklist of this store => top-up (7.9).
	var newItems []struct {
		ID         uint
		ProductID  uint
		PicklistID uint
	}
	if err := tx.Raw(`SELECT pp.id, pp.product_id, pl.id AS picklist_id
		FROM picklist_products pp JOIN picklists pl ON pl.id = pp.picklist_id
		WHERE pl.store_id = ? AND pl.status = ? AND pp.product_id IN ?
		ORDER BY pp.id`, storeID, picklistStatusNew, triggered).Scan(&newItems).Error; err != nil {
		return report, err
	}

	// On an ongoing picklist (not NEW, not COMPLETED) => nothing happens; its counter keeps growing.
	var inOngoing []uint
	if err := tx.Raw(`SELECT DISTINCT pp.product_id
		FROM picklist_products pp JOIN picklists pl ON pl.id = pp.picklist_id
		WHERE pl.store_id = ? AND pl.status NOT IN ? AND pp.product_id IN ?`,
		storeID, []string{picklistStatusNew, picklistStatusCompleted}, triggered).Scan(&inOngoing).Error; err != nil {
		return report, err
	}
	for _, pid := range inOngoing {
		filter(pid, "already on an ongoing picklist")
	}

	inNew := make([]uint, 0, len(newItems))
	for _, item := range newItems {
		inNew = append(inNew, item.ProductID)
	}
	// Neither => new item through the picklist engine (7.10).
	forNewItem := without(without(triggered, inNew), inOngoing)

	// ---- 7.9 Top-up every NEW-picklist item of a triggered product ----
	// cases = floor(counter / inner_pack), qty = cases x inner_pack; counter below inner_pack => skip
	// (the CLI would crash on a second item of the same product; the API skips it instead).
	for _, item := range newItems {
		cases, qty, ok, err := pickedFromCounter(tx, storeID, item.ProductID)
		if err != nil {
			return report, err
		}
		if !ok {
			report.ToppedUp = append(report.ToppedUp, dto.ToppedUpItem{
				PicklistProductID: item.ID, PicklistID: item.PicklistID, ProductID: item.ProductID,
				Skipped: "deduction_counter below inner_pack",
			})
			continue
		}

		if err := tx.Exec(`UPDATE picklist_products
			SET pickedup_quantity = pickedup_quantity + ?, pickedup_cases = pickedup_cases + ?, desired_quantity = desired_quantity + ?
			WHERE id = ?`, qty, cases, qty, item.ID).Error; err != nil {
			return report, err
		}
		if err := resetCounter(tx, storeID, item.ProductID, qty); err != nil {
			return report, err
		}

		report.ToppedUp = append(report.ToppedUp, dto.ToppedUpItem{
			PicklistProductID: item.ID, PicklistID: item.PicklistID, ProductID: item.ProductID,
			AddedCases: cases, AddedQty: qty,
		})
		report.CountersReset = append(report.CountersReset, dto.CounterReset{ProductID: item.ProductID, By: qty, PickedCases: cases})
	}

	// ---- 7.10 New item through the picklist engine, then reset the counters ----
	if len(forNewItem) > 0 {
		if err := createAdvancedPicklist(tx, storeID, forNewItem, &report); err != nil {
			return report, err
		}

		// Same as the CLI: for every product sent to the engine, counter -= whole cases picked.
		for _, pid := range forNewItem {
			cases, qty, ok, err := pickedFromCounter(tx, storeID, pid)
			if err != nil {
				return report, err
			}
			if !ok {
				continue
			}
			if err := resetCounter(tx, storeID, pid, qty); err != nil {
				return report, err
			}
			report.CountersReset = append(report.CountersReset, dto.CounterReset{ProductID: pid, By: qty, PickedCases: cases})
		}
	}

	report.Result = "processed"
	return report, nil
}

// engineProduct is one valid product inside the picklist engine (7.10).
type engineProduct struct {
	ID               uint
	Sku              string
	InnerPack        int
	VendorID         *uint
	VendorName       *string
	DepartmentID     *uint
	DepartmentName   *string
	DeductionCounter int
}

// =============================================================================
// STEP 7.10 — PICKLIST ENGINE (CreateAdvancedPicklistUseCase, POS action,
// replenishment 'filltomax', never urgent)
//
//	safety checks (any failure => *PicklistGuardError => 409, everything rolled back):
//	  1 a department in sync-up mode in this store has the name of the product category's level_3_name
//	  2 the product is already on a not-completed picklist of this store (item not finished)
//	  3 the product's department is inactive in this store
//	valid products: active, with a location AND a store_products row in this store (others dropped silently)
//	category without a department => guard
//	grouping: vendor + department when store_vendors.picklist_creation = true, otherwise by department
//	per product: no FOS location => guard; find or create the picklist; item = max(floor(counter/inner_pack), 1) cases
//	placing: max 25 items per picklist, overflow to a matching picklist or a copy
//
// =============================================================================
func createAdvancedPicklist(tx *gorm.DB, storeID uint, ids []uint, report *dto.PosPicklistReport) error {
	// ---- Safety check 1: department (by NAME) in sync-up mode in this store ----
	var syncUp bool
	if err := tx.Raw(`SELECT EXISTS (
			SELECT 1 FROM products p JOIN categories c ON c.id = p.category_id
			WHERE p.id IN ? AND c.level_3_name IN (
				SELECT d.name FROM departments d JOIN store_departments sd ON sd.department_id = d.id
				WHERE sd.store_id = ? AND sd.sync_up_mode = true))`, ids, storeID).Scan(&syncUp).Error; err != nil {
		return err
	}
	if syncUp {
		return &PicklistGuardError{Message: "Product is undergoing sync up."}
	}

	// ---- Safety check 2: product already on a not-completed picklist of this store ----
	var onPicklist bool
	if err := tx.Raw(`SELECT EXISTS (
			SELECT 1 FROM picklist_products pp JOIN picklists pl ON pl.id = pp.picklist_id
			WHERE pp.product_id IN ? AND pl.store_id = ? AND pl.status <> ? AND pp.status <> ?
			  AND EXISTS (SELECT 1 FROM product_location l JOIN store_location sl ON sl.id = l.location_id
			              WHERE l.product_id = pp.product_id AND sl.store_id = ?))`,
		ids, storeID, picklistStatusCompleted, picklistItemFinished, storeID).Scan(&onPicklist).Error; err != nil {
		return err
	}
	if onPicklist {
		return &PicklistGuardError{Message: "Product is already in a picklist."}
	}

	// ---- Safety check 3: department inactive in this store ----
	inactive, err := productsInDepartmentStatus(tx, storeID, ids, departmentInactive)
	if err != nil {
		return err
	}
	if len(inactive) > 0 {
		return &PicklistGuardError{Message: "One or more products are under inactive departments."}
	}

	// ---- Valid products: active, a location AND a store_products row in this store ----
	var products []engineProduct
	if err := tx.Raw(`SELECT p.id, p.sku, p.inner_pack, p.vendor_id, v.name AS vendor_name,
			d.id AS department_id, d.name AS department_name, sp.deduction_counter
		FROM products p
		JOIN store_products sp ON sp.product_id = p.id AND sp.store_id = ?
		LEFT JOIN categories c ON c.id = p.category_id
		LEFT JOIN departments d ON d.id = c.department_id
		LEFT JOIN vendors v ON v.id = p.vendor_id
		WHERE p.id IN ? AND p.status = 'A'
		  AND EXISTS (SELECT 1 FROM product_location l JOIN store_location sl ON sl.id = l.location_id
		              WHERE l.product_id = p.id AND sl.store_id = ?)
		ORDER BY p.id`, storeID, ids, storeID).Scan(&products).Error; err != nil {
		return err
	}

	// ---- Category must have a department ----
	for _, p := range products {
		if p.DepartmentID == nil {
			return &PicklistGuardError{Message: "One or more products have no department assigned to their categories."}
		}
	}

	// ---- Grouping 1: vendor + department (vendor has picklist_creation = true for this store) ----
	type group struct {
		vendorID   *uint
		vendorName *string
		products   []engineProduct
	}
	var groups []*group
	byKey := map[string]*group{}
	taken := map[uint]bool{}

	var vendorIDs []uint
	for _, p := range products {
		if p.VendorID != nil && !slices.Contains(vendorIDs, *p.VendorID) {
			vendorIDs = append(vendorIDs, *p.VendorID)
		}
	}
	if len(vendorIDs) > 0 {
		var activeVendors []uint
		if err := tx.Raw(`SELECT sv.vendor_id FROM store_vendors sv JOIN vendors v ON v.id = sv.vendor_id
			WHERE sv.store_id = ? AND sv.picklist_creation = true AND sv.vendor_id IN ?`,
			storeID, vendorIDs).Scan(&activeVendors).Error; err != nil {
			return err
		}
		for _, p := range products {
			if p.VendorID == nil || !slices.Contains(activeVendors, *p.VendorID) {
				continue
			}
			key := fmt.Sprintf("%d:%d", *p.VendorID, *p.DepartmentID)
			if byKey[key] == nil {
				byKey[key] = &group{vendorID: p.VendorID, vendorName: p.VendorName}
				groups = append(groups, byKey[key])
			}
			byKey[key].products = append(byKey[key].products, p)
			taken[p.ID] = true
		}
	}

	// ---- Grouping 2: everything else by department ----
	rest := &group{}
	for _, p := range products {
		if !taken[p.ID] {
			rest.products = append(rest.products, p)
		}
	}
	if len(rest.products) > 0 {
		groups = append(groups, rest)
	}

	// ---- Per product: FOS required, find/create picklist, build the item ----
	queues := map[uint][]model.PicklistProduct{}
	var order []uint // picklist ids in first-use order
	for _, g := range groups {
		for _, p := range g.products {
			hasFos, err := productsWithLocation(tx, storeID, []uint{p.ID}, false)
			if err != nil {
				return err
			}
			if len(hasFos) == 0 {
				return &PicklistGuardError{Message: fmt.Sprintf("No front-of-store location for product %s.", p.Sku)}
			}

			picklistID, created, err := findOrCreatePicklist(tx, storeID, *p.DepartmentID, p.DepartmentName, g.vendorID, g.vendorName)
			if err != nil {
				return err
			}
			if created {
				if err := reportCreatedPicklist(tx, picklistID, report); err != nil {
					return err
				}
			}

			// Item quantity: at least 1 case, based on deduction_counter.
			innerPack := max(p.InnerPack, 1)
			cases := max(p.DeductionCounter/innerPack, 1)
			units := cases * innerPack

			if _, ok := queues[picklistID]; !ok {
				order = append(order, picklistID)
			}
			queues[picklistID] = append(queues[picklistID], model.PicklistProduct{
				ProductID: p.ID, Status: picklistItemOpen,
				DesiredQuantity: units, PickedupQuantity: units, PickedupCases: cases,
			})
		}
	}

	// ---- Place items: max 25 per picklist, overflow to a matching or copied picklist ----
	for _, picklistID := range order {
		current, remaining := picklistID, queues[picklistID]

		for len(remaining) > 0 {
			var used int64
			if err := tx.Exec(`SELECT id FROM picklists WHERE id = ? FOR UPDATE`, current).Error; err != nil {
				return err
			}
			if err := tx.Model(&model.PicklistProduct{}).Where("picklist_id = ?", current).Count(&used).Error; err != nil {
				return err
			}
			slots := maxItemsPerPicklist - int(used)

			if slots > 0 {
				n := min(slots, len(remaining))
				for _, item := range remaining[:n] {
					item.PicklistID = current
					if err := tx.Select("PicklistID", "ProductID", "Status", "DesiredQuantity", "PickedupQuantity", "PickedupCases").
						Create(&item).Error; err != nil {
						return err
					}
					report.AddedToPicklists = append(report.AddedToPicklists, dto.AddedToPicklist{
						PicklistProductID: item.ID, PicklistID: current, ProductID: item.ProductID,
						Cases: item.PickedupCases, Qty: item.PickedupQuantity,
					})
				}
				remaining = remaining[n:]
				continue
			}

			// Full => another matching NEW picklist, or a copy of this one.
			next, created, err := findOrCreateOverflowPicklist(tx, storeID, current)
			if err != nil {
				return err
			}
			if created {
				if err := reportCreatedPicklist(tx, next, report); err != nil {
					return err
				}
			}
			current = next
		}
	}
	return nil
}

// -----------------------------------------------------------------------------
// Find an open picklist for a department (+ optional vendor), or create one. Returns (id, created).
//
//	open = this store, type advanced, status N, not urgent, not locked, filltomax, no delivery,
//	       this department, fewer than 25 items; vendor group => carries that vendor's identifier,
//	       department group => carries no vendor identifier. Lowest id wins.
//	none => create: name "<vendor> - <department>" / <vendor> / <department>,
//	       sequence (newest sequence mod 999) + 1, type name, group identifiers, store link.
//
// -----------------------------------------------------------------------------
func findOrCreatePicklist(tx *gorm.DB, storeID, departmentID uint, departmentName *string, vendorID *uint, vendorName *string) (uint, bool, error) {
	// Picklists that carry a vendor group identifier.
	const vendorPicklists = `SELECT pt.picklist_id FROM picklist_types pt
		JOIN picklist_groups pg ON pg.picklist_type_id = pt.id
		JOIN picklist_group_identifiers pgi ON pgi.id = pg.picklist_group_identifier_id
		JOIN vendors v ON v.picklist_group_identifier_id = pgi.id`

	query := `SELECT pl.id FROM picklists pl
		WHERE pl.store_id = ? AND pl.type = ? AND pl.status = ? AND pl.is_urgent = false AND pl.is_locked = false
		  AND pl.replenishment_type = ? AND pl.delivery_id IS NULL AND pl.department_id = ?
		  AND (SELECT COUNT(*) FROM picklist_products pp WHERE pp.picklist_id = pl.id) < ?`
	args := []any{storeID, picklistTypeAdvanced, picklistStatusNew, replenishmentFillToMax, departmentID, maxItemsPerPicklist}
	if vendorID != nil {
		query += ` AND pl.id IN (` + vendorPicklists + ` WHERE v.id = ?)`
		args = append(args, *vendorID)
	} else {
		query += ` AND pl.id NOT IN (` + vendorPicklists + `)`
	}
	query += ` ORDER BY pl.id LIMIT 1 FOR UPDATE`

	var existing []uint
	if err := tx.Raw(query, args...).Scan(&existing).Error; err != nil {
		return 0, false, err
	}
	if len(existing) > 0 {
		return existing[0], false, nil
	}

	// ---- Create the picklist ----
	name := ""
	switch {
	case vendorName != nil && departmentName != nil:
		name = *vendorName + " - " + *departmentName
	case vendorName != nil:
		name = *vendorName
	case departmentName != nil:
		name = *departmentName
	}

	sequence, err := nextSequence(tx, storeID)
	if err != nil {
		return 0, false, err
	}
	deptID := departmentID
	picklist := model.Picklist{
		Name: name, StoreID: &storeID, Sequence: sequence, Status: picklistStatusNew, Type: picklistTypeAdvanced,
		ReplenishmentType: replenishmentFillToMax, DepartmentID: &deptID, CreatedAt: time.Now(),
	}
	if err := tx.Select("Name", "StoreID", "Sequence", "Status", "Type", "IsUrgent", "IsLocked",
		"ReplenishmentType", "DeliveryID", "DepartmentID", "CreatedAt").Create(&picklist).Error; err != nil {
		return 0, false, err
	}

	// Type name: vendor group => "vendor", otherwise "department" (default app mode).
	typeName := "department"
	// POS app mode (DISABLED): the CLI uses "moq" when APP_MODE=pos. To switch, uncomment the next line.
	// typeName = "moq"
	if vendorID != nil {
		typeName = "vendor"
	}
	typeID, err := createPicklistType(tx, picklist.ID, typeName)
	if err != nil {
		return 0, false, err
	}

	// Group identifiers: the department's always, the vendor's when grouped by vendor.
	identifierIDs := []uint{}
	deptIdentifier, err := groupIdentifier(tx, "departments", departmentID, "department")
	if err != nil {
		return 0, false, err
	}
	identifierIDs = append(identifierIDs, deptIdentifier)
	if vendorID != nil {
		vendorIdentifier, err := groupIdentifier(tx, "vendors", *vendorID, "vendor")
		if err != nil {
			return 0, false, err
		}
		identifierIDs = append(identifierIDs, vendorIdentifier)
	}

	if err := linkPicklist(tx, storeID, picklist.ID, typeID, identifierIDs); err != nil {
		return 0, false, err
	}
	return picklist.ID, true, nil
}

// -----------------------------------------------------------------------------
// Overflow (the current picklist has 25 items): another NEW picklist of this store with EXACTLY the
// same group identifiers, same replenishment type, urgent and locked flags and free slots;
// otherwise a copy of the full picklist (same name, status, type, flags, type name, identifiers,
// store link; new sequence). Returns (id, created).
// -----------------------------------------------------------------------------
func findOrCreateOverflowPicklist(tx *gorm.DB, storeID, currentID uint) (uint, bool, error) {
	var current model.Picklist
	if err := tx.First(&current, currentID).Error; err != nil {
		return 0, false, err
	}

	var identifierIDs []uint
	if err := tx.Raw(`SELECT DISTINCT pg.picklist_group_identifier_id FROM picklist_types pt
		JOIN picklist_groups pg ON pg.picklist_type_id = pt.id WHERE pt.picklist_id = ?`, currentID).Scan(&identifierIDs).Error; err != nil {
		return 0, false, err
	}

	if len(identifierIDs) > 0 {
		// replenishment_type is NOT NULL in the DB, so a plain comparison is enough.
		var match []uint
		if err := tx.Raw(`SELECT p.id FROM picklists p
			LEFT JOIN picklist_products pp ON pp.picklist_id = p.id
			LEFT JOIN picklist_types pt ON pt.picklist_id = p.id
			LEFT JOIN picklist_groups pg ON pg.picklist_type_id = pt.id
			WHERE p.store_id = ? AND p.type = ? AND p.status = ? AND p.id <> ?
			  AND p.replenishment_type = ? AND p.is_urgent = ? AND p.is_locked = ?
			GROUP BY p.id
			HAVING COUNT(DISTINCT pp.id) < ?
			   AND COUNT(DISTINCT pg.picklist_group_identifier_id) = ?
			   AND COUNT(DISTINCT CASE WHEN pg.picklist_group_identifier_id IN ? THEN pg.picklist_group_identifier_id END) = ?
			ORDER BY p.id LIMIT 1`,
			storeID, picklistTypeAdvanced, picklistStatusNew, currentID,
			current.ReplenishmentType, current.IsUrgent, current.IsLocked,
			maxItemsPerPicklist, len(identifierIDs), identifierIDs, len(identifierIDs)).Scan(&match).Error; err != nil {
			return 0, false, err
		}
		if len(match) > 0 {
			return match[0], false, nil
		}
	}

	// ---- Copy the full picklist into a new one ----
	var typeNameIDs []uint
	if err := tx.Raw(`SELECT picklist_type_name_id FROM picklist_types WHERE picklist_id = ? LIMIT 1`, currentID).Scan(&typeNameIDs).Error; err != nil {
		return 0, false, err
	}
	if len(typeNameIDs) == 0 {
		return 0, false, fmt.Errorf("picklist %d has no picklist type; cannot create overflow picklist", currentID)
	}

	sequence, err := nextSequence(tx, storeID)
	if err != nil {
		return 0, false, err
	}
	copied := model.Picklist{
		Name: current.Name, StoreID: &storeID, Sequence: sequence, Status: current.Status, Type: current.Type,
		IsUrgent: current.IsUrgent, IsLocked: current.IsLocked, ReplenishmentType: current.ReplenishmentType,
		DeliveryID: current.DeliveryID, DepartmentID: current.DepartmentID, CreatedAt: time.Now(),
	}
	if err := tx.Select("Name", "StoreID", "Sequence", "Status", "Type", "IsUrgent", "IsLocked",
		"ReplenishmentType", "DeliveryID", "DepartmentID", "CreatedAt").Create(&copied).Error; err != nil {
		return 0, false, err
	}

	var typeID uint
	if err := tx.Raw(`INSERT INTO picklist_types (picklist_id, picklist_type_name_id) VALUES (?, ?) RETURNING id`,
		copied.ID, typeNameIDs[0]).Scan(&typeID).Error; err != nil {
		return 0, false, err
	}
	if err := linkPicklist(tx, storeID, copied.ID, typeID, identifierIDs); err != nil {
		return 0, false, err
	}
	return copied.ID, true, nil
}

// =============================================================================
// Helpers (all use the INTERNAL store id)
// =============================================================================

// productsWithLocation returns the ids (from ids) that have a BOS (isBack) or FOS location in this store.
func productsWithLocation(tx *gorm.DB, storeID uint, ids []uint, isBack bool) ([]uint, error) {
	var found []uint
	err := tx.Raw(`SELECT DISTINCT l.product_id FROM product_location l
		JOIN store_location sl ON sl.id = l.location_id
		WHERE l.product_id IN ? AND sl.store_id = ? AND sl.is_back = ?`, ids, storeID, isBack).Scan(&found).Error
	return found, err
}

// productsInDepartmentStatus returns the products (with a store_products row here) whose
// department has this store_departments.status in this store.
func productsInDepartmentStatus(tx *gorm.DB, storeID uint, ids []uint, status string) ([]uint, error) {
	var found []uint
	err := tx.Raw(`SELECT DISTINCT p.id FROM products p
		JOIN categories c ON c.id = p.category_id
		JOIN store_departments sd ON sd.department_id = c.department_id AND sd.store_id = ? AND sd.status = ?
		WHERE p.id IN ?
		  AND EXISTS (SELECT 1 FROM store_products sp WHERE sp.product_id = p.id AND sp.store_id = ?)`,
		storeID, status, ids, storeID).Scan(&found).Error
	return found, err
}

// productsInSyncUp returns the products whose department is ACTIVE and in sync-up mode in this store.
func productsInSyncUp(tx *gorm.DB, storeID uint, ids []uint) ([]uint, error) {
	var found []uint
	err := tx.Raw(`SELECT DISTINCT p.id FROM products p
		JOIN categories c ON c.id = p.category_id
		JOIN store_departments sd ON sd.department_id = c.department_id
		 AND sd.store_id = ? AND sd.status = ? AND sd.sync_up_mode = true
		WHERE p.id IN ?`, storeID, departmentActive, ids).Scan(&found).Error
	return found, err
}

// pickedFromCounter: whole cases covered by the deduction counter (row locked).
//
//	counter >= inner_pack => cases = floor(counter / inner_pack), qty = cases x inner_pack, ok
//	otherwise             => not ok
func pickedFromCounter(tx *gorm.DB, storeID, productID uint) (cases, qty int, ok bool, err error) {
	var rows []struct {
		DeductionCounter int
		InnerPack        int
	}
	if err = tx.Raw(`SELECT sp.deduction_counter, p.inner_pack
		FROM store_products sp JOIN products p ON p.id = sp.product_id
		WHERE sp.store_id = ? AND sp.product_id = ?
		LIMIT 1 FOR UPDATE OF sp`, storeID, productID).Scan(&rows).Error; err != nil {
		return 0, 0, false, err
	}
	if len(rows) == 0 || rows[0].InnerPack <= 0 || rows[0].DeductionCounter < rows[0].InnerPack {
		return 0, 0, false, nil
	}
	cases = rows[0].DeductionCounter / rows[0].InnerPack
	return cases, cases * rows[0].InnerPack, true, nil
}

// resetCounter: deduction_counter -= qty.
func resetCounter(tx *gorm.DB, storeID, productID uint, qty int) error {
	return tx.Exec(`UPDATE store_products SET deduction_counter = deduction_counter - ? WHERE store_id = ? AND product_id = ?`,
		qty, storeID, productID).Error
}

// nextSequence: (the store's newest picklist's sequence mod 999) + 1.
func nextSequence(tx *gorm.DB, storeID uint) (int, error) {
	var last []int
	if err := tx.Raw(`SELECT sequence FROM picklists WHERE store_id = ? ORDER BY id DESC LIMIT 1`, storeID).Scan(&last).Error; err != nil {
		return 0, err
	}
	if len(last) == 0 {
		return 1, nil
	}
	return last[0]%999 + 1, nil
}

// createPicklistType inserts picklist_types for a picklist; the picklist_type_names row is created if missing.
func createPicklistType(tx *gorm.DB, picklistID uint, typeName string) (uint, error) {
	var nameIDs []uint
	if err := tx.Raw(`SELECT id FROM picklist_type_names WHERE name = ? ORDER BY id LIMIT 1`, typeName).Scan(&nameIDs).Error; err != nil {
		return 0, err
	}
	if len(nameIDs) == 0 {
		var id uint
		if err := tx.Raw(`INSERT INTO picklist_type_names (name) VALUES (?) RETURNING id`, typeName).Scan(&id).Error; err != nil {
			return 0, err
		}
		nameIDs = append(nameIDs, id)
	}

	var typeID uint
	err := tx.Raw(`INSERT INTO picklist_types (picklist_id, picklist_type_name_id) VALUES (?, ?) RETURNING id`,
		picklistID, nameIDs[0]).Scan(&typeID).Error
	return typeID, err
}

// groupIdentifier returns the department's / vendor's group identifier, or creates one
// (value "department" / "vendor") and stores it on that row. table is "departments" or "vendors".
func groupIdentifier(tx *gorm.DB, table string, id uint, value string) (uint, error) {
	var current []struct{ PicklistGroupIdentifierID *uint }
	if err := tx.Raw(`SELECT picklist_group_identifier_id FROM `+table+` WHERE id = ? FOR UPDATE`, id).Scan(&current).Error; err != nil {
		return 0, err
	}
	if len(current) > 0 && current[0].PicklistGroupIdentifierID != nil {
		return *current[0].PicklistGroupIdentifierID, nil
	}

	var identifierID uint
	if err := tx.Raw(`INSERT INTO picklist_group_identifiers (value) VALUES (?) RETURNING id`, value).Scan(&identifierID).Error; err != nil {
		return 0, err
	}
	err := tx.Exec(`UPDATE `+table+` SET picklist_group_identifier_id = ? WHERE id = ?`, identifierID, id).Error
	return identifierID, err
}

// linkPicklist writes one picklist_groups row per identifier and the store_picklists row.
func linkPicklist(tx *gorm.DB, storeID, picklistID, typeID uint, identifierIDs []uint) error {
	for _, identifierID := range identifierIDs {
		if err := tx.Exec(`INSERT INTO picklist_groups (picklist_group_identifier_id, picklist_type_id) VALUES (?, ?)`,
			identifierID, typeID).Error; err != nil {
			return err
		}
	}
	return tx.Exec(`INSERT INTO store_picklists (store_id, picklist_id) VALUES (?, ?)`, storeID, picklistID).Error
}

// reportCreatedPicklist adds a created picklist (with its type name) to the report.
func reportCreatedPicklist(tx *gorm.DB, picklistID uint, report *dto.PosPicklistReport) error {
	var created dto.CreatedPicklist
	err := tx.Raw(`SELECT pl.id, pl.name, pl.sequence, pl.department_id, pl.replenishment_type, ptn.name AS type_name
		FROM picklists pl
		LEFT JOIN picklist_types pt ON pt.picklist_id = pl.id
		LEFT JOIN picklist_type_names ptn ON ptn.id = pt.picklist_type_name_id
		WHERE pl.id = ?`, picklistID).Scan(&created).Error
	if err == nil {
		report.CreatedPicklists = append(report.CreatedPicklists, created)
	}
	return err
}

// without returns list minus the ids in remove, keeping the order.
func without(list, remove []uint) []uint {
	out := []uint{}
	for _, id := range list {
		if !slices.Contains(remove, id) {
			out = append(out, id)
		}
	}
	return out
}
