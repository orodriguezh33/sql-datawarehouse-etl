/*
===============================================================================
Quality Checks: Silver Layer
===============================================================================
Purpose:
    Run data quality checks for consistency, accuracy, and standardization
    across the Silver layer.

Notes:
    - Most checks are designed to return NO ROWS when data is clean.
    - "Domain" checks return distinct values to review standardization.
===============================================================================
*/

-- =============================================================================
-- silver.crm_cust_info
-- =============================================================================
-- Primary key: NULLs or duplicates (Expectation: no rows)
SELECT cst_id, COUNT(*) AS cnt
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING cst_id IS NULL OR COUNT(*) > 1;

-- Unwanted spaces (Expectation: no rows)
SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key <> TRIM(cst_key);

-- Domain review (Expectation: only known values)
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

-- =============================================================================
-- silver.crm_prd_info
-- =============================================================================
-- Primary key: NULLs or duplicates (Expectation: no rows)
SELECT prd_id, COUNT(*) AS cnt
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING prd_id IS NULL OR COUNT(*) > 1;

-- Unwanted spaces (Expectation: no rows)
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);

-- Cost validation (Expectation: no rows)
SELECT prd_id, prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Domain review
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Date order (Expectation: no rows)
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt IS NOT NULL
  AND prd_end_dt < prd_start_dt;

-- =============================================================================
-- silver.crm_sales_details
-- =============================================================================
-- Date range sanity (Expectation: no rows)
SELECT *
FROM silver.crm_sales_details
WHERE (sls_order_dt IS NOT NULL AND (sls_order_dt < '1900-01-01' OR sls_order_dt > '2050-01-01'))
   OR (sls_ship_dt  IS NOT NULL AND (sls_ship_dt  < '1900-01-01' OR sls_ship_dt  > '2050-01-01'))
   OR (sls_due_dt   IS NOT NULL AND (sls_due_dt   < '1900-01-01' OR sls_due_dt   > '2050-01-01'));

-- Date order (Expectation: no rows)
SELECT *
FROM silver.crm_sales_details
WHERE (sls_ship_dt IS NOT NULL AND sls_order_dt > sls_ship_dt)
   OR (sls_due_dt  IS NOT NULL AND sls_order_dt > sls_due_dt);

-- Sales consistency (Expectation: no rows)
SELECT sls_ord_num, sls_prd_key, sls_cust_id, sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details
WHERE sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
   OR sls_sales <> sls_quantity * sls_price;

-- =============================================================================
-- silver.erp_cust_az12
-- =============================================================================
-- Birthdate sanity (Expectation: no rows)
SELECT cid, bdate
FROM silver.erp_cust_az12
WHERE bdate IS NOT NULL
  AND (bdate < '1924-01-01' OR bdate > CAST(GETDATE() AS DATE));

-- Domain review
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

-- =============================================================================
-- silver.erp_loc_a101
-- =============================================================================
-- Domain review
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

-- =============================================================================
-- silver.erp_px_cat_g1v2
-- =============================================================================
-- Unwanted spaces (Expectation: no rows)
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat <> TRIM(cat)
   OR subcat <> TRIM(subcat)
   OR maintenance <> TRIM(maintenance);

-- Domain review
SELECT DISTINCT maintenance
FROM silver.erp_px_cat_g1v2;
