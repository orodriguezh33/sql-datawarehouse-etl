/*
===============================================================================
Quality Checks: Gold Layer
===============================================================================
Purpose:
    Validate integrity, consistency, and analytical readiness of the Gold layer.

Checks include:
    - Surrogate key uniqueness in dimensions.
    - Referential integrity between fact and dimensions.
    - Null key validation for fact records.
===============================================================================
*/

-- =============================================================================
-- gold.dim_customers
-- =============================================================================
-- Surrogate key uniqueness (Expectation: no rows)
SELECT
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- =============================================================================
-- gold.dim_products
-- =============================================================================
-- Surrogate key uniqueness (Expectation: no rows)
SELECT
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- =============================================================================
-- gold.fact_sales
-- =============================================================================
-- Null foreign keys in fact (Expectation: no rows)
SELECT
    order_number,
    customer_key,
    product_key
FROM gold.fact_sales
WHERE customer_key IS NULL
   OR product_key IS NULL;

-- Orphan fact records (fact -> dims) (Expectation: no rows)
SELECT
    f.order_number,
    f.customer_key,
    f.product_key,
    CASE WHEN c.customer_key IS NULL THEN 1 ELSE 0 END AS missing_customer_dim,
    CASE WHEN p.product_key IS NULL THEN 1 ELSE 0 END AS missing_product_dim
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
WHERE c.customer_key IS NULL
   OR p.product_key IS NULL;
