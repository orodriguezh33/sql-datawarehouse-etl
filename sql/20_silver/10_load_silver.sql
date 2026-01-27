/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    Performs the ETL process to populate the 'silver' schema tables from the
    'bronze' schema.

Actions Performed:
    - Truncates Silver tables (full refresh).
    - Inserts transformed and cleansed data from Bronze into Silver tables.
    - Prints per-table load duration and total batch duration.
    - TRY/CATCH with error details.

Parameters:
    None.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @start_time        DATETIME,
        @end_time          DATETIME,
        @batch_start_time  DATETIME,
        @batch_end_time    DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

        /* =========================================================
           Loading CRM Tables
        ========================================================= */

        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';

        /* -----------------------------
           silver.crm_cust_info
        ----------------------------- */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data Into: silver.crm_cust_info';
        ;WITH ranked AS (
            SELECT
                cst_id,
                cst_key,
                cst_firstname,
                cst_lastname,
                cst_marital_status,
                cst_gndr,
                cst_create_date,
                ROW_NUMBER() OVER (
                    PARTITION BY cst_id
                    ORDER BY cst_create_date DESC
                ) AS rn
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        )
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            r.cst_id,
            r.cst_key,
            TRIM(r.cst_firstname) AS cst_firstname,
            TRIM(r.cst_lastname)  AS cst_lastname,
            CASE
                WHEN UPPER(TRIM(r.cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(r.cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE
                WHEN UPPER(TRIM(r.cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(r.cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            r.cst_create_date
        FROM ranked r
        WHERE r.rn = 1;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';


        /* -----------------------------
           silver.crm_prd_info
        ----------------------------- */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting Data Into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            p.prd_id,
            REPLACE(SUBSTRING(p.prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(p.prd_key, 7, LEN(p.prd_key))       AS prd_key,
            p.prd_nm,
            ISNULL(p.prd_cost, 0) AS prd_cost,
            CASE
                WHEN UPPER(TRIM(p.prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(p.prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(p.prd_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(p.prd_line)) = 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(p.prd_start_dt AS DATE) AS prd_start_dt,
            CAST(
                LEAD(p.prd_start_dt) OVER (PARTITION BY p.prd_key ORDER BY p.prd_start_dt) - 1
                AS DATE
            ) AS prd_end_dt
        FROM bronze.crm_prd_info p;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';


        /* -----------------------------
           silver.crm_sales_details
        ----------------------------- */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data Into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            s.sls_ord_num,
            s.sls_prd_key,
            s.sls_cust_id,

            CASE
                WHEN s.sls_order_dt = 0 OR LEN(s.sls_order_dt) <> 8 THEN NULL
                ELSE CAST(CAST(s.sls_order_dt AS VARCHAR(8)) AS DATE)
            END AS sls_order_dt,

            CASE
                WHEN s.sls_ship_dt = 0 OR LEN(s.sls_ship_dt) <> 8 THEN NULL
                ELSE CAST(CAST(s.sls_ship_dt AS VARCHAR(8)) AS DATE)
            END AS sls_ship_dt,

            CASE
                WHEN s.sls_due_dt = 0 OR LEN(s.sls_due_dt) <> 8 THEN NULL
                ELSE CAST(CAST(s.sls_due_dt AS VARCHAR(8)) AS DATE)
            END AS sls_due_dt,

            CASE
                WHEN s.sls_sales IS NULL
                  OR s.sls_sales <= 0
                  OR s.sls_sales <> s.sls_quantity * ABS(s.sls_price)
                THEN s.sls_quantity * ABS(s.sls_price)
                ELSE s.sls_sales
            END AS sls_sales,

            s.sls_quantity,

            CASE
                WHEN s.sls_price IS NULL OR s.sls_price <= 0
                THEN (s.sls_sales / NULLIF(s.sls_quantity, 0))
                ELSE s.sls_price
            END AS sls_price
        FROM bronze.crm_sales_details s;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';


        /* =========================================================
           Loading ERP Tables
        ========================================================= */

        PRINT '------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------';

        /* -----------------------------
           silver.erp_cust_az12
           - Fix cid 'NAS' prefix
           - Null future birthdates
           - Clean gen (CR/LF/TAB) before mapping
        ----------------------------- */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting Data Into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE
                WHEN b.cid LIKE 'NAS%' THEN SUBSTRING(b.cid, 4, LEN(b.cid))
                ELSE b.cid
            END AS cid,

            CASE
                WHEN b.bdate > GETDATE() THEN NULL
                ELSE b.bdate
            END AS bdate,

            CASE
                WHEN g.gen_clean IN ('F', 'FEMALE') THEN 'Female'
                WHEN g.gen_clean IN ('M', 'MALE')   THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12 b
        CROSS APPLY (
            SELECT UPPER(
                LTRIM(RTRIM(
                    REPLACE(REPLACE(REPLACE(b.gen, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')
                ))
            ) AS gen_clean
        ) g;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';


        /* -----------------------------
           silver.erp_loc_a101
           - Remove hyphens from cid
           - Clean cntry (CR/LF/TAB), normalize codes
        ----------------------------- */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting Data Into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            REPLACE(l.cid, '-', '') AS cid,
            CASE
                WHEN g.cntry_clean = 'DE' THEN 'GERMANY'
                WHEN g.cntry_clean IN ('US', 'USA') THEN 'UNITED STATES'
                WHEN g.cntry_clean IS NULL OR g.cntry_clean = '' THEN 'n/a'
                ELSE g.cntry_clean
            END AS cntry
        FROM bronze.erp_loc_a101 l
        CROSS APPLY (
            SELECT UPPER(
                LTRIM(RTRIM(
                    REPLACE(REPLACE(REPLACE(l.cntry, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')
                ))
            ) AS cntry_clean
        ) g;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';


        /* -----------------------------
           silver.erp_px_cat_g1v2
           - Clean maintenance (CR/LF/TAB), normalize to Yes/No
        ----------------------------- */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
            e.id,
            e.cat,
            e.subcat,
            CASE
                WHEN g.maintenance_clean = 'NO' THEN 'No'
                ELSE 'Yes'
            END AS maintenance
        FROM bronze.erp_px_cat_g1v2 e
        CROSS APPLY (
            SELECT UPPER(
                LTRIM(RTRIM(
                    REPLACE(REPLACE(REPLACE(e.maintenance, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')
                ))
            ) AS maintenance_clean
        ) g;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';


        /* =========================================================
           Batch end
        ========================================================= */
        SET @batch_end_time = GETDATE();

        PRINT '==========================================';
        PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '==========================================';

    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR(20));
        PRINT 'Error State  : ' + CAST(ERROR_STATE()  AS NVARCHAR(20));
        PRINT 'Error Line   : ' + CAST(ERROR_LINE()   AS NVARCHAR(20));
        PRINT '==========================================';

        -- Optional: rethrow to fail pipeline hard
        -- THROW;
    END CATCH
END;
GO
