USE DataWarehouse;
GO

/*
===============================================================================
Stored Procedure: bronze.load_bronze
===============================================================================
Purpose:
    Load raw source CSV files into the Bronze layer tables.

Behavior:
    - Truncates Bronze tables before loading.
    - Uses BULK INSERT to load CSV files from /datasets (Docker volume mount).
    - Prints duration per table and total duration.
    - Throws on error to fail the ETL pipeline (sqlcmd -b).

Usage:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @start_time       DATETIME,
        @end_time         DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time   DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '================================================';

        -- ================================================================
        -- CRM Tables
        -- ================================================================
        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';

        -- CRM: bronze.crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Bulk inserting: /datasets/source_crm/cust_info.csv';
        BULK INSERT bronze.crm_cust_info
        FROM '/datasets/source_crm/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';

        -- CRM: bronze.crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Bulk inserting: /datasets/source_crm/prd_info.csv';
        BULK INSERT bronze.crm_prd_info
        FROM '/datasets/source_crm/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';

        -- CRM: bronze.crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Bulk inserting: /datasets/source_crm/sales_details.csv';
        BULK INSERT bronze.crm_sales_details
        FROM '/datasets/source_crm/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';

        -- ================================================================
        -- ERP Tables
        -- ================================================================
        PRINT '------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------';

        -- ERP: bronze.erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        -- NOTE: Ensure file name casing matches the mounted file system.
        PRINT '>> Bulk inserting: /datasets/source_erp/LOC_A101.csv';
        BULK INSERT bronze.erp_loc_a101
        FROM '/datasets/source_erp/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';

        -- ERP: bronze.erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Bulk inserting: /datasets/source_erp/CUST_AZ12.csv';
        BULK INSERT bronze.erp_cust_az12
        FROM '/datasets/source_erp/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';

        -- ERP: bronze.erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Bulk inserting: /datasets/source_erp/PX_CAT_G1V2.csv';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/datasets/source_erp/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '>> -------------';

        SET @batch_end_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Bronze load completed successfully';
        PRINT 'Total duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '==========================================';

    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR OCCURRED DURING BRONZE LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR(20));
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR(20));
        PRINT '==========================================';

        THROW; -- Fail fast so the Python runner detects the failure
    END CATCH
END;
GO
