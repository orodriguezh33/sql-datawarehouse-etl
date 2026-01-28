/*
===============================================================================
DDL Script: Create Data Warehouse Database
===============================================================================
Purpose:
    Create the DataWarehouse database if it does not already exist.

Notes:
    - This script is idempotent: it can be executed multiple times safely.
    - It must be executed against the master database.
    - The GO statement is required to terminate the batch after CREATE DATABASE.
===============================================================================
*/

-- Check if the DataWarehouse database already exists
IF DB_ID('DataWarehouse') IS NULL
BEGIN
    -- Create the Data Warehouse database
    CREATE DATABASE DataWarehouse;
END;
GO
