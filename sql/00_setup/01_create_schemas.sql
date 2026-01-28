/*
===============================================================================
DDL Script: Create Data Warehouse Schemas
===============================================================================
Purpose:
    Create the Bronze, Silver, and Gold schemas inside the DataWarehouse database
    if they do not already exist.

Notes:
    - This script is idempotent: it can be executed multiple times safely.
    - It assumes the DataWarehouse database already exists.
    - GO is used to separate the USE statement into its own batch.
===============================================================================
*/

-- Switch context to the target database
USE DataWarehouse;
GO

-- Create schemas if they do not exist
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze');
END;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
END;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END;
GO
