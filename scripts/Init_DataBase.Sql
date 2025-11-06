/********************************************************************************************
    Project: Modern Data Warehouse Setup
    Author: Vikas Sharma
    Description:
        This script creates a structured SQL Server Data Warehouse environment
        following the multi-layered architecture pattern:
            • Bronze – Raw/ingested data
            • Silver – Cleaned and transformed data
            • Gold – Curated, analytics-ready data

    Version: 1.0
    Date Created: 2025-11-06

    ----------------------------------------------------------------------------------------
    ⚠️ WARNINGS & NOTES:
      • Run this script in SQL Server Management Studio (SSMS) with proper permissions.
      • Ensure you are NOT connected to a production environment before executing.
      • Modify database name if 'DataWareHouse' already exists.
      • Use this as part of your ETL / ELT setup.

********************************************************************************************/

-- Switch to master database (recommended before creating a new database)
USE master;
GO

----------------------------------------------------------------------------------------------
-- 1️⃣ Create the Data Warehouse database
----------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'DataWareHouse')
BEGIN
    PRINT('✅ Creating database: DataWareHouse...');
    CREATE DATABASE DataWareHouse;
END
ELSE
BEGIN
    PRINT('⚠️ Database [DataWareHouse] already exists. Skipping creation.');
END
GO

----------------------------------------------------------------------------------------------
-- 2️⃣ Switch to the newly created Data Warehouse
----------------------------------------------------------------------------------------------
USE DataWareHouse;
GO

----------------------------------------------------------------------------------------------
-- 3️⃣ Create Schemas for Multi-Layered Architecture
----------------------------------------------------------------------------------------------

-- 🔹 Bronze Layer: Raw / Staging Area
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze');
    PRINT('✅ Schema [bronze] created successfully.');
END
ELSE
BEGIN
    PRINT('⚠️ Schema [bronze] already exists.');
END
GO

-- 🔸 Silver Layer: Cleaned / Transformed Data
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
    PRINT('✅ Schema [silver] created successfully.');
END
ELSE
BEGIN
    PRINT('⚠️ Schema [silver] already exists.');
END
GO

-- 🟡 Gold Layer: Curated / Analytics-Ready Data
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
    PRINT('✅ Schema [gold] created successfully.');
END
ELSE
BEGIN
    PRINT('⚠️ Schema [gold] already exists.');
END
GO

----------------------------------------------------------------------------------------------
-- ✅ Summary
----------------------------------------------------------------------------------------------
PRINT('🎯 Data Warehouse setup completed successfully!');
PRINT('   Database: DataWareHouse');
PRINT('   Schemas: bronze, silver, gold');
GO
