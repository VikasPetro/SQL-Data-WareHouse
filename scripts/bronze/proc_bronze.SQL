/*
========================================================================
Stored Procedure :Load Bronze Layer (Source-> Bronze)
========================================================================
Script Purpose:
This Stored procedure load data into the 'bronze' schema from external CSV files.
It performs the Following actions :
-truncate the bronze table before loading the data.
-use the 'BULK INSERT' Command to load data from csv files to bronze table.

Parameters
None.
this stored procedure does not accept any parameters to return any values.
Usage Example:
EXEC bronze.load_bronze;
========================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze As
BEGIN
DECLARE @START_TIME DATETIME, @END_TIME DATETIME;
BEGIN TRY
SET @START_TIME = GETDATE();
TRUNCATE TABLE bronze.crm_cust_info;
BULK INSERT  bronze.crm_cust_info
FROM 'C:\Users\HP\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
 FIRSTROW = 2,
 FIELDTERMINATOR =',',
 TABLOCK
);
SET @END_TIME = GETDATE();
PRINT '>> LOAD DURATION :' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS';
PRINT '>> --------';

SET @START_TIME = GETDATE();
TRUNCATE TABLE bronze.crm_prd_info;
BULK INSERT  bronze.crm_prd_info
FROM 'C:\Users\HP\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
 FIRSTROW = 2,
 FIELDTERMINATOR =',',
 TABLOCK
);
SET @END_TIME = GETDATE();
PRINT '>> LOAD DURATION :' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS';
PRINT '>> --------';

SET @START_TIME = GETDATE();
TRUNCATE TABLE bronze.crm_sales_details;
BULK INSERT  bronze.crm_sales_details
FROM 'C:\Users\HP\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
 FIRSTROW = 2,
 FIELDTERMINATOR =',',
 TABLOCK
);
SET @END_TIME = GETDATE();
PRINT '>> LOAD DURATION :' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS';
PRINT '>> --------';

SET @START_TIME = GETDATE();
TRUNCATE TABLE bronze.erp_cust_az12;
BULK INSERT  bronze.erp_cust_az12
FROM 'C:\Users\HP\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
WITH (
 FIRSTROW = 2,
 FIELDTERMINATOR =',',
 TABLOCK
);
SET @END_TIME = GETDATE();
PRINT '>> LOAD DURATION :' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS';
PRINT '>> --------';
SET @START_TIME = GETDATE();
TRUNCATE TABLE bronze.erp_loc_a101;
BULK INSERT  bronze.erp_loc_a101
FROM 'C:\Users\HP\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
WITH (
 FIRSTROW = 2,
 FIELDTERMINATOR =',',
 TABLOCK
);
SET @END_TIME = GETDATE();
PRINT '>> LOAD DURATION :' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS';
PRINT '>> --------';

SET @START_TIME = GETDATE();
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
BULK INSERT  bronze.erp_px_cat_g1v2
FROM 'C:\Users\HP\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
WITH (
 FIRSTROW = 2,
 FIELDTERMINATOR =',',
 TABLOCK
);
SET @END_TIME = GETDATE();
PRINT '>> LOAD DURATION :' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS';
PRINT '>> --------';
END TRY 
BEGIN CATCH 
PRINT '==============================================='
PRINT 'ERROR OCCURED DURING LOADING IN BRONZE LAYER'
PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR );
PRINT '==============================================='
END CATCH
END
