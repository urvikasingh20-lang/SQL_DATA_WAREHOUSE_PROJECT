/*
==============================================================================================
Stored ProcedureL Load Bronze Layer(Source ->Bronze)
==============================================================================================
Script Purpose:
    This stored procedure loads data into the "bronze"schema from external CSV files.
    Its perfroms the following actions:
    - Trunacte the bronze tables before loading the data.
    - Uses the "BULK INSERT" command to load data from csv files to bronze tables.

Parameters:
  None
This stored procedure does not accept any paramenters or return any values.

Usage Exapmle:
  EXEC bronze.load_bronze;
===============================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS --creating stored procedure/when we need to execute any code frequently
BEGIN
	DECLARE @start_time DATETIME , @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY

		SET @batch_start_time=GETDATE();
		PRINT'======================================================================================';
		PRINT'Loading Bronze Layer';
		PRINT'======================================================================================';


		PRINT'--------------------------------------------------------------------------------------';
		PRINT'Loading CRM Tables';
		PRINT'--------------------------------------------------------------------------------------';


		SET @start_time=GETDATE();
		PRINT'..TRUNCATING THE TABLE';
		TRUNCATE TABLE bronze.crm_cust_info;

		 PRINT'..INSERTING THE DATA INTO bronze.crm_cust_info ';
		 BULK INSERT bronze.crm_cust_info
		 FROM 'C:\Users\URVIKA SINGH\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		 WITH(
			FIRSTROW=2,-- as first row in csv is of column names
			FIELDTERMINATOR=',', --seperator
			TABLOCK
			);

		SET @end_time=GETDATE();
		PRINT'>>>LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
		SELECT COUNT(*) FROM bronze.crm_cust_info


		SET @start_time=GETDATE();
		PRINT'..TRUNCATING THE TABLE';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT'..INSERTING THE DATA INTO bronze.crm_prd_info ';

		 BULK INSERT bronze.crm_prd_info
		 FROM 'C:\Users\URVIKA SINGH\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		 WITH(
			FIRSTROW=2,-- as first row in csv is of column names
			FIELDTERMINATOR=',', --seperator
			TABLOCK
			);

		SET @end_time=GETDATE();
		PRINT'>>>LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';

		SELECT COUNT(*) FROM bronze.crm_prd_info


		SET @start_time=GETDATE();
		PRINT'..TRUNCATING THE TABLE';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT'..INSERTING THE DATA INTO bronze.crm_sales_details';
		 BULK INSERT bronze.crm_sales_details
		 FROM 'C:\Users\URVIKA SINGH\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		 WITH(
			FIRSTROW=2,-- as first row in csv is of column names
			FIELDTERMINATOR=',', --seperator
			TABLOCK
			);
		SET @end_time=GETDATE();
		PRINT'>>>LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';

		SELECT COUNT(*) FROM bronze.crm_sales_details

	


		PRINT'--------------------------------------------------------------------------------------';
		PRINT'Loading ERP Tables';
		PRINT'--------------------------------------------------------------------------------------';


		SET @start_time=GETDATE();
		PRINT'..TRUNCATING THE TABLE';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT'..INSERTING THE DATA INTO bronze.erp_cust_az12';
		 BULK INSERT bronze.erp_cust_az12
		 FROM 'C:\Users\URVIKA SINGH\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		 WITH(
			FIRSTROW=2,-- as first row in csv is of column names
			FIELDTERMINATOR=',', --seperator
			TABLOCK
			);

		SET @end_time=GETDATE();
		PRINT'>>>LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';

		SELECT COUNT(*) FROM bronze.erp_cust_az12


		SET @start_time=GETDATE();
		PRINT'..TRUNCATING THE TABLE';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT'..INSERTING THE DATA INTO bronze.erp_loc_a101';
		 BULK INSERT bronze.erp_loc_a101
		 FROM 'C:\Users\URVIKA SINGH\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		 WITH(
			FIRSTROW=2,-- as first row in csv is of column names
			FIELDTERMINATOR=',', --seperator
			TABLOCK
			);

		SET @end_time=GETDATE();
		PRINT'>>>LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';

		SELECT COUNT(*) FROM bronze.erp_loc_a101

		
		SET @start_time=GETDATE();
		PRINT'..TRUNCATING THE TABLE';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT'..INSERTING THE DATA INTO bronze.erp_px_cat_g1v2';
		 BULK INSERT bronze.erp_px_cat_g1v2
		 FROM 'C:\Users\URVIKA SINGH\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		 WITH(
			FIRSTROW=2,-- as first row in csv is of column names
			FIELDTERMINATOR=',', --seperator
			TABLOCK
			);

		SET @end_time=GETDATE();
		PRINT'>>>LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';

		SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2

		SET @batch_end_time=GETDATE();
		PRINT'=====================================================================';
		PRINT'LOADING BRONZE LAYER IS COMPLETED';
		PRINT' --TOTAL LOAD DURATION: '+CAST(DATEDIFF(second,@batch_start_time,@batch_end_time)AS NVARCHAR)+'seconds';
		PRINT'=====================================================================';



	END TRY
	BEGIN CATCH

		PRINT'=======================================================================';
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT'ERROR MESSAGE'+CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'ERROR MESSAGE'+CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'=======================================================================';

	END CATCH

END
