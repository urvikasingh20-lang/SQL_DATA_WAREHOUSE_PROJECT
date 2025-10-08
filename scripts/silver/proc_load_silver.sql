/*
=====================================================================================================
Stored Procedure: Load Silver Layer(Bronze -> Silver)
=====================================================================================================

Script Purpose:
  This stored procedure performs the ETL(extract,transform,load) process to
  populate the 'silver' scehma tables from the 'bronze' schema.
Actions Performed:
  -Truncate silver tables
  -inserts transformed and cleansed data from bronze into silver tables.


Parameters:
  None.
  This stored procedure does not accept any parameters or return any values.


Usage Example:
  EXEC Silver.load_silver;
=======================================================================================================
*/





CREATE OR ALTER PROCEDURE silver.load_silver AS--CREATE STORED PROCEDURE
BEGIN
	DECLARE @start_time DATETIME , @end_time DATETIME,  @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY

		SET @batch_start_time=GETDATE();
		PRINT'======================================================================================';
		PRINT'Loading Silver Layer';
		PRINT'======================================================================================';


		PRINT'--------------------------------------------------------------------------------------';
		PRINT'Loading CRM Tables';
		PRINT'--------------------------------------------------------------------------------------';



		
		SET @start_time=GETDATE();
		PRINT'>>>TRUNCATING TABLE: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;


		PRINT'>>>> INSERTING DATA INTO: silver.crm_cust_info';

		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_create_date,
			cst_marital_status,
			cst_gndr
		)
			SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname, -- removing the empty spaces
			TRIM(cst_lastname) AS cst_lastname,
			cst_create_date,
			CASE WHEN UPPER(TRIM(cst_material_status))='M' THEN 'Married'-- normalize the data into readable format
				WHEN UPPER(TRIM(cst_material_status))='S' THEN 'Single'
				ELSE 'N/A'
			END cst_marital_status,

			CASE WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female' -- removing the short entries as per our rule
				WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
				ELSE 'UNKNOWN'
			END cst_gndr

			FROM (
			SELECT *,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
			)t WHERE flag_last=1 -- removing duplicates

		SET @end_time=GETDATE();
		PRINT'>>>LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
		SELECT COUNT(*) FROM silver.crm_cust_info




		SET @start_time=GETDATE();
		PRINT'>>>TRUNCATING TABLE: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;

		PRINT'>>>> INSERTING DATA INTO: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
			prd_id ,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt

		)


		SELECT 
		prd_id,
		--making the key short so that we can connect it with erp table
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,--extract categori id
		--making the key short so that we can connect it with sale crm table
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, -- extract product key
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			 WHEN 'M' THEN 'MOUNTAIN'
			 WHEN 'R' THEN 'ROAD'
			 WHEN 'S' THEN 'OTHER SALES'
			 WHEN 'T' THEN 'TOURING'
			 ELSE 'N/A'
		END AS prd_line, -- map product line codes to descriptive values
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt-- calculate end date as one day before the next start date
		FROM bronze.crm_prd_info

		SET @end_time=GETDATE();
		PRINT'>>>LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
		SELECT COUNT(*) FROM silver.crm_prd_info





		SET @start_time=GETDATE();
		PRINT'>>>TRUNCATING TABLE: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;

		PRINT'>>>> INSERTING DATA INTO: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(

			sls_cust_id,
			sls_prd_key,
			sls_ord_num,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_price,
			sls_quantity
			)
	

		SELECT 
		sls_cust_id,
		sls_prd_key,
		sls_ord_num,
		CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 
			THEN NULL
			-- INTO CAN NOT BE CONVERTED INTO DATE DIRECTLY
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 
			THEN NULL
			-- INTO CAN NOT BE CONVERTED INTO DATE DIRECTLY
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 
			THEN NULL
			-- INTO CAN NOT BE CONVERTED INTO DATE DIRECTLY
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales != sls_quantity* ABS(sls_price)
			THEN sls_quantity*ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales, --recalculate sales if original value is missing or incorrect 
		CASE WHEN sls_price IS NULL OR sls_price<=0
			THEN sls_sales/ NULLIF(sls_quantity,0)
			ELSE sls_price --deriveprice if original value is invalid
		END AS sls_price,
		sls_quantity
		FROM bronze.crm_sales_details

		SET @end_time=GETDATE();
		PRINT'>>>LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
		SELECT COUNT(*) FROM silver.crm_sales_details



		PRINT'--------------------------------------------------------------------------------------';
		PRINT'Loading ERP Tables';
		PRINT'--------------------------------------------------------------------------------------';

		SET @start_time=GETDATE();
		PRINT'>>>TRUNCATING TABLE: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12

		PRINT'>>>> INSERTING DATA INTO: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)


		SELECT 
		CASE WHEN cid LIKE 'NAS%' 
			THEN SUBSTRING(cid,4,LEN(cid))--Remove 'NAS' prefix if present 
			ELSE cid
		END cid,
		CASE WHEN bdate>GETDATE() THEN NULL
			ELSE bdate 
		END AS bdate,--SET FUTURE BDATES TO NULL
		CASE 
			WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Feamle'
			WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
			ELSE 'N/A'
		END gen--NORMALIZE THE GENDER
		FROM bronze.erp_cust_az12

		SET @end_time=GETDATE();
		PRINT'>>>LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
		SELECT COUNT(*) FROM silver.erp_cust_az12




		SET @start_time=GETDATE();
		PRINT'>>>TRUNCATING TABLE: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101

		PRINT'>>>> INSERTING DATA INTO: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(
		cid,
		cntry)


		SELECT 
		REPLACE (cid,'-','')cid,
		CASE  
			WHEN TRIM(cntry)='DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US','USA') THEN 'Unites States'
			WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'N/A'
			ELSE TRIM(cntry)
		END AS cntry

		FROM bronze.erp_loc_a101

		SET @end_time=GETDATE();
		PRINT'>>>LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
		SELECT COUNT(*) FROM silver.erp_loc_a101


		SET @start_time=GETDATE();
		PRINT'>>>TRUNCATING TABLE: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2

		PRINT'>>>> INSERTING DATA INTO: silver.erp_px_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance)

		SELECT 
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2

		SET @end_time=GETDATE();
		PRINT'>>>LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
		SELECT COUNT(*) FROM silver.erp_px_cat_g1v2

		SET @batch_end_time=GETDATE();
		PRINT'=====================================================================';
		PRINT'LOADING SILVER LAYER IS COMPLETED';
		PRINT' --TOTAL LOAD DURATION: '+CAST(DATEDIFF(second,@batch_start_time,@batch_end_time)AS NVARCHAR)+'seconds';
		PRINT'=====================================================================';


	END TRY
	BEGIN CATCH


		PRINT'=======================================================================';
		PRINT'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT'ERROR MESSAGE'+CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'ERROR MESSAGE'+CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'=======================================================================';

	END CATCH		

END
