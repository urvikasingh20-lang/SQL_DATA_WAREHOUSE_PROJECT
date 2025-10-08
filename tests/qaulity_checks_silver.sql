/*

===============================================================================================
Qaulity Checks
===============================================================================================
Script Purpose:
  This script performs various qaulity checks for data consistency , accuracy, and 
  standardization across the 'silver' schemas. It includes checks for:
  -Null or duplicates primary keys.
  -Unwaanted spaces in string fields.
  -Data Standardization and consistency.
  -Invalid data ranges and orders.
  -Data consistency b/w related fileds.


Usage Notes:
  -Run these checks after data loading silver layer.
  -Investigate and resolve any discrepancies found during the checks.
=================================================================================================
*/

--==============================================================================================
--CHECKING for silver.crm_cust_info
--==============================================================================================

--CHECK FRO DUPLICATES IN PRIMARY KEY>>> in silver 
--NO RESULT

SELECT cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL;


--check for unwanted spaces
--expectaions - no result

SELECT cst_lastname
FROM silver.crm_cust_info 
WHERE cst_lastname != TRIM(cst_lastname);


SELECT *
FROM silver.crm_cust_info




--data standardization & consistency 

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;


--==============================================================================================
--CHECKING for silver.crm_prd_info
--==============================================================================================

-- checking quality of silver schema


--CHECK FOR NULL OR DUPLICATES IN PRIMARY KEY 
--EXPECTATIONS : NO RESULT
SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) >1 OR prd_id IS NULL


--CHECK FOR UNWANTED SPACES 
--EXPECTION NO RESULT


SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)



--CHECK FOR THE COST IS NEG OR NULL
--EXPECTATION << NO RESULT


SELECT 
prd_cost
FROM silver.crm_prd_info
WHERE prd_cost<0 OR prd_cost IS NULL


--DATA STANDARDIZATION AND CONSISTENCY 

SELECT DISTINCT(prd_line)
FROM bronze.crm_prd_info

--CHECK FOR INVALID DATE ORDERS
--SOLUTION 1:INTERCHANGE WITH EACH OTHER
--DATES SHOULD NOT OVERLAP 
--RULE : END OF THE FIRST SHOULD BE SMALLER THAN FISRT OF THE SECOND
--SOLUTION 2: END DATE=STARTDATE OF THE NEXT RECORD-1// START DATE SHOULD NOT BE NULL

SELECT 
*
FROM silver.crm_prd_info



  --==============================================================================================
--CHECKING for silver.crm_sales_details
--==============================================================================================

-- checking quality of silver sales deatils tables 

-- FOR JOINING THE TABLE 
--WE WILL CHECK IF THE data in joining key exist in both 
SELECT *
FROM silver.crm_sales_details p
 WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)



SELECT *
FROM silver.crm_sales_details p
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)



SELECT * FROM silver.crm_sales_details



-- check for invalid dates (the date are in int form)
--RULE: negative number or zero cant be cast to a date
-- CHANGE 0 TO NULL
--In case of in , dates must be of length 8 
--check for boundary

SELECT 
NULLIF(sls_order_dt,0) sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt<=0 
OR LEN(sls_order_dt)!=8 OR 
sls_order_dt> 20500101 
OR sls_order_dt<19000101
 



SELECT 
NULLIF(sls_ship_dt,0) sls_ship_dt
FROM silver.crm_sales_details
WHERE sls_ship_dt<=0 
OR LEN(sls_ship_dt)!=8 OR 
sls_ship_dt> 20500101 
OR sls_ship_dt<19000101




--ORDER DATE MUST BE ALWAYS BE EARLIER THAN SHIPPING OR DUE DATE

SELECT 
*
FROM silver.crm_sales_details
WHERE sls_order_dt>sls_ship_dt OR sls_order_dt>sls_due_dt



--BUSINESS RULE
-- SUM(SALES) = QTY*PRICE
--NEGATIVE , ZEROS OR NULL ARE NOT ALLOWED

--1 SOLUTION :data issues will be fixed direct in source system 
--2 SOLUTION : data issues has to be fixed in data warehouse


/*RULES:
1> IF SALES IS NEGATIVE , ZERO OR NULL , DERIVE IT USING QUANTITY AND PRICE
2> IF PRICE IS ZERO OR NULL, CALCULATE IT USING SALES AND QANTITY 
3> IF PRICE IS NEGATIVE , CONVERT IT TO A POSITIVE VALUE
*/
SELECT DISTINCT
sls_price,
sls_quantity,
sls_sales
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity*sls_price
OR sls_quantity IS NULL OR sls_sales IS NULL OR sls_price IS NULL
OR sls_quantity<=0 OR sls_sales<=0 OR sls_price<=0
WHERE prd_end_dt<prd_start_dt





--==============================================================================================
--CHECKING for silver.erp_cust_az12
--==============================================================================================

--CHECK QALITY OF SILVER.ERP_CUST_AZ12


SELECT *
FROM silver.erp_cust_az12


--cst_id in erp_cust_az has additional "NAS" string 
SELECT * FROM silver.crm_cust_info


--check for bdate boundaries

SELECT 
bdate
FROM silver.erp_cust_az12
WHERE bdate<'1924-01-01' OR bdate > GETDATE()



--check for all distict value of gender


SELECT DISTINCT 
gen
FROM silver.erp_cust_az12
