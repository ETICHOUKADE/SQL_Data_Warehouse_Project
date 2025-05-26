/*
===============================================================================
Quality Checks - Silver Layer
===============================================================================
Purpose:
    performed essential quality checks on the data
    loaded into the 'Silver Layer'. It helps ensure:
    - No null or duplicate values in primary key columns.
    - Removal of unwanted spaces in string fields.
    - Standardized and consistent data formats.
    - Valid date ranges and correct ordering.
    - Logical consistency between related fields.

===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results

SELECT cst_id,
count(*)
FROM silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is NULL;

--Check for unwanted sapce in string columns
-- Expectations := No results

select cst_firstname
FROM silver.crm_cust_info
where cst_firstname != trim(cst_firstname)

select cst_lastname
FROM silver.crm_cust_info
where cst_lastname != trim(cst_lastname)

-- Data consistency & standarization

select distinct cst_gndr
FROM silver.crm_cust_info

select distinct cst_marital_status
FROM silver.crm_cust_info

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results

SELECT prd_id,
count(*)
FROM silver.crm_prd_info
group by prd_id
having count(*) > 1 or or prd_id is NULL;

--Check for unwanted sapce in string columns
-- Expectations := No results

select prd_nm
FROM silver.crm_prd_info
where prd_nm != trim(prd_nm)

-- Check for NULLs or Negative Values in Cost
-- Expectation: No Results
SELECT 
    prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT 
    prd_line 
FROM silver.crm_prd_info;

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================

-- check for null and duplicates in the primary key column 
-- Expectations := No results

select * from bronze.crm_sales_details
SELECT sls_ord_num,
count(*)
FROM bronze.crm_sales_details
group by sls_ord_num
having count(*) > 1

--Check for unwanted sapce in string columns
-- Expectations := No results

select sls_ord_num
FROM bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num)

--Invalid date format

select nullif(sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <=0 
or sls_order_dt > 20500101 
or sls_order_dt < 19000101 
or length(sls_order_dt::text) <> 8


select nullif(sls_due_dt,0) sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <=0 
or sls_due_dt > 20500101 
or sls_due_dt < 19000101 
or length(sls_due_dt::text) <> 8

--invalid date order (Order Date > Shipping/Due Dates)
-- Expectation: No Results
select sls_order_dt
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_ship_dt > sls_due_dt

-- data consistency btw sales, price and quantity coulum 
-- cuz sales = (quantity * Price) 
-- & these column values can't be null, 0 or -ve.

SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================

-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today

select bdate 
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > now()

-- Data Standardization & Consistency
SELECT DISTINCT  gen 
FROM silver.erp_cust_az12;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- Data Standardization & Consistency

SELECT DISTINCT cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results

SELECT * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT maintenance 
FROM silver.erp_px_cat_g1v2;



