/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/
DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info 
(  cst_id  INT,
   cst_key  VARCHAR(50),
   cst_firstname  VARCHAR(50),
   cst_lastname   VARCHAR(50),
   cst_marital_status VARCHAR(50),
   cst_gndr VARCHAR(50),
   cst_create_date  Date,
   dwh_create_date  timestamp default now()
);

DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info 
(  prd_id  INT,
   cat_id   VARCHAR(50),
   prd_key  VARCHAR(50),
   prd_nm  VARCHAR(50),
   prd_cost   INT,
   prd_line VARCHAR(50),
   prd_start_dt DATE,
   prd_end_dt  Date,
   dwh_create_date  timestamp default now()
);

DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details 
(  sls_ord_num  VARCHAR(50),
   sls_prd_key  VARCHAR(50),
   sls_cust_id  INT,
   sls_order_dt  Date,
   sls_ship_dt Date,
   sls_due_dt Date,
   sls_sales  INT,
   sls_quantity INT,
   sls_price   INT,
   dwh_create_date  timestamp default now()
);

DROP TABLE IF EXISTS erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 
(  cid  VARCHAR(50),
   bdate  DATE,
   gen  varchar(10),
   dwh_create_date  timestamp default now()
);

DROP TABLE IF EXISTS erp_loc_a101;
CREATE TABLE silver.erp_loc_a101
(   cid  VARCHAR(50),
    cntry varCHar(50),
   dwh_create_date  timestamp default now()
);

DROP TABLE IF EXISTS erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2
(   id  VARCHAR(10),
    cat  VARCHAR(50),
    subcat  VARCHAR(50),
    maintenance  VARCHAR(5),
   dwh_create_date  timestamp default now()
);
