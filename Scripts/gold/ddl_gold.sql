/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

DROP VIEW IF EXISTS gold.dim_customers;
CREATE VIEW gold.dim_customers as
SELECT
  ROW_NUMBER() OVER (ORDER BY cst_id) as customer_key,
	ci.cst_id customer_id,
	ci.cst_key customer_number,
	ci.cst_firstname first_name,
	ci.cst_lastname last_name,
	la.cntry country,
	ci.cst_marital_status marital_status,
CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr  --CRM IS MASTER FOR GENDER INFO 
	ELSE COALESCE (ca.gen, 'N/A')
END AS gender,
  ca.bdate birthdate,
	ci.cst_create_date create_date
from Silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la 
ON ci.cst_key = la.cid

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

DROP VIEW IF EXISTS gold.dim_products;
CREATE VIEW gold.dim_products as
SELECT 
    ROW_NUMBER() OVER ( ORDER BY pr.prd_start_dt, pr.prd_key) product_key,
	pr.prd_id product_id,
	pr.prd_key product_number,
	pr.prd_nm product_name,
	pr.cat_id category_id,
	cat.cat category,
	cat.subcat subcategory,
	cat.maintenance,
	pr.prd_cost cost,
	pr.prd_line product_line,
	pr.prd_start_dt start_date
FROM silver.crm_prd_info pr
LEFT JOIN SILVER.ERP_PX_CAT_G1V2 cat 
ON pr.cat_id = cat.id
WHERE pr.prd_end_dt is NULL --FILTER OUT ALL HISTORICAL DATA

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================

DROP VIEW IF EXISTS gold.fact_sales;
CREATE VIEW gold.fact_sales as
SELECT 
sd.sls_ord_num order_number,
pr.product_key,
cus.customer_key,
sd.sls_order_dt order_date,
sd.sls_ship_dt shipping_date,
sd.sls_due_dt due_date,
sd.sls_sales sales_amount,
sd.sls_quantity quantity,
sd.sls_price price
FROM silver.crm_sales_details sd 
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cus
ON sd.sls_cust_id = cus.customer_id






