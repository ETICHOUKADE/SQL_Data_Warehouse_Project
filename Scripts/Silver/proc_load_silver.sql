/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    call Silver.load_silver;
===============================================================================
*/
create or replace procedure silver.load_silver()
language plpgsql
as $$
Declare 
 	start_time Timestamp;
	End_time Timestamp;
	exec_time numeric;
	
	table_start_time TIMESTAMP;
    table_end_time TIMESTAMP;
    table_exec_time NUMERIC;
BEGIN 
		Begin --(try Block)
		Raise Notice '==========================================================';
		Raise Notice 'Loading Silver Layer';
        Raise Notice '==========================================================';
		
		Raise Notice '-----------------------------------------------------------';
		Raise Notice 'Loading CRM Tables';
		Raise Notice '-----------------------------------------------------------';
		
		start_time = clock_timestamp();
		Raise Notice '<<Truncating Table silver.crm_cust_info>>';
		Truncate table silver.crm_cust_info;
		Raise Notice '<<Inserting Data into silver.crm_cust_info>>';

		insert into silver.crm_cust_info 
			( cst_id,
			  cst_key,
			  cst_firstname,
			  cst_lastname,
			  cst_marital_status,
			  cst_gndr,
			  cst_create_date
			)

		select 
				cst_id,
				cst_key,
				trim(cst_firstname) as cst_firstname,
				trim(cst_lastname) as cst_lastname,
				case when upper(cst_marital_status) = 'S' then 'Single'
					 when upper(cst_marital_status) = 'M' then 'Married'
					 else 'N/A'
				end as cst_marital_status,

				case when upper(cst_gndr) = 'M' then 'Male'
					 when upper(cst_gndr) = 'F' then 'Female'
					 else 'N/A'
				end as cst_gndr,
				cst_create_date
		from (
				SELECT *,
				row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
				FROM bronze.crm_cust_info 
			 )
		where flag_last = 1;
		
		end_time = clock_timestamp();
		
		exec_time = Extract(Epoch from (end_time - start_time));
		
		Raise Notice 'Execution Time: % seconds',exec_time;
		
		----------------------------------------------
		
		table_start_time := clock_timestamp();

		Raise Notice '<<Truncating Table silver.crm_prd_info>>';
		Truncate table silver.crm_prd_info;
		Raise Notice '<<Inserting Data into silver.crm_prd_info>>';

		insert into silver.crm_prd_info
			(prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
			)
		select 
			prd_id,
			replace(substring(prd_key,1,5),'-','_') as cat_id,
			substring(prd_key,7, length(prd_key)) as prd_key,
			prd_nm,
			coalesce(prd_cost,0) as prd_cost,

			case upper(trim(prd_line))
				 when 'M' then 'Mountain'
				 when 'R' then 'Road'
				 when 'S' then 'Other sales'
				 when 'T' then 'Touring'
				 Else 'N/A'
			end as prd_line,

			cast(prd_start_dt as date),
			cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt

		from bronze.crm_prd_info;
		
		end_time = clock_timestamp();
		
		exec_time = Extract(Epoch from (end_time - start_time));
		
		Raise Notice 'Execution Time: % seconds',exec_time;
		
		---------------------------------------
		
		table_start_time := clock_timestamp();

		Raise Notice '<<Truncating Table silver.crm_sales_details>>';
		Truncate table silver.crm_sales_details;
		Raise Notice '<<Inserting Data into silver.crm_sales_details>>';

		insert into silver.crm_sales_details 
				(sls_ord_num,
				 sls_prd_key,
				 sls_cust_id,
				 sls_order_dt,
				 sls_ship_dt,
				 sls_due_dt,
				 sls_sales,
				 sls_quantity,
				 sls_price
				)
		select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case when sls_order_dt = 0 or length(sls_order_dt::text)<>8
				 then null
				 else cast(cast(sls_order_dt as varchar) as date) 
			end as sls_order_dt,

			case when sls_ship_dt = 0 or length(sls_ship_dt::text)<>8
				 then null
				 else cast(cast(sls_ship_dt as varchar) as date) 
			end as sls_ship_dt,

			case when sls_due_dt = 0 or length(sls_due_dt::text)<>8
				 then null
				 else cast(cast(sls_due_dt as varchar) as date) 
			end as sls_due_dt,

			case when sls_sales is null or sls_sales<=0 or sls_sales!= sls_quantity* abs(sls_price)
				 then sls_quantity * abs(sls_price)
				 else sls_sales
			end as sls_sales,

			sls_quantity,

			case when sls_price is null or sls_price<=0 
				 then sls_sales / nullif(sls_quantity,0)
				 else sls_price
			end as sls_price

		from bronze.crm_sales_details;
		
		end_time = clock_timestamp();
		
		exec_time = Extract(Epoch from (end_time - start_time));
		
		Raise Notice 'Execution Time: % seconds',exec_time;
		
		---------------------------------------
		
		table_start_time := clock_timestamp();

		Raise Notice '<<Truncating Table silver.erp_cust_az12>>';
		Truncate table silver.erp_cust_az12;
		Raise Notice '<<Inserting Data into silver.erp_cust_az12>>';

		insert into silver.erp_cust_az12
			(cid,
			 bdate,
			 gen)

		select 
			case when cid like 'NAS%' then substring(cid,4,length(cid))
				 else cid
			end as cid,

			case when bdate > now() then NULL
				 else bdate
			end as bdate,

			case when upper(trim(gen)) = 'F' then 'Female'
				 when upper(trim(gen)) = 'M' then 'Male'
				 when trim(gen) is null or trim(gen) = '' then 'N/A'
				 else trim(gen)
			end as gen

		from bronze.erp_cust_az12;
		
		end_time = clock_timestamp();
		
		exec_time = Extract(Epoch from (end_time - start_time));
		
		Raise Notice 'Execution Time: % seconds',exec_time;
		
		---------------------------------------

		table_start_time := clock_timestamp();

		Raise Notice '<<Truncating Table silver.erp_loc_a101>>';
		Truncate table silver.erp_loc_a101;
		Raise Notice '<<Inserting Data into silver.erp_loc_a101>>';

		insert into silver.erp_loc_a101
			(cid,
			 cntry
			)
		select 
			replace(cid,'-','') cid,

			case when trim(cntry) = 'DE' then 'Germany'
				 when trim(cntry) in ('US','USA') then 'United States'
				 when trim(cntry) is null or trim(cntry) = '' then 'N/A'
				 else trim(cntry)
			end as cntry

		from bronze.erp_loc_a101;
		
		end_time = clock_timestamp();
		
		exec_time = Extract(Epoch from (end_time - start_time));
		
		Raise Notice 'Execution Time: % seconds',exec_time;
		
		---------------------------------------

		table_start_time := clock_timestamp();

		Raise Notice '<<Truncating Table silver.erp_px_cat_g1v2>>';
		Truncate table silver.erp_px_cat_g1v2;
		Raise Notice '<<Inserting Data into silver.erp_px_cat_g1v2>>';

		insert into silver.erp_px_cat_g1v2
			(id, 
			 cat, 
			 subcat,
			 maintenance
			)
		select 
			id,
			cat,
			subcat,
			maintenance
		from bronze.erp_px_cat_g1v2;
		
		end_time = clock_timestamp();
		
		exec_time = Extract(Epoch from (end_time - start_time));
		
		Raise Notice 'Execution Time: % seconds',exec_time;
		Raise Notice '==========================================================';
		Raise Notice 'Data Loaded Succesfully';
        Raise Notice '==========================================================';
	END;
		
	 exception -- (catch Block)
	     when others then 
		      Raise Notice '================================================';
		      Raise Notice 'Error occurred during loading silver layer';
			  raise notice 'Error_message: %',SQLERRM;
			  raise notice 'Error code : %',SQLSTATE;
			  Raise Notice '================================================';
END;
$$;
