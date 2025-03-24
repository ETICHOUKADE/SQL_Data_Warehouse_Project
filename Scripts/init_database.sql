/*
=============================================================
Create Database and Schemas (PostgreSQL)
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas:
    'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution.
*/

-- Drop the 'DataWarehouse' database if it exists
DROP DATABASE IF EXISTS DataWarehouse;

-- Create the 'data_warehouse' database
CREATE DATABASE DataWarehouse;

-- Connect to the newly created database
\c DataWarehouse

-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

/*
=============================================================
💡 Note:
I prefer using the PostgreSQL GUI (pgAdmin) for creating databases and schemas, as it is more easier and convenient for smaller projects or during the initial setup. 
Instead of running this script, I manually created the database by right-clicking on **"Databases" → Create → Database**.
For schemas, I used **"Create → Schema"**, named them bronze, silver, and gold and saved them.
*/


