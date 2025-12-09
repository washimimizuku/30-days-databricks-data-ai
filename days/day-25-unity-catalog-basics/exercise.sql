-- Day 25: Unity Catalog Basics - Exercises
-- Complete these exercises to practice Unity Catalog concepts
-- Replace <your_username> with your actual username throughout

-- ============================================================================
-- PART 1: CATALOG AND SCHEMA BASICS (10 exercises)
-- ============================================================================

-- Exercise 1: Show all catalogs in your metastore
-- TODO: Write query to list all catalogs


-- Exercise 2: Show the current metastore name
-- TODO: Write query to display current metastore


-- Exercise 3: Create a new catalog for testing
-- TODO: Create catalog named 'test_catalog_<your_username>'


-- Exercise 4: Show all schemas in your day25 catalog
-- TODO: Write query to list schemas in uc_day25_<your_username>


-- Exercise 5: Create a new schema called 'staging' in your catalog
-- TODO: Create schema with comment 'Staging area for data processing'


-- Exercise 6: Set your default catalog and schema
-- TODO: Set default to uc_day25_<your_username>.bronze


-- Exercise 7: Check your current catalog and schema
-- TODO: Write queries to show current_catalog() and current_schema()


-- Exercise 8: Describe your bronze schema
-- TODO: Write query to describe the bronze schema


-- Exercise 9: Show all tables in the bronze schema
-- TODO: List all tables in uc_day25_<your_username>.bronze


-- Exercise 10: Get detailed information about the customers table
-- TODO: Use DESCRIBE EXTENDED on bronze.customers


-- ============================================================================
-- PART 2: THREE-LEVEL NAMESPACE (10 exercises)
-- ============================================================================

-- Exercise 11: Query customers using fully qualified name
-- TODO: SELECT * from customers using catalog.schema.table format


-- Exercise 12: Query orders from bronze using short name
-- TODO: First USE the bronze schema, then SELECT * from orders


-- Exercise 13: Join tables across schemas using full names
-- TODO: Join bronze.customers with silver.customers_clean on customer_id


-- Exercise 14: Create a view in silver schema with full namespace
-- TODO: Create view silver.customer_emails selecting customer_id and email from bronze.customers


-- Exercise 15: Query the view you just created
-- TODO: SELECT * from the customer_emails view


-- Exercise 16: Create a table in staging schema from bronze data
-- TODO: Create staging.temp_orders AS SELECT * FROM bronze.orders WHERE status = 'pending'


-- Exercise 17: Count rows in each layer's customer table
-- TODO: Use UNION ALL to count rows in bronze.customers, silver.customers_clean


-- Exercise 18: Create a cross-schema aggregation
-- TODO: Count orders by status from bronze.orders, group by status


-- Exercise 19: Use current_catalog() in a query
-- TODO: SELECT current_catalog(), current_schema(), COUNT(*) FROM bronze.customers


-- Exercise 20: Switch between schemas and query
-- TODO: USE silver schema, then SELECT * FROM customers_clean LIMIT 5


-- ============================================================================
-- PART 3: ACCESS CONTROL - GRANTING PERMISSIONS (10 exercises)
-- ============================================================================

-- Exercise 21: Grant USE CATALOG permission
-- TODO: GRANT USE CATALOG on your catalog TO `users` group
-- Note: Replace `users` with an actual group in your workspace


-- Exercise 22: Grant USE SCHEMA permission on bronze
-- TODO: GRANT USE SCHEMA on bronze TO `users`


-- Exercise 23: Grant SELECT permission on customers table
-- TODO: GRANT SELECT on bronze.customers TO `users`


-- Exercise 24: Grant MODIFY permission on staging schema
-- TODO: GRANT MODIFY on staging.temp_orders TO `data_engineers`


-- Exercise 25: Grant ALL PRIVILEGES on gold schema
-- TODO: GRANT ALL PRIVILEGES on gold schema TO `admins`


-- Exercise 26: Show grants on your catalog
-- TODO: SHOW GRANTS ON CATALOG uc_day25_<your_username>


-- Exercise 27: Show grants on bronze.customers table
-- TODO: SHOW GRANTS ON TABLE bronze.customers


-- Exercise 28: Grant SELECT on all tables in silver schema
-- TODO: GRANT SELECT ON SCHEMA silver TO `analysts`


-- Exercise 29: Grant CREATE TABLE permission on staging schema
-- TODO: GRANT CREATE TABLE ON SCHEMA staging TO `data_engineers`


-- Exercise 30: Show grants for a specific principal
-- TODO: SHOW GRANTS `users` ON CATALOG uc_day25_<your_username>


-- ============================================================================
-- PART 4: REVOKING PERMISSIONS (5 exercises)
-- ============================================================================

-- Exercise 31: Revoke SELECT permission from a table
-- TODO: REVOKE SELECT ON TABLE bronze.customers FROM `users`


-- Exercise 32: Revoke MODIFY permission from staging
-- TODO: REVOKE MODIFY ON TABLE staging.temp_orders FROM `data_engineers`


-- Exercise 33: Revoke USE SCHEMA from bronze
-- TODO: REVOKE USE SCHEMA ON SCHEMA bronze FROM `users`


-- Exercise 34: Revoke ALL PRIVILEGES from gold
-- TODO: REVOKE ALL PRIVILEGES ON SCHEMA gold FROM `admins`


-- Exercise 35: Verify revocation by showing grants
-- TODO: SHOW GRANTS ON TABLE bronze.customers


-- ============================================================================
-- PART 5: TABLE PROPERTIES AND COMMENTS (10 exercises)
-- ============================================================================

-- Exercise 36: Add a comment to the orders table
-- TODO: COMMENT ON TABLE bronze.orders IS 'Raw order data from e-commerce platform'


-- Exercise 37: Add a comment to the order_date column
-- TODO: ALTER TABLE bronze.orders ALTER COLUMN order_date COMMENT 'Date when order was placed'


-- Exercise 38: Set table properties for data classification
-- TODO: ALTER TABLE bronze.customers SET TBLPROPERTIES ('data_classification' = 'confidential', 'contains_pii' = 'true')


-- Exercise 39: Show table properties for customers
-- TODO: SHOW TBLPROPERTIES bronze.customers


-- Exercise 40: Add owner property to products table
-- TODO: ALTER TABLE bronze.products SET TBLPROPERTIES ('owner' = 'product_team')


-- Exercise 41: Describe the customers table to see comments
-- TODO: DESCRIBE TABLE bronze.customers


-- Exercise 42: Add multiple properties to order_items
-- TODO: Set properties: 'refresh_frequency' = 'hourly', 'data_source' = 'order_system'


-- Exercise 43: Create a new table with comment
-- TODO: CREATE TABLE staging.test_data (id INT, name STRING) COMMENT 'Test table for experiments'


-- Exercise 44: Show extended information for gold.customer_summary
-- TODO: DESCRIBE TABLE EXTENDED gold.customer_summary


-- Exercise 45: Update an existing table property
-- TODO: ALTER TABLE bronze.customers SET TBLPROPERTIES ('data_classification' = 'highly_confidential')


-- ============================================================================
-- PART 6: MANAGED VS EXTERNAL TABLES (5 exercises)
-- ============================================================================

-- Exercise 46: Create a managed table in staging
-- TODO: CREATE TABLE staging.managed_test (id INT, value STRING) USING DELTA


-- Exercise 47: Insert data into managed table
-- TODO: INSERT INTO staging.managed_test VALUES (1, 'test'), (2, 'data')


-- Exercise 48: Check if table is managed or external
-- TODO: DESCRIBE EXTENDED staging.managed_test and look for 'Type' property


-- Exercise 49: Create another managed table from query
-- TODO: CREATE TABLE staging.customer_backup AS SELECT * FROM bronze.customers


-- Exercise 50: Show the location of a managed table
-- TODO: DESCRIBE EXTENDED staging.customer_backup and find 'Location'


-- Note: External table exercises require storage credentials and external locations
-- which are typically admin-only operations. See solution.sql for examples.


-- ============================================================================
-- PART 7: DATA DISCOVERY (10 exercises)
-- ============================================================================

-- Exercise 51: Search for tables with 'customer' in the name
-- TODO: SHOW TABLES IN uc_day25_<your_username> LIKE '*customer*'


-- Exercise 52: Search for tables in bronze schema starting with 'order'
-- TODO: SHOW TABLES IN bronze LIKE 'order*'


-- Exercise 53: List all tables across all schemas in your catalog
-- TODO: SELECT * FROM system.information_schema.tables WHERE table_catalog = 'uc_day25_<your_username>'


-- Exercise 54: Find all tables with PII
-- TODO: Query system tables or use SHOW TBLPROPERTIES to find tables where contains_pii = 'true'


-- Exercise 55: Get column information for customers table
-- TODO: DESCRIBE TABLE bronze.customers


-- Exercise 56: Show detailed column info including comments
-- TODO: SELECT * FROM system.information_schema.columns WHERE table_name = 'customers'


-- Exercise 57: Find all tables in gold schema
-- TODO: SHOW TABLES IN gold


-- Exercise 58: Search for tables by comment
-- TODO: DESCRIBE TABLE EXTENDED bronze.orders and check comment


-- Exercise 59: List all schemas with their comments
-- TODO: SHOW SCHEMAS IN uc_day25_<your_username>


-- Exercise 60: Find tables modified in the last 7 days
-- TODO: Use DESCRIBE EXTENDED or system tables to check last_modified


-- ============================================================================
-- PART 8: CROSS-CATALOG QUERIES (5 exercises)
-- ============================================================================

-- Exercise 61: Query your catalog and another catalog (if available)
-- TODO: SELECT COUNT(*) FROM uc_day25_<your_username>.bronze.customers
-- UNION ALL SELECT COUNT(*) FROM other_catalog.schema.table


-- Exercise 62: Create a view that references multiple catalogs
-- TODO: CREATE VIEW gold.cross_catalog_summary AS 
-- SELECT * FROM uc_day25_<your_username>.gold.customer_summary


-- Exercise 63: Join tables from different catalogs
-- TODO: If you have access to multiple catalogs, join tables across them


-- Exercise 64: Copy data from one catalog to another
-- TODO: CREATE TABLE test_catalog_<your_username>.default.customers_copy AS
-- SELECT * FROM uc_day25_<your_username>.bronze.customers


-- Exercise 65: Compare table counts across catalogs
-- TODO: Count rows in tables from different catalogs using UNION ALL


-- ============================================================================
-- PART 9: SYSTEM TABLES (5 exercises)
-- ============================================================================

-- Exercise 66: Query audit logs for table access
-- TODO: SELECT * FROM system.access.audit 
-- WHERE action_name = 'getTable' AND event_date >= current_date() - 1
-- LIMIT 10


-- Exercise 67: Query table lineage
-- TODO: SELECT * FROM system.access.table_lineage
-- WHERE target_table_full_name LIKE '%customer_summary%'


-- Exercise 68: Query billing usage
-- TODO: SELECT * FROM system.billing.usage
-- WHERE usage_date >= current_date() - 7
-- ORDER BY usage_date DESC LIMIT 10


-- Exercise 69: Find who accessed your tables
-- TODO: SELECT user_identity.email, action_name, event_time
-- FROM system.access.audit
-- WHERE request_params.full_name_arg LIKE '%uc_day25_%'


-- Exercise 70: Query table metadata from information schema
-- TODO: SELECT table_catalog, table_schema, table_name, table_type
-- FROM system.information_schema.tables
-- WHERE table_catalog = 'uc_day25_<your_username>'


-- ============================================================================
-- PART 10: ADVANCED SCENARIOS (10 exercises)
-- ============================================================================

-- Exercise 71: Create a multi-layer data pipeline
-- TODO: Create bronze table, transform to silver, aggregate to gold
-- All using proper three-level namespace


-- Exercise 72: Implement row-level security with views
-- TODO: CREATE VIEW gold.my_region_sales AS
-- SELECT * FROM gold.daily_sales WHERE region = current_user()


-- Exercise 73: Create a schema for each environment
-- TODO: CREATE SCHEMA dev, CREATE SCHEMA staging, CREATE SCHEMA prod


-- Exercise 74: Clone a table across schemas
-- TODO: CREATE TABLE silver.customers_clone SHALLOW CLONE bronze.customers


-- Exercise 75: Set up a data sharing pattern
-- TODO: Create a shared schema and grant appropriate permissions


-- Exercise 76: Implement a permission hierarchy
-- TODO: Grant permissions at catalog level, schema level, and table level
-- Show how they inherit


-- Exercise 77: Create a governance report
-- TODO: Query system tables to create a report of all tables, owners, and classifications


-- Exercise 78: Implement a naming convention check
-- TODO: Query information_schema to find tables not following naming conventions


-- Exercise 79: Create a data quality metadata table
-- TODO: CREATE TABLE gold.table_metadata with columns for table_name, row_count, last_updated


-- Exercise 80: Build a permission audit query
-- TODO: Create a query that shows all permissions granted in your catalog


-- ============================================================================
-- BONUS CHALLENGES (5 exercises)
-- ============================================================================

-- Bonus 1: Create a complete medallion architecture
-- TODO: Create bronze, silver, gold layers with proper permissions for each


-- Bonus 2: Implement a data catalog
-- TODO: Create tables to store metadata about all your data assets


-- Bonus 3: Build a lineage tracking system
-- TODO: Create tables to manually track data lineage (source → target)


-- Bonus 4: Create a permission management system
-- TODO: Build views that show who has access to what


-- Bonus 5: Implement a data quality framework
-- TODO: Create tables and views for tracking data quality metrics


-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Run these to verify your work:

-- 1. Count objects created
SELECT 'Catalogs' as object_type, COUNT(*) as count 
FROM system.information_schema.catalogs 
WHERE catalog_name LIKE '%day25%'
UNION ALL
SELECT 'Schemas', COUNT(*) 
FROM system.information_schema.schemata 
WHERE catalog_name LIKE '%day25%'
UNION ALL
SELECT 'Tables', COUNT(*) 
FROM system.information_schema.tables 
WHERE table_catalog LIKE '%day25%';

-- 2. List all your tables with full names
SELECT 
    CONCAT(table_catalog, '.', table_schema, '.', table_name) as full_table_name,
    table_type
FROM system.information_schema.tables
WHERE table_catalog LIKE '%day25%'
ORDER BY table_schema, table_name;

-- 3. Check table properties
SHOW TBLPROPERTIES bronze.customers;

-- 4. Verify permissions (if you have access)
SHOW GRANTS ON CATALOG uc_day25_<your_username>;

-- ============================================================================
-- END OF EXERCISES
-- ============================================================================

-- Great job! You've completed 85 exercises covering:
-- ✅ Catalog and schema management
-- ✅ Three-level namespace
-- ✅ Access control (GRANT/REVOKE)
-- ✅ Table properties and comments
-- ✅ Managed vs external tables
-- ✅ Data discovery
-- ✅ Cross-catalog queries
-- ✅ System tables
-- ✅ Advanced governance scenarios
-- ✅ Bonus challenges

-- Next: Check solution.sql for complete solutions and explanations
