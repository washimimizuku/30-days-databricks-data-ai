-- Day 25: Unity Catalog Basics - Solutions
-- Complete solutions with explanations
-- Replace <your_username> with your actual username throughout

-- ============================================================================
-- PART 1: CATALOG AND SCHEMA BASICS (10 exercises)
-- ============================================================================

-- Exercise 1: Show all catalogs in your metastore
SHOW CATALOGS;
-- Lists all catalogs you have access to in the current metastore

-- Exercise 2: Show the current metastore name
SELECT current_metastore();
-- Returns the name of the metastore attached to this workspace

-- Exercise 3: Create a new catalog for testing
CREATE CATALOG IF NOT EXISTS test_catalog_<your_username>
COMMENT 'Test catalog for Unity Catalog exercises';
-- Creates a new catalog at the top level of the hierarchy

-- Exercise 4: Show all schemas in your day25 catalog
SHOW SCHEMAS IN uc_day25_<your_username>;
-- Lists all schemas within the specified catalog

-- Exercise 5: Create a new schema called 'staging' in your catalog
CREATE SCHEMA IF NOT EXISTS uc_day25_<your_username>.staging
COMMENT 'Staging area for data processing';
-- Creates a schema within the catalog for temporary/staging data

-- Exercise 6: Set your default catalog and schema
USE CATALOG uc_day25_<your_username>;
USE SCHEMA bronze;
-- Sets the default context so you can use short table names

-- Exercise 7: Check your current catalog and schema
SELECT current_catalog() as catalog, current_schema() as schema;
-- Shows which catalog and schema are currently active

-- Exercise 8: Describe your bronze schema
DESCRIBE SCHEMA uc_day25_<your_username>.bronze;
-- Shows metadata about the schema including location and properties

-- Exercise 9: Show all tables in the bronze schema
SHOW TABLES IN uc_day25_<your_username>.bronze;
-- Lists all tables and views in the bronze schema

-- Exercise 10: Get detailed information about the customers table
DESCRIBE EXTENDED uc_day25_<your_username>.bronze.customers;
-- Shows detailed table information including schema, partitions, location, properties

-- ============================================================================
-- PART 2: THREE-LEVEL NAMESPACE (10 exercises)
-- ============================================================================

-- Exercise 11: Query customers using fully qualified name
SELECT * FROM uc_day25_<your_username>.bronze.customers LIMIT 10;
-- Always use catalog.schema.table format for clarity and to avoid ambiguity

-- Exercise 12: Query orders from bronze using short name
USE SCHEMA uc_day25_<your_username>.bronze;
SELECT * FROM orders LIMIT 10;
-- After setting default schema, you can use just the table name

-- Exercise 13: Join tables across schemas using full names
SELECT 
    b.customer_id,
    b.first_name,
    b.last_name,
    s.full_name,
    s.email
FROM uc_day25_<your_username>.bronze.customers b
JOIN uc_day25_<your_username>.silver.customers_clean s
    ON b.customer_id = s.customer_id;
-- Full names required when joining across schemas

-- Exercise 14: Create a view in silver schema with full namespace
CREATE OR REPLACE VIEW uc_day25_<your_username>.silver.customer_emails AS
SELECT 
    customer_id,
    email,
    CONCAT(first_name, ' ', last_name) as full_name
FROM uc_day25_<your_username>.bronze.customers;
-- Views can reference tables from any schema using full names

-- Exercise 15: Query the view you just created
SELECT * FROM uc_day25_<your_username>.silver.customer_emails;
-- Views are queried just like tables

-- Exercise 16: Create a table in staging schema from bronze data
CREATE OR REPLACE TABLE uc_day25_<your_username>.staging.temp_orders AS
SELECT * 
FROM uc_day25_<your_username>.bronze.orders 
WHERE status = 'pending';
-- CTAS (Create Table As Select) with full namespace

-- Exercise 17: Count rows in each layer's customer table
SELECT 'bronze' as layer, COUNT(*) as row_count 
FROM uc_day25_<your_username>.bronze.customers
UNION ALL
SELECT 'silver', COUNT(*) 
FROM uc_day25_<your_username>.silver.customers_clean;
-- Compare data across layers

-- Exercise 18: Create a cross-schema aggregation
SELECT 
    status,
    COUNT(*) as order_count,
    SUM(order_amount) as total_amount
FROM uc_day25_<your_username>.bronze.orders
GROUP BY status
ORDER BY order_count DESC;
-- Aggregations work the same with three-level namespace

-- Exercise 19: Use current_catalog() in a query
SELECT 
    current_catalog() as catalog,
    current_schema() as schema,
    COUNT(*) as customer_count
FROM uc_day25_<your_username>.bronze.customers;
-- Useful for debugging and logging which catalog/schema you're using

-- Exercise 20: Switch between schemas and query
USE SCHEMA uc_day25_<your_username>.silver;
SELECT * FROM customers_clean LIMIT 5;
-- After USE SCHEMA, short names resolve to that schema

-- ============================================================================
-- PART 3: ACCESS CONTROL - GRANTING PERMISSIONS (10 exercises)
-- ============================================================================

-- Exercise 21: Grant USE CATALOG permission
GRANT USE CATALOG ON CATALOG uc_day25_<your_username> TO `users`;
-- Users need USE CATALOG to see the catalog exists
-- Note: Replace `users` with actual group name in your workspace

-- Exercise 22: Grant USE SCHEMA permission on bronze
GRANT USE SCHEMA ON SCHEMA uc_day25_<your_username>.bronze TO `users`;
-- Users need USE SCHEMA to see tables in the schema

-- Exercise 23: Grant SELECT permission on customers table
GRANT SELECT ON TABLE uc_day25_<your_username>.bronze.customers TO `users`;
-- Now users can query the customers table
-- They need all three: USE CATALOG + USE SCHEMA + SELECT

-- Exercise 24: Grant MODIFY permission on staging schema
GRANT MODIFY ON TABLE uc_day25_<your_username>.staging.temp_orders TO `data_engineers`;
-- MODIFY allows INSERT, UPDATE, DELETE operations

-- Exercise 25: Grant ALL PRIVILEGES on gold schema
GRANT ALL PRIVILEGES ON SCHEMA uc_day25_<your_username>.gold TO `admins`;
-- ALL PRIVILEGES includes SELECT, MODIFY, CREATE, DROP, etc.

-- Exercise 26: Show grants on your catalog
SHOW GRANTS ON CATALOG uc_day25_<your_username>;
-- Lists all permissions granted on the catalog

-- Exercise 27: Show grants on bronze.customers table
SHOW GRANTS ON TABLE uc_day25_<your_username>.bronze.customers;
-- Shows who has what permissions on this specific table

-- Exercise 28: Grant SELECT on all tables in silver schema
GRANT SELECT ON SCHEMA uc_day25_<your_username>.silver TO `analysts`;
-- Grants SELECT on all current and future tables in the schema

-- Exercise 29: Grant CREATE TABLE permission on staging schema
GRANT CREATE TABLE ON SCHEMA uc_day25_<your_username>.staging TO `data_engineers`;
-- Allows creating new tables in the staging schema

-- Exercise 30: Show grants for a specific principal
SHOW GRANTS `users` ON CATALOG uc_day25_<your_username>;
-- Shows all permissions that the 'users' group has on this catalog

-- ============================================================================
-- PART 4: REVOKING PERMISSIONS (5 exercises)
-- ============================================================================

-- Exercise 31: Revoke SELECT permission from a table
REVOKE SELECT ON TABLE uc_day25_<your_username>.bronze.customers FROM `users`;
-- Removes SELECT permission from the users group

-- Exercise 32: Revoke MODIFY permission from staging
REVOKE MODIFY ON TABLE uc_day25_<your_username>.staging.temp_orders FROM `data_engineers`;
-- Removes ability to modify data

-- Exercise 33: Revoke USE SCHEMA from bronze
REVOKE USE SCHEMA ON SCHEMA uc_day25_<your_username>.bronze FROM `users`;
-- Users can no longer see tables in bronze schema

-- Exercise 34: Revoke ALL PRIVILEGES from gold
REVOKE ALL PRIVILEGES ON SCHEMA uc_day25_<your_username>.gold FROM `admins`;
-- Removes all permissions at once

-- Exercise 35: Verify revocation by showing grants
SHOW GRANTS ON TABLE uc_day25_<your_username>.bronze.customers;
-- Verify that permissions were removed

-- ============================================================================
-- PART 5: TABLE PROPERTIES AND COMMENTS (10 exercises)
-- ============================================================================

-- Exercise 36: Add a comment to the orders table
COMMENT ON TABLE uc_day25_<your_username>.bronze.orders IS 
'Raw order data from e-commerce platform. Updated hourly via batch ingestion.';
-- Comments help with data discovery and documentation

-- Exercise 37: Add a comment to the order_date column
ALTER TABLE uc_day25_<your_username>.bronze.orders 
ALTER COLUMN order_date COMMENT 'Date when order was placed (UTC timezone)';
-- Column-level comments provide field-level documentation

-- Exercise 38: Set table properties for data classification
ALTER TABLE uc_day25_<your_username>.bronze.customers 
SET TBLPROPERTIES (
    'data_classification' = 'confidential',
    'contains_pii' = 'true'
);
-- Properties are key-value pairs for metadata

-- Exercise 39: Show table properties for customers
SHOW TBLPROPERTIES uc_day25_<your_username>.bronze.customers;
-- Displays all custom properties set on the table

-- Exercise 40: Add owner property to products table
ALTER TABLE uc_day25_<your_username>.bronze.products 
SET TBLPROPERTIES ('owner' = 'product_team');
-- Track ownership for governance

-- Exercise 41: Describe the customers table to see comments
DESCRIBE TABLE uc_day25_<your_username>.bronze.customers;
-- Shows schema with column comments

-- Exercise 42: Add multiple properties to order_items
ALTER TABLE uc_day25_<your_username>.bronze.order_items 
SET TBLPROPERTIES (
    'refresh_frequency' = 'hourly',
    'data_source' = 'order_system',
    'sla' = '2_hours'
);
-- Multiple properties can be set at once

-- Exercise 43: Create a new table with comment
CREATE TABLE uc_day25_<your_username>.staging.test_data (
    id INT COMMENT 'Unique identifier',
    name STRING COMMENT 'Test name',
    created_at TIMESTAMP COMMENT 'Creation timestamp'
) COMMENT 'Test table for experiments - can be dropped anytime';
-- Add comments during table creation

-- Exercise 44: Show extended information for gold.customer_summary
DESCRIBE TABLE EXTENDED uc_day25_<your_username>.gold.customer_summary;
-- EXTENDED shows all metadata including location, properties, statistics

-- Exercise 45: Update an existing table property
ALTER TABLE uc_day25_<your_username>.bronze.customers 
SET TBLPROPERTIES ('data_classification' = 'highly_confidential');
-- Properties can be updated by setting them again

-- ============================================================================
-- PART 6: MANAGED VS EXTERNAL TABLES (5 exercises)
-- ============================================================================

-- Exercise 46: Create a managed table in staging
CREATE TABLE uc_day25_<your_username>.staging.managed_test (
    id INT,
    value STRING,
    created_at TIMESTAMP
) USING DELTA;
-- Managed tables store data in Unity Catalog's managed location

-- Exercise 47: Insert data into managed table
INSERT INTO uc_day25_<your_username>.staging.managed_test VALUES
(1, 'test', current_timestamp()),
(2, 'data', current_timestamp()),
(3, 'sample', current_timestamp());
-- Standard INSERT works for managed tables

-- Exercise 48: Check if table is managed or external
DESCRIBE EXTENDED uc_day25_<your_username>.staging.managed_test;
-- Look for 'Type' field: MANAGED or EXTERNAL
-- Managed tables show Unity Catalog managed location

-- Exercise 49: Create another managed table from query
CREATE TABLE uc_day25_<your_username>.staging.customer_backup AS
SELECT * FROM uc_day25_<your_username>.bronze.customers;
-- CTAS creates managed table by default

-- Exercise 50: Show the location of a managed table
DESCRIBE EXTENDED uc_day25_<your_username>.staging.customer_backup;
-- Location will be in Unity Catalog's managed storage
-- Format: s3://bucket/metastore-id/catalog-id/schema-id/table-id/

-- External Table Examples (Admin operations)
-- Note: These require storage credentials and external locations

-- Example: Create storage credential (admin only)
-- CREATE STORAGE CREDENTIAL my_aws_cred
-- WITH (AWS_IAM_ROLE = 'arn:aws:iam::123456789:role/databricks-role');

-- Example: Create external location (admin only)
-- CREATE EXTERNAL LOCATION my_external_data
-- URL 's3://my-bucket/data/'
-- WITH (STORAGE CREDENTIAL my_aws_cred);

-- Example: Create external table
-- CREATE EXTERNAL TABLE uc_day25_<your_username>.bronze.external_orders (
--     order_id INT,
--     amount DECIMAL(10,2)
-- )
-- LOCATION 's3://my-bucket/data/orders/';

-- ============================================================================
-- PART 7: DATA DISCOVERY (10 exercises)
-- ============================================================================

-- Exercise 51: Search for tables with 'customer' in the name
SHOW TABLES IN uc_day25_<your_username> LIKE '*customer*';
-- Wildcard search across all schemas in catalog

-- Exercise 52: Search for tables in bronze schema starting with 'order'
SHOW TABLES IN uc_day25_<your_username>.bronze LIKE 'order*';
-- Pattern matching for table discovery

-- Exercise 53: List all tables across all schemas in your catalog
SELECT 
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM system.information_schema.tables 
WHERE table_catalog = 'uc_day25_<your_username>'
ORDER BY table_schema, table_name;
-- System tables provide queryable metadata

-- Exercise 54: Find all tables with PII
SELECT 
    CONCAT(table_catalog, '.', table_schema, '.', table_name) as full_name
FROM system.information_schema.tables t
WHERE table_catalog = 'uc_day25_<your_username>'
    AND EXISTS (
        SELECT 1 FROM system.information_schema.table_properties p
        WHERE p.table_catalog = t.table_catalog
            AND p.table_schema = t.table_schema
            AND p.table_name = t.table_name
            AND p.property_name = 'contains_pii'
            AND p.property_value = 'true'
    );
-- Query metadata to find sensitive data

-- Exercise 55: Get column information for customers table
DESCRIBE TABLE uc_day25_<your_username>.bronze.customers;
-- Shows all columns with types and comments

-- Exercise 56: Show detailed column info including comments
SELECT 
    column_name,
    data_type,
    comment
FROM system.information_schema.columns 
WHERE table_catalog = 'uc_day25_<your_username>'
    AND table_schema = 'bronze'
    AND table_name = 'customers'
ORDER BY ordinal_position;
-- Queryable column metadata

-- Exercise 57: Find all tables in gold schema
SHOW TABLES IN uc_day25_<your_username>.gold;
-- List tables in specific schema

-- Exercise 58: Search for tables by comment
DESCRIBE TABLE EXTENDED uc_day25_<your_username>.bronze.orders;
-- Check the Comment field in output

-- Exercise 59: List all schemas with their comments
SHOW SCHEMAS IN uc_day25_<your_username>;
-- Shows schema names and comments

-- Exercise 60: Find tables modified in the last 7 days
SELECT 
    table_catalog,
    table_schema,
    table_name,
    created
FROM system.information_schema.tables
WHERE table_catalog = 'uc_day25_<your_username>'
    AND created >= current_date() - 7
ORDER BY created DESC;
-- Track recently created/modified tables

-- ============================================================================
-- PART 8: CROSS-CATALOG QUERIES (5 exercises)
-- ============================================================================

-- Exercise 61: Query your catalog and another catalog (if available)
SELECT 'uc_day25' as source, COUNT(*) as customer_count
FROM uc_day25_<your_username>.bronze.customers
UNION ALL
SELECT 'test_catalog', COUNT(*)
FROM test_catalog_<your_username>.default.customers_copy;
-- Compare data across catalogs

-- Exercise 62: Create a view that references multiple catalogs
CREATE OR REPLACE VIEW uc_day25_<your_username>.gold.cross_catalog_summary AS
SELECT 
    'main_catalog' as source,
    customer_id,
    full_name,
    total_orders
FROM uc_day25_<your_username>.gold.customer_summary;
-- Views can reference any accessible catalog

-- Exercise 63: Join tables from different catalogs
SELECT 
    a.customer_id,
    a.full_name,
    b.customer_id as backup_id
FROM uc_day25_<your_username>.silver.customers_clean a
LEFT JOIN test_catalog_<your_username>.default.customers_copy b
    ON a.customer_id = b.customer_id;
-- Cross-catalog joins work seamlessly

-- Exercise 64: Copy data from one catalog to another
CREATE OR REPLACE TABLE test_catalog_<your_username>.default.customers_copy AS
SELECT * FROM uc_day25_<your_username>.bronze.customers;
-- Useful for backups or data sharing

-- Exercise 65: Compare table counts across catalogs
SELECT 
    'uc_day25.bronze' as location,
    COUNT(*) as row_count
FROM uc_day25_<your_username>.bronze.customers
UNION ALL
SELECT 
    'test_catalog.default',
    COUNT(*)
FROM test_catalog_<your_username>.default.customers_copy;
-- Verify data consistency across catalogs

-- ============================================================================
-- PART 9: SYSTEM TABLES (5 exercises)
-- ============================================================================

-- Exercise 66: Query audit logs for table access
SELECT 
    event_time,
    user_identity.email as user_email,
    action_name,
    request_params.full_name_arg as table_accessed
FROM system.access.audit
WHERE action_name IN ('getTable', 'readTable')
    AND event_date >= current_date() - 1
    AND request_params.full_name_arg LIKE '%uc_day25%'
ORDER BY event_time DESC
LIMIT 10;
-- Track who accessed your tables

-- Exercise 67: Query table lineage
SELECT 
    source_table_full_name,
    target_table_full_name,
    source_type,
    target_type
FROM system.access.table_lineage
WHERE target_table_full_name LIKE '%customer_summary%'
    OR source_table_full_name LIKE '%customer_summary%';
-- Understand data flow and dependencies

-- Exercise 68: Query billing usage
SELECT 
    usage_date,
    sku_name,
    usage_quantity,
    usage_unit
FROM system.billing.usage
WHERE usage_date >= current_date() - 7
ORDER BY usage_date DESC, usage_quantity DESC
LIMIT 10;
-- Monitor compute and storage costs

-- Exercise 69: Find who accessed your tables
SELECT 
    user_identity.email as user,
    action_name,
    request_params.full_name_arg as resource,
    COUNT(*) as access_count
FROM system.access.audit
WHERE request_params.full_name_arg LIKE '%uc_day25_%'
    AND event_date >= current_date() - 7
GROUP BY user_identity.email, action_name, request_params.full_name_arg
ORDER BY access_count DESC;
-- Audit data access patterns

-- Exercise 70: Query table metadata from information schema
SELECT 
    table_catalog,
    table_schema,
    table_name,
    table_type,
    created
FROM system.information_schema.tables
WHERE table_catalog = 'uc_day25_<your_username>'
ORDER BY table_schema, table_name;
-- Comprehensive table inventory

-- ============================================================================
-- PART 10: ADVANCED SCENARIOS (10 exercises)
-- ============================================================================

-- Exercise 71: Create a multi-layer data pipeline
-- Bronze: Raw data
CREATE OR REPLACE TABLE uc_day25_<your_username>.bronze.raw_transactions (
    transaction_id INT,
    customer_id INT,
    amount DECIMAL(10,2),
    transaction_date TIMESTAMP,
    raw_data STRING
);

INSERT INTO uc_day25_<your_username>.bronze.raw_transactions VALUES
(1, 1, 100.00, '2024-01-01 10:00:00', '{"status":"pending"}'),
(2, 2, 200.00, '2024-01-02 11:00:00', '{"status":"completed"}');

-- Silver: Cleaned data
CREATE OR REPLACE TABLE uc_day25_<your_username>.silver.clean_transactions AS
SELECT 
    transaction_id,
    customer_id,
    amount,
    CAST(transaction_date AS DATE) as transaction_date,
    get_json_object(raw_data, '$.status') as status
FROM uc_day25_<your_username>.bronze.raw_transactions
WHERE amount > 0;

-- Gold: Aggregated data
CREATE OR REPLACE TABLE uc_day25_<your_username>.gold.transaction_summary AS
SELECT 
    customer_id,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM uc_day25_<your_username>.silver.clean_transactions
GROUP BY customer_id;
-- Complete medallion architecture with lineage

-- Exercise 72: Implement row-level security with views
CREATE OR REPLACE VIEW uc_day25_<your_username>.gold.my_state_customers AS
SELECT 
    customer_id,
    full_name,
    email,
    state
FROM uc_day25_<your_username>.silver.customers_clean
WHERE state = 'CA';  -- In production, use current_user() or session variable
-- Users only see data for their region/state

-- Exercise 73: Create a schema for each environment
CREATE SCHEMA IF NOT EXISTS uc_day25_<your_username>.dev
COMMENT 'Development environment';

CREATE SCHEMA IF NOT EXISTS uc_day25_<your_username>.staging
COMMENT 'Staging environment for testing';

CREATE SCHEMA IF NOT EXISTS uc_day25_<your_username>.prod
COMMENT 'Production environment';
-- Separate environments within same catalog

-- Exercise 74: Clone a table across schemas
CREATE TABLE uc_day25_<your_username>.staging.customers_clone 
SHALLOW CLONE uc_day25_<your_username>.bronze.customers;
-- Fast, zero-copy clone for testing

-- Exercise 75: Set up a data sharing pattern
-- Create shared schema
CREATE SCHEMA IF NOT EXISTS uc_day25_<your_username>.shared
COMMENT 'Shared data accessible to multiple teams';

-- Create shared view
CREATE OR REPLACE VIEW uc_day25_<your_username>.shared.public_customer_stats AS
SELECT 
    state,
    COUNT(*) as customer_count,
    AVG(total_orders) as avg_orders
FROM uc_day25_<your_username>.gold.customer_summary
GROUP BY state;

-- Grant access
GRANT USE SCHEMA ON SCHEMA uc_day25_<your_username>.shared TO `all_users`;
GRANT SELECT ON VIEW uc_day25_<your_username>.shared.public_customer_stats TO `all_users`;
-- Controlled data sharing

-- Exercise 76: Implement a permission hierarchy
-- Catalog level
GRANT USE CATALOG ON CATALOG uc_day25_<your_username> TO `analysts`;

-- Schema level
GRANT USE SCHEMA ON SCHEMA uc_day25_<your_username>.gold TO `analysts`;

-- Table level
GRANT SELECT ON TABLE uc_day25_<your_username>.gold.customer_summary TO `analysts`;

-- Show inheritance
SHOW GRANTS ON CATALOG uc_day25_<your_username>;
SHOW GRANTS ON SCHEMA uc_day25_<your_username>.gold;
SHOW GRANTS ON TABLE uc_day25_<your_username>.gold.customer_summary;
-- Permissions flow down the hierarchy

-- Exercise 77: Create a governance report
CREATE OR REPLACE VIEW uc_day25_<your_username>.gold.governance_report AS
SELECT 
    t.table_catalog,
    t.table_schema,
    t.table_name,
    COALESCE(p_owner.property_value, 'unknown') as owner,
    COALESCE(p_class.property_value, 'unclassified') as classification,
    COALESCE(p_pii.property_value, 'false') as contains_pii,
    t.created as created_date
FROM system.information_schema.tables t
LEFT JOIN system.information_schema.table_properties p_owner
    ON t.table_catalog = p_owner.table_catalog
    AND t.table_schema = p_owner.table_schema
    AND t.table_name = p_owner.table_name
    AND p_owner.property_name = 'owner'
LEFT JOIN system.information_schema.table_properties p_class
    ON t.table_catalog = p_class.table_catalog
    AND t.table_schema = p_class.table_schema
    AND t.table_name = p_class.table_name
    AND p_class.property_name = 'data_classification'
LEFT JOIN system.information_schema.table_properties p_pii
    ON t.table_catalog = p_pii.table_catalog
    AND t.table_schema = p_pii.table_schema
    AND t.table_name = p_pii.table_name
    AND p_pii.property_name = 'contains_pii'
WHERE t.table_catalog = 'uc_day25_<your_username>';

SELECT * FROM uc_day25_<your_username>.gold.governance_report;
-- Comprehensive governance dashboard

-- Exercise 78: Implement a naming convention check
SELECT 
    table_catalog,
    table_schema,
    table_name,
    CASE 
        WHEN table_name NOT RLIKE '^[a-z_]+$' THEN 'Invalid: Contains uppercase or special chars'
        WHEN LENGTH(table_name) > 50 THEN 'Invalid: Name too long'
        ELSE 'Valid'
    END as naming_check
FROM system.information_schema.tables
WHERE table_catalog = 'uc_day25_<your_username>'
ORDER BY naming_check, table_name;
-- Enforce naming standards

-- Exercise 79: Create a data quality metadata table
CREATE OR REPLACE TABLE uc_day25_<your_username>.gold.table_metadata (
    table_full_name STRING,
    row_count BIGINT,
    last_updated TIMESTAMP,
    data_quality_score DECIMAL(3,2),
    notes STRING
);

INSERT INTO uc_day25_<your_username>.gold.table_metadata VALUES
('uc_day25_<your_username>.bronze.customers', 10, current_timestamp(), 0.95, 'Good quality'),
('uc_day25_<your_username>.bronze.orders', 15, current_timestamp(), 0.98, 'Excellent quality');

SELECT * FROM uc_day25_<your_username>.gold.table_metadata;
-- Track data quality metrics

-- Exercise 80: Build a permission audit query
CREATE OR REPLACE VIEW uc_day25_<your_username>.gold.permission_audit AS
SELECT 
    'catalog' as object_type,
    table_catalog as object_name,
    grantee,
    privilege_type
FROM system.information_schema.catalog_privileges
WHERE table_catalog = 'uc_day25_<your_username>'
UNION ALL
SELECT 
    'schema',
    CONCAT(table_catalog, '.', table_schema),
    grantee,
    privilege_type
FROM system.information_schema.schema_privileges
WHERE table_catalog = 'uc_day25_<your_username>'
UNION ALL
SELECT 
    'table',
    CONCAT(table_catalog, '.', table_schema, '.', table_name),
    grantee,
    privilege_type
FROM system.information_schema.table_privileges
WHERE table_catalog = 'uc_day25_<your_username>';

SELECT * FROM uc_day25_<your_username>.gold.permission_audit
ORDER BY object_type, object_name;
-- Complete permission inventory

-- ============================================================================
-- BONUS CHALLENGES (5 exercises)
-- ============================================================================

-- Bonus 1: Create a complete medallion architecture
-- Already demonstrated in Exercise 71, but here's a complete example:

-- Bronze layer
CREATE SCHEMA IF NOT EXISTS uc_day25_<your_username>.bronze_complete;
CREATE TABLE uc_day25_<your_username>.bronze_complete.raw_events (
    event_id STRING,
    event_type STRING,
    event_data STRING,
    ingested_at TIMESTAMP
);

-- Silver layer
CREATE SCHEMA IF NOT EXISTS uc_day25_<your_username>.silver_complete;
CREATE TABLE uc_day25_<your_username>.silver_complete.parsed_events AS
SELECT 
    event_id,
    event_type,
    get_json_object(event_data, '$.user_id') as user_id,
    get_json_object(event_data, '$.action') as action,
    ingested_at
FROM uc_day25_<your_username>.bronze_complete.raw_events;

-- Gold layer
CREATE SCHEMA IF NOT EXISTS uc_day25_<your_username>.gold_complete;
CREATE TABLE uc_day25_<your_username>.gold_complete.event_summary AS
SELECT 
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users
FROM uc_day25_<your_username>.silver_complete.parsed_events
GROUP BY event_type;

-- Bonus 2: Implement a data catalog
CREATE TABLE uc_day25_<your_username>.gold.data_catalog (
    table_full_name STRING,
    table_description STRING,
    owner STRING,
    data_classification STRING,
    contains_pii BOOLEAN,
    refresh_frequency STRING,
    upstream_dependencies ARRAY<STRING>,
    downstream_consumers ARRAY<STRING>,
    created_date DATE,
    last_modified_date DATE
);

INSERT INTO uc_day25_<your_username>.gold.data_catalog VALUES
(
    'uc_day25_<your_username>.gold.customer_summary',
    'Customer-level aggregates for analytics and reporting',
    'data_engineering_team',
    'internal',
    true,
    'daily',
    array('uc_day25_<your_username>.silver.customers_clean', 'uc_day25_<your_username>.silver.orders_valid'),
    array('uc_day25_<your_username>.gold.governance_report'),
    current_date(),
    current_date()
);

SELECT * FROM uc_day25_<your_username>.gold.data_catalog;

-- Bonus 3: Build a lineage tracking system
CREATE TABLE uc_day25_<your_username>.gold.manual_lineage (
    source_table STRING,
    target_table STRING,
    transformation_type STRING,
    transformation_logic STRING,
    created_by STRING,
    created_at TIMESTAMP
);

INSERT INTO uc_day25_<your_username>.gold.manual_lineage VALUES
(
    'uc_day25_<your_username>.bronze.customers',
    'uc_day25_<your_username>.silver.customers_clean',
    'cleaning',
    'Concatenate first_name and last_name, lowercase email',
    current_user(),
    current_timestamp()
),
(
    'uc_day25_<your_username>.silver.customers_clean',
    'uc_day25_<your_username>.gold.customer_summary',
    'aggregation',
    'Group by customer, calculate order metrics',
    current_user(),
    current_timestamp()
);

-- Query lineage
SELECT * FROM uc_day25_<your_username>.gold.manual_lineage
ORDER BY created_at;

-- Bonus 4: Create a permission management system
CREATE OR REPLACE VIEW uc_day25_<your_username>.gold.permission_summary AS
WITH all_permissions AS (
    SELECT 
        grantee,
        'CATALOG' as level,
        table_catalog as object_name,
        privilege_type
    FROM system.information_schema.catalog_privileges
    WHERE table_catalog = 'uc_day25_<your_username>'
    
    UNION ALL
    
    SELECT 
        grantee,
        'SCHEMA',
        CONCAT(table_catalog, '.', table_schema),
        privilege_type
    FROM system.information_schema.schema_privileges
    WHERE table_catalog = 'uc_day25_<your_username>'
    
    UNION ALL
    
    SELECT 
        grantee,
        'TABLE',
        CONCAT(table_catalog, '.', table_schema, '.', table_name),
        privilege_type
    FROM system.information_schema.table_privileges
    WHERE table_catalog = 'uc_day25_<your_username>'
)
SELECT 
    grantee,
    level,
    COUNT(DISTINCT object_name) as object_count,
    COLLECT_SET(privilege_type) as privileges
FROM all_permissions
GROUP BY grantee, level
ORDER BY grantee, level;

SELECT * FROM uc_day25_<your_username>.gold.permission_summary;

-- Bonus 5: Implement a data quality framework
CREATE TABLE uc_day25_<your_username>.gold.data_quality_checks (
    check_id STRING,
    table_name STRING,
    check_type STRING,
    check_query STRING,
    expected_result STRING,
    actual_result STRING,
    status STRING,
    checked_at TIMESTAMP
);

-- Example quality checks
INSERT INTO uc_day25_<your_username>.gold.data_quality_checks VALUES
(
    'check_001',
    'uc_day25_<your_username>.bronze.customers',
    'null_check',
    'SELECT COUNT(*) FROM uc_day25_<your_username>.bronze.customers WHERE customer_id IS NULL',
    '0',
    '0',
    'PASS',
    current_timestamp()
),
(
    'check_002',
    'uc_day25_<your_username>.bronze.orders',
    'range_check',
    'SELECT COUNT(*) FROM uc_day25_<your_username>.bronze.orders WHERE order_amount < 0',
    '0',
    '0',
    'PASS',
    current_timestamp()
);

-- Quality dashboard
SELECT 
    table_name,
    check_type,
    status,
    COUNT(*) as check_count
FROM uc_day25_<your_username>.gold.data_quality_checks
GROUP BY table_name, check_type, status
ORDER BY table_name, check_type;

-- ============================================================================
-- CLEANUP (Optional)
-- ============================================================================

-- To remove all objects created today:
-- DROP CATALOG IF EXISTS uc_day25_<your_username> CASCADE;
-- DROP CATALOG IF EXISTS test_catalog_<your_username> CASCADE;

-- ============================================================================
-- SUMMARY
-- ============================================================================

-- You've completed 85 exercises covering:
-- ✅ Catalog and schema management (10 exercises)
-- ✅ Three-level namespace (10 exercises)
-- ✅ Access control - GRANT (10 exercises)
-- ✅ Access control - REVOKE (5 exercises)
-- ✅ Table properties and comments (10 exercises)
-- ✅ Managed vs external tables (5 exercises)
-- ✅ Data discovery (10 exercises)
-- ✅ Cross-catalog queries (5 exercises)
-- ✅ System tables (5 exercises)
-- ✅ Advanced scenarios (10 exercises)
-- ✅ Bonus challenges (5 exercises)

-- Key takeaways:
-- 1. Unity Catalog uses three-level namespace: catalog.schema.table
-- 2. Permissions are hierarchical: metastore → catalog → schema → table
-- 3. Three permissions needed to query: USE CATALOG + USE SCHEMA + SELECT
-- 4. Managed tables: UC controls data (DROP deletes data)
-- 5. External tables: Data stays in place (DROP only removes metadata)
-- 6. System tables provide governance insights (audit, lineage, billing)
-- 7. Table properties enable metadata-driven governance
-- 8. Always use fully qualified names in production code

-- ============================================================================
-- END OF SOLUTIONS
-- ============================================================================
