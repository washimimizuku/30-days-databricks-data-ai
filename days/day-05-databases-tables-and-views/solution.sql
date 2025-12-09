-- Day 5: Databases, Tables, and Views - Solutions
-- =========================================================

-- =========================================================
-- PART 1: DATABASE OPERATIONS
-- =========================================================

-- Exercise 1: Create Database
CREATE DATABASE IF NOT EXISTS company_db;

-- Exercise 2: Create Database with Location
CREATE DATABASE IF NOT EXISTS external_db
LOCATION '/FileStore/external_db/';

-- Exercise 3: List Databases
SHOW DATABASES;

-- Exercise 4: Use Database
USE company_db;

-- Exercise 5: Describe Database
DESCRIBE DATABASE EXTENDED company_db;

-- =========================================================
-- PART 2: MANAGED TABLES
-- =========================================================

-- Exercise 6: Create Basic Managed Table
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT,
    name STRING,
    email STRING,
    city STRING
) USING DELTA;

-- Exercise 7: Insert Data
INSERT INTO customers VALUES
    (1, 'John Smith', 'john.smith@email.com', 'New York'),
    (2, 'Jane Doe', 'jane.doe@email.com', 'Los Angeles'),
    (3, 'Bob Johnson', 'bob.j@email.com', 'Chicago'),
    (4, 'Alice Williams', 'alice.w@email.com', 'New York'),
    (5, 'Charlie Brown', 'charlie.b@email.com', 'Houston');

-- Exercise 8: Create Table from Query (CTAS)
CREATE TABLE customers_backup
AS SELECT * FROM customers;

-- Exercise 9: Check Table Location
DESCRIBE DETAIL customers_backup;

-- Exercise 10: Drop Managed Table
DROP TABLE IF EXISTS customers_backup;
-- Verify with: %fs ls /user/hive/warehouse/company_db.db/
-- The customers_backup directory should be deleted

-- =========================================================
-- PART 3: EXTERNAL TABLES
-- =========================================================

-- Exercise 11: Create External Table
CREATE TABLE IF NOT EXISTS customers_external (
    customer_id INT,
    name STRING,
    email STRING,
    city STRING
) USING DELTA
LOCATION '/FileStore/day5/customers';

-- Exercise 12: Insert Data into External Table
INSERT INTO customers_external
SELECT * FROM customers;

-- Exercise 13: Verify External Location
DESCRIBE DETAIL customers_external;
-- Also check: %fs ls /FileStore/day5/customers

-- Exercise 14: Drop External Table
DROP TABLE IF EXISTS customers_external;
-- Verify data remains: %fs ls /FileStore/day5/customers

-- Exercise 15: Recreate External Table from Existing Data
CREATE TABLE customers_external
USING DELTA
LOCATION '/FileStore/day5/customers';

-- Verify data is still there
SELECT * FROM customers_external;

-- =========================================================
-- PART 4: VIEWS
-- =========================================================

-- Exercise 16: Create Standard View
CREATE OR REPLACE VIEW local_customers AS
SELECT * FROM customers
WHERE city = 'New York';

-- Exercise 17: Create Temporary View
CREATE OR REPLACE TEMP VIEW premium_customers AS
SELECT * FROM customers
WHERE customer_id IN (
    SELECT customer_id FROM customers
    WHERE customer_id > 3  -- Simulating premium customers
);

-- Exercise 18: Create Global Temporary View
CREATE OR REPLACE GLOBAL TEMP VIEW customer_summary AS
SELECT 
    city,
    COUNT(*) as customer_count
FROM customers
GROUP BY city;

-- Exercise 19: Query Views
-- Standard view
SELECT * FROM local_customers;

-- Temporary view
SELECT * FROM premium_customers;

-- Global temporary view (note the global_temp prefix)
SELECT * FROM global_temp.customer_summary;

-- Exercise 20: Drop View
DROP VIEW IF EXISTS local_customers;

-- =========================================================
-- PART 5: TABLE TYPES AND FORMATS
-- =========================================================

-- Exercise 21: Create Delta Table
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id INT,
    customer_id INT,
    amount DECIMAL(10,2),
    transaction_date DATE
) USING DELTA;

INSERT INTO transactions VALUES
    (1, 1, 150.00, '2024-01-15'),
    (2, 2, 200.50, '2024-01-16'),
    (3, 1, 75.25, '2024-01-17'),
    (4, 3, 300.00, '2024-01-18'),
    (5, 4, 125.75, '2024-01-19');

-- Exercise 22: Create Parquet Table
CREATE TABLE transactions_parquet
USING PARQUET
AS SELECT * FROM transactions;

-- Exercise 23: Create CSV Table
CREATE TABLE transactions_csv
USING CSV
AS SELECT * FROM transactions;

-- Exercise 24: Compare Formats
-- Check file sizes
DESCRIBE DETAIL transactions;
DESCRIBE DETAIL transactions_parquet;
DESCRIBE DETAIL transactions_csv;

-- Query performance comparison
SELECT COUNT(*) FROM transactions;
SELECT COUNT(*) FROM transactions_parquet;
SELECT COUNT(*) FROM transactions_csv;

-- Exercise 25: Convert to Delta
-- For Parquet table
CREATE TABLE transactions_parquet_delta
USING DELTA
AS SELECT * FROM transactions_parquet;

-- Or use CONVERT TO DELTA (if table is already Parquet)
-- CONVERT TO DELTA parquet.`/path/to/parquet/table`;

-- =========================================================
-- PART 6: PARTITIONING
-- =========================================================

-- Exercise 26: Create Partitioned Table
CREATE TABLE IF NOT EXISTS sales (
    sale_id INT,
    product_id INT,
    amount DECIMAL(10,2),
    sale_date DATE
) USING DELTA
PARTITIONED BY (sale_date);

-- Exercise 27: Insert Partitioned Data
INSERT INTO sales VALUES
    (1, 101, 50.00, '2024-01-01'),
    (2, 102, 75.50, '2024-01-01'),
    (3, 103, 100.00, '2024-01-02'),
    (4, 101, 60.00, '2024-01-02'),
    (5, 104, 200.00, '2024-01-03'),
    (6, 102, 85.25, '2024-01-03'),
    (7, 105, 150.00, '2024-01-04'),
    (8, 103, 95.75, '2024-01-04');

-- Exercise 28: Show Partitions
SHOW PARTITIONS sales;

-- Exercise 29: Query Specific Partition
SELECT * FROM sales
WHERE sale_date = '2024-01-02';

-- Exercise 30: Add Partition
INSERT INTO sales VALUES
    (9, 106, 175.00, '2024-01-05'),
    (10, 107, 225.50, '2024-01-05');

-- Verify new partition
SHOW PARTITIONS sales;

-- =========================================================
-- PART 7: TABLE METADATA
-- =========================================================

-- Exercise 31: Describe Table
DESCRIBE TABLE customers;

-- Exercise 32: Describe Extended
DESCRIBE EXTENDED customers;

-- Exercise 33: Show Create Table
SHOW CREATE TABLE customers;

-- Exercise 34: Show Columns
SHOW COLUMNS IN customers;

-- Exercise 35: Show Table Properties
SHOW TBLPROPERTIES customers;

-- =========================================================
-- PART 8: ALTER TABLE OPERATIONS
-- =========================================================

-- Exercise 36: Add Column
ALTER TABLE customers ADD COLUMN phone STRING;

-- Exercise 37: Rename Column
ALTER TABLE customers RENAME COLUMN phone TO phone_number;

-- Exercise 38: Add Column Comment
ALTER TABLE customers ALTER COLUMN email COMMENT 'Customer email address';

-- Exercise 39: Set Table Properties
ALTER TABLE customers SET TBLPROPERTIES (
    'description' = 'Customer master data table',
    'owner' = 'data_engineering_team'
);

-- Exercise 40: Rename Table
ALTER TABLE customers RENAME TO clients;

-- Verify rename
SHOW TABLES;
SELECT * FROM clients;

-- =========================================================
-- REFLECTION ANSWERS
-- =========================================================
%md
### Reflection Answers

1. **Difference between managed and external tables**:
   - Managed: Databricks manages both metadata and data files
   - External: Databricks manages only metadata; data files are in user-specified location

2. **DROP managed table**:
   - Deletes both metadata AND data files
   - Data is permanently removed from storage

3. **DROP external table**:
   - Deletes only metadata
   - Data files remain in the specified location

4. **When to use managed vs external**:
   - Managed: Data used only within Databricks, temporary data, development
   - External: Production data, data shared with other systems, data you want to keep after DROP

5. **Three types of views**:
   - Standard View: Persistent, database-scoped
   - Temporary View: Session-scoped
   - Global Temporary View: Cluster-scoped

6. **Scope of temporary view**:
   - Available only in the current session
   - Automatically dropped when session ends

7. **Access global temporary view**:
   - Use the global_temp database prefix
   - Example: SELECT * FROM global_temp.view_name

8. **Benefit of partitioning**:
   - Faster queries through partition pruning
   - Better data organization
   - Skip irrelevant partitions during queries

9. **Default managed table location**:
   - /user/hive/warehouse/<database>.db/<table>/

10. **Why Delta Lake is recommended**:
    - ACID transactions
    - Time travel
    - Schema evolution
    - Better performance
    - Data reliability

-- =========================================================
-- BONUS SOLUTIONS
-- =========================================================

-- Bonus 1: Complete E-commerce Schema
-- Create database
CREATE DATABASE IF NOT EXISTS ecommerce_db;
USE ecommerce_db;

-- Customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT,
    name STRING,
    email STRING,
    city STRING,
    registration_date DATE
) USING DELTA;

-- Products table
CREATE TABLE IF NOT EXISTS products (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10,2)
) USING DELTA;

-- Orders table (partitioned)
CREATE TABLE IF NOT EXISTS orders (
    order_id INT,
    customer_id INT,
    order_total DECIMAL(10,2),
    order_status STRING,
    order_date DATE
) USING DELTA
PARTITIONED BY (order_date);

-- Order items table
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    item_price DECIMAL(10,2)
) USING DELTA;

-- Views
CREATE OR REPLACE VIEW active_orders AS
SELECT * FROM orders
WHERE order_status = 'Active';

CREATE OR REPLACE VIEW customer_order_summary AS
SELECT 
    c.customer_id,
    c.name,
    COUNT(o.order_id) as total_orders,
    SUM(o.order_total) as total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name;

-- Bonus 2: Slowly Changing Dimension
CREATE TABLE IF NOT EXISTS customer_dimension (
    customer_key INT,
    customer_id INT,
    name STRING,
    email STRING,
    city STRING,
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
) USING DELTA;

-- Insert initial records
INSERT INTO customer_dimension VALUES
    (1, 1, 'John Smith', 'john@email.com', 'New York', '2024-01-01', '9999-12-31', true),
    (2, 2, 'Jane Doe', 'jane@email.com', 'LA', '2024-01-01', '9999-12-31', true);

-- Bonus 3: Table Comparison Report
CREATE OR REPLACE VIEW table_comparison AS
SELECT 
    table_name,
    CASE 
        WHEN location LIKE '%/user/hive/warehouse/%' THEN 'Managed'
        ELSE 'External'
    END as table_type,
    location,
    sizeInBytes,
    numFiles
FROM (
    SELECT 
        tableName as table_name,
        location,
        sizeInBytes,
        numFiles
    FROM (
        DESCRIBE DETAIL clients
        UNION ALL
        DESCRIBE DETAIL customers_external
    )
);

-- Bonus 4: Partition Management
-- Create a view to identify old partitions
CREATE OR REPLACE VIEW old_partitions AS
SELECT 
    'sales' as table_name,
    partition,
    CAST(partition AS DATE) as partition_date
FROM (
    SHOW PARTITIONS sales
)
WHERE CAST(partition AS DATE) < date_sub(current_date(), 90);

-- To delete old partitions (be careful!):
-- ALTER TABLE sales DROP PARTITION (sale_date='2024-01-01');

-- Bonus 5: Metadata Catalog
CREATE OR REPLACE VIEW metadata_catalog AS
SELECT 
    database,
    tableName as table_name,
    tableType as table_type,
    provider as format,
    location,
    CASE 
        WHEN location LIKE '%/user/hive/warehouse/%' THEN 'Managed'
        ELSE 'External'
    END as management_type,
    partitionColumns,
    numFiles,
    sizeInBytes / 1024 / 1024 as size_mb,
    createdAt as created_at
FROM (
    SELECT * FROM (
        DESCRIBE DETAIL clients
        UNION ALL
        DESCRIBE DETAIL customers_external
        UNION ALL
        DESCRIBE DETAIL transactions
        UNION ALL
        DESCRIBE DETAIL sales
    )
);

SELECT * FROM metadata_catalog;

-- =========================================================
-- CLEANUP
-- =========================================================

-- Uncomment to clean up
-- DROP DATABASE IF EXISTS company_db CASCADE;
-- DROP DATABASE IF EXISTS external_db CASCADE;
-- DROP DATABASE IF EXISTS ecommerce_db CASCADE;

-- =========================================================
-- KEY LEARNINGS SUMMARY
-- =========================================================
%md
## Key Learnings from Day 5

### Database Operations
- Created databases with and without custom locations
- Used SHOW, DESCRIBE, and USE commands
- Understood database hierarchy

### Managed Tables
- Created managed tables with various methods
- Understood default storage location
- Verified DROP behavior (deletes data + metadata)

### External Tables
- Created external tables with custom locations
- Verified DROP behavior (keeps data, deletes metadata)
- Recreated tables from existing data

### Views
- Created standard, temporary, and global temporary views
- Understood scope and lifetime of each view type
- Accessed global temp views with global_temp prefix

### Table Formats
- Worked with Delta, Parquet, and CSV formats
- Compared performance and file sizes
- Converted between formats

### Partitioning
- Created partitioned tables
- Inserted data into partitions
- Queried specific partitions
- Managed partitions

### Metadata Operations
- Used DESCRIBE, SHOW commands
- Inspected table properties
- Viewed table structure and details

### ALTER Operations
- Added and renamed columns
- Added comments
- Set table properties
- Renamed tables

**Next**: Day 6 will cover Spark SQL basics with more advanced querying techniques!
