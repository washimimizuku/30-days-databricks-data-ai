-- Day 3: Delta Lake Fundamentals - Solutions
-- =========================================================

-- =========================================================
-- PART 1: CREATING DELTA TABLES
-- =========================================================

-- Exercise 1: Create a Basic Delta Table
CREATE TABLE products (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10,2),
    in_stock BOOLEAN
) USING DELTA;

-- Exercise 2: Create Table with Data (CTAS)
CREATE TABLE products_backup
USING DELTA
AS SELECT * FROM products;

-- Exercise 3: Create External Delta Table
CREATE TABLE products_external (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10,2),
    in_stock BOOLEAN
) USING DELTA
LOCATION '/tmp/delta/products_external';

-- Exercise 4: Insert Data into Delta Table
INSERT INTO products VALUES
    (1, 'Laptop', 'Electronics', 999.99, true),
    (2, 'Mouse', 'Electronics', 29.99, true),
    (3, 'Desk Chair', 'Furniture', 199.99, true),
    (4, 'Monitor', 'Electronics', 299.99, false),
    (5, 'Keyboard', 'Electronics', 79.99, true),
    (6, 'Notebook', 'Stationery', 4.99, true),
    (7, 'Pen Set', 'Stationery', 12.99, true),
    (8, 'Desk Lamp', 'Furniture', 45.99, true);

-- Exercise 5: Verify Table Format
-- Method 1: DESCRIBE DETAIL
DESCRIBE DETAIL products;
-- Look for 'format' column showing 'delta'

-- Method 2: SHOW CREATE TABLE
SHOW CREATE TABLE products;
-- Look for 'USING DELTA' in output


-- =========================================================
-- PART 2: TIME TRAVEL
-- =========================================================

-- Exercise 6: Create Multiple Versions

-- Version 1: INSERT
INSERT INTO products VALUES
    (9, 'Headphones', 'Electronics', 149.99, true),
    (10, 'Webcam', 'Electronics', 89.99, true),
    (11, 'USB Cable', 'Electronics', 9.99, true);

-- Version 2: UPDATE
UPDATE products 
SET price = price * 1.10 
WHERE category = 'Electronics';

-- Version 3: DELETE
DELETE FROM products WHERE product_id = 4;

-- Exercise 7: View Table History
DESCRIBE HISTORY products;

-- Expected output shows:
-- - version, timestamp, operation, operationParameters
-- - Version 0: CREATE TABLE
-- - Version 1: WRITE (initial INSERT)
-- - Version 2: WRITE (second INSERT)
-- - Version 3: UPDATE
-- - Version 4: DELETE

-- Exercise 8: Query by Version Number
-- Query version 0 (initial state - empty table)
SELECT * FROM products VERSION AS OF 0;

-- Query version 1 (after first INSERT)
SELECT * FROM products VERSION AS OF 1;

-- Exercise 9: Query by Timestamp
-- Query 5 minutes ago
SELECT * FROM products 
TIMESTAMP AS OF (current_timestamp() - INTERVAL 5 MINUTES);

-- Query specific timestamp
SELECT * FROM products 
TIMESTAMP AS OF '2024-12-08 10:00:00';

-- Exercise 10: Compare Versions
-- Method 1: UNION to show differences
SELECT 'Version 0' as version, * FROM products VERSION AS OF 0
UNION ALL
SELECT 'Current' as version, * FROM products;

-- Method 2: Count comparison
SELECT 
    'Version 0' as version,
    COUNT(*) as row_count,
    SUM(price) as total_value
FROM products VERSION AS OF 0
UNION ALL
SELECT 
    'Current' as version,
    COUNT(*) as row_count,
    SUM(price) as total_value
FROM products;

-- Method 3: Find added products
SELECT * FROM products
WHERE product_id NOT IN (
    SELECT product_id FROM products VERSION AS OF 0
);


-- =========================================================
-- PART 3: SCHEMA EVOLUTION
-- =========================================================

-- Exercise 11: Add a Column
ALTER TABLE products ADD COLUMN supplier STRING;

-- Verify new column
DESCRIBE products;

-- Exercise 12: Insert Data with New Schema
INSERT INTO products VALUES
    (12, 'Tablet', 'Electronics', 399.99, true, 'TechCorp');

-- Query to see new column
SELECT * FROM products WHERE supplier IS NOT NULL;

-- Exercise 13: Schema Merge on Write
%python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, BooleanType, DoubleType

# Create DataFrame with additional 'discount' column
new_data = spark.createDataFrame([
    (13, 'Smart Watch', 'Electronics', 249.99, True, 'TechCorp', 0.10),
    (14, 'Fitness Tracker', 'Electronics', 99.99, True, 'HealthTech', 0.15)
], ['product_id', 'product_name', 'category', 'price', 'in_stock', 'supplier', 'discount'])

# Append with schema merge
new_data.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("products")

-- Exercise 14: View Schema History
-- Query old version (before discount column)
SELECT * FROM products VERSION AS OF 1;

-- Query current version (with discount column)
SELECT * FROM products;

-- Compare schemas
DESCRIBE products VERSION AS OF 1;
DESCRIBE products;


-- =========================================================
-- PART 4: ACID TRANSACTIONS
-- =========================================================

-- Exercise 15: Atomic Transaction
-- Both operations succeed or both fail
BEGIN TRANSACTION;
    INSERT INTO products VALUES (15, 'New Product', 'Electronics', 199.99, true, 'Supplier X', 0.0);
    UPDATE products SET price = price * 1.05 WHERE product_id = 1;
COMMIT;

-- Note: Databricks auto-commits, but the concept is that operations are atomic

-- Exercise 16: Concurrent Reads
-- Cell 1: Long-running update
UPDATE products SET price = price * 1.01;

-- Cell 2: Query while update is running (run immediately after Cell 1)
SELECT COUNT(*), AVG(price) FROM products;
-- This will succeed! Reads are not blocked by writes

-- Exercise 17: Transaction Isolation
-- Uncommitted changes are not visible to other queries
-- Delta Lake ensures snapshot isolation

-- Start transaction (conceptually)
UPDATE products SET price = 999.99 WHERE product_id = 1;

-- Other queries see the committed state, not intermediate states
-- This is handled automatically by Delta Lake's transaction log


-- =========================================================
-- PART 5: TABLE OPERATIONS
-- =========================================================

-- Exercise 18: UPDATE Operation
UPDATE products 
SET price = price * 1.15 
WHERE category = 'Electronics';

-- Verify update
SELECT product_id, product_name, category, price 
FROM products 
WHERE category = 'Electronics';

-- Exercise 19: DELETE Operation
DELETE FROM products WHERE in_stock = false;

-- Verify deletion
SELECT COUNT(*) as remaining_products FROM products;
SELECT COUNT(*) as deleted_products FROM products VERSION AS OF 1 
WHERE in_stock = false;

-- Exercise 20: MERGE Operation (UPSERT)
-- Create source table (already provided in exercise)
CREATE OR REPLACE TEMP VIEW new_products AS
SELECT * FROM VALUES
    (1, 'Updated Product', 'Electronics', 299.99, true, 'Supplier A', 0.0),
    (99, 'New Product', 'Books', 19.99, true, 'Supplier B', 0.0)
AS t(product_id, product_name, category, price, in_stock, supplier, discount);

-- MERGE statement
MERGE INTO products t
USING new_products s
ON t.product_id = s.product_id
WHEN MATCHED THEN
    UPDATE SET 
        t.product_name = s.product_name,
        t.price = s.price,
        t.supplier = s.supplier
WHEN NOT MATCHED THEN
    INSERT (product_id, product_name, category, price, in_stock, supplier, discount)
    VALUES (s.product_id, s.product_name, s.category, s.price, s.in_stock, s.supplier, s.discount);

-- Verify MERGE results
SELECT * FROM products WHERE product_id IN (1, 99);


-- =========================================================
-- PART 6: OPTIMIZATION
-- =========================================================

-- Exercise 21: OPTIMIZE Table
OPTIMIZE products;

-- View optimization results
DESCRIBE HISTORY products LIMIT 1;

-- Exercise 22: Z-ORDER Optimization
OPTIMIZE products ZORDER BY (category, price);

-- Z-ordering co-locates related data for faster queries on these columns

-- Exercise 23: Check File Statistics
%python
# Before OPTIMIZE - check file count
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "products")
detail = delta_table.detail().collect()[0]

print(f"Number of files: {detail['numFiles']}")
print(f"Size in bytes: {detail['sizeInBytes']}")

# List data files
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/day3_delta.db/products/"))

# After OPTIMIZE, re-run to see fewer, larger files

-- Exercise 24: VACUUM Old Files
-- Remove files older than 7 days
VACUUM products RETAIN 168 HOURS;

-- Note: VACUUM permanently deletes files
-- Cannot time travel beyond VACUUM retention period
-- Default retention is 7 days (168 hours)

-- Check what would be deleted (dry run)
VACUUM products RETAIN 168 HOURS DRY RUN;


-- =========================================================
-- PART 7: TRANSACTION LOG
-- =========================================================

-- Exercise 25: Explore Transaction Log
%python
# List transaction log files
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/day3_delta.db/products/_delta_log/"))

-- Exercise 26: Read Transaction Log
%python
# Read first transaction log file
log_content = dbutils.fs.head("dbfs:/user/hive/warehouse/day3_delta.db/products/_delta_log/00000000000000000000.json")
print(log_content)

# Or read as JSON
import json
log_path = "dbfs:/user/hive/warehouse/day3_delta.db/products/_delta_log/00000000000000000000.json"
with open(log_path.replace("dbfs:", "/dbfs"), 'r') as f:
    for line in f:
        log_entry = json.loads(line)
        print(json.dumps(log_entry, indent=2))

-- Exercise 27: Understand Log Entries
%python
# Transaction log contains:
# - protocol: Delta Lake protocol version
# - metaData: Table schema, partitioning, configuration
# - add: Files added to table
# - remove: Files removed from table
# - commitInfo: Information about the commit (operation, timestamp, user)

# Example structure:
# {"commitInfo":{"timestamp":1234567890,"operation":"WRITE","operationParameters":{...}}}
# {"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
# {"metaData":{"id":"...","format":{"provider":"parquet"},"schemaString":"..."}}
# {"add":{"path":"part-00000-....parquet","size":1234,"modificationTime":1234567890}}


-- =========================================================
-- PART 8: MANAGED VS EXTERNAL TABLES
-- =========================================================

-- Exercise 28: Create Managed Table
CREATE TABLE managed_test (id INT, name STRING) USING DELTA;

-- Check location
DESCRIBE DETAIL managed_test;
-- Location: dbfs:/user/hive/warehouse/day3_delta.db/managed_test

-- Exercise 29: Create External Table
CREATE TABLE external_test (id INT, name STRING)
USING DELTA
LOCATION '/tmp/delta/external_test';

-- Check location
DESCRIBE DETAIL external_test;
-- Location: /tmp/delta/external_test

-- Exercise 30: DROP Table Comparison
-- Insert data first
INSERT INTO managed_test VALUES (1, 'Test');
INSERT INTO external_test VALUES (1, 'Test');

-- Drop managed table
DROP TABLE managed_test;

-- Check if data files still exist
%python
try:
    display(dbutils.fs.ls("dbfs:/user/hive/warehouse/day3_delta.db/managed_test/"))
    print("Files still exist!")
except Exception as e:
    print("Files deleted! (Expected for managed table)")

-- Drop external table
DROP TABLE external_test;

-- Check if data files still exist
%python
try:
    display(dbutils.fs.ls("/tmp/delta/external_test/"))
    print("Files still exist! (Expected for external table)")
except Exception as e:
    print("Files deleted!")


-- =========================================================
-- PART 9: RESTORE TABLE
-- =========================================================

-- Exercise 31: Restore to Previous Version
-- First, check current state
SELECT COUNT(*) as current_count FROM products;

-- Restore to version 1
RESTORE TABLE products TO VERSION AS OF 1;

-- Verify restore
SELECT COUNT(*) as restored_count FROM products;
DESCRIBE HISTORY products LIMIT 5;

-- Exercise 32: Restore by Timestamp
-- Restore to 10 minutes ago
RESTORE TABLE products TO TIMESTAMP AS OF (current_timestamp() - INTERVAL 10 MINUTES);

-- Or specific timestamp
RESTORE TABLE products TO TIMESTAMP AS OF '2024-12-08 10:00:00';

-- Exercise 33: Verify Restore
DESCRIBE HISTORY products;
-- Look for RESTORE operation in history


-- =========================================================
-- PART 10: ADVANCED SCENARIOS
-- =========================================================

-- Exercise 34: Partitioned Delta Table
CREATE TABLE products_partitioned (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10,2)
)
USING DELTA
PARTITIONED BY (category);

-- Insert data
INSERT INTO products_partitioned
SELECT product_id, product_name, category, price
FROM products;

-- Verify partitioning
DESCRIBE DETAIL products_partitioned;
-- Check partitionColumns

-- View partition structure
%python
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/day3_delta.db/products_partitioned/"))
-- You'll see folders like: category=Electronics/, category=Furniture/, etc.

-- Exercise 35: Clone Table (Shallow Copy)
CREATE TABLE products_shallow_clone
SHALLOW CLONE products;

-- Shallow clone shares data files, only copies metadata
-- Fast and space-efficient

-- Exercise 36: Clone Table (Deep Copy)
CREATE TABLE products_deep_clone
DEEP CLONE products;

-- Deep clone copies all data files
-- Independent copy, slower but fully isolated

-- Compare
DESCRIBE DETAIL products_shallow_clone;
DESCRIBE DETAIL products_deep_clone;

-- Exercise 37: Change Data Feed
-- Enable Change Data Feed
ALTER TABLE products SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Verify
DESCRIBE DETAIL products;
-- Look for delta.enableChangeDataFeed = true in properties

-- Exercise 38: Read Change Data
-- Make changes
UPDATE products SET price = price * 1.1 WHERE category = 'Electronics';
INSERT INTO products VALUES (100, 'New Item', 'Books', 29.99, true, 'Publisher', 0.0);
DELETE FROM products WHERE product_id = 5;

-- Read changes between versions
SELECT * FROM table_changes('products', 2, 5);

-- Read changes by timestamp
SELECT * FROM table_changes('products', '2024-12-08 10:00:00', '2024-12-08 11:00:00');

-- Change data includes:
-- - _change_type: insert, update_preimage, update_postimage, delete
-- - _commit_version: Version number
-- - _commit_timestamp: Timestamp of change

-- Exercise 39: Table Properties
-- Set custom properties
ALTER TABLE products SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'description' = 'Product catalog table'
);

-- View properties
SHOW TBLPROPERTIES products;

-- Exercise 40: Performance Comparison
-- Create Parquet table
CREATE TABLE products_parquet
USING PARQUET
AS SELECT * FROM products;

-- Create Delta table (already exists)
-- Compare query performance

-- Query 1: Full scan
SELECT COUNT(*), AVG(price) FROM products_parquet;
SELECT COUNT(*), AVG(price) FROM products;

-- Query 2: Filtered query
SELECT * FROM products_parquet WHERE category = 'Electronics';
SELECT * FROM products WHERE category = 'Electronics';

-- Delta Lake advantages:
-- - Data skipping (min/max statistics)
-- - Z-ordering for co-location
-- - Caching of metadata
-- - ACID guarantees

-- Check query plans
EXPLAIN SELECT * FROM products_parquet WHERE category = 'Electronics';
EXPLAIN SELECT * FROM products WHERE category = 'Electronics';


-- =========================================================
-- REFLECTION QUESTIONS - ANSWERS
-- =========================================================

%md
## Reflection Answers

1. **Key differences between Delta Lake and Parquet**:
   - Delta Lake = Parquet + Transaction Log + ACID
   - Delta supports time travel, Parquet doesn't
   - Delta supports ACID transactions, Parquet doesn't
   - Delta supports schema evolution, Parquet requires manual handling
   - Delta supports concurrent writes, Parquet doesn't
   - Delta provides audit trail, Parquet doesn't

2. **How time travel works**:
   - Transaction log records all changes with version numbers
   - Each operation creates a new version
   - Can query by version number (VERSION AS OF) or timestamp (TIMESTAMP AS OF)
   - Data files are immutable, new files created for changes
   - Old files retained until VACUUM

3. **Purpose of transaction log**:
   - Records all changes to the table
   - Enables ACID properties
   - Enables time travel
   - Provides audit trail
   - Enables concurrent reads/writes
   - Single source of truth for table state

4. **When to run OPTIMIZE**:
   - After many small writes (file compaction)
   - Before read-heavy workloads
   - Regularly for frequently updated tables
   - When query performance degrades
   - After bulk inserts/updates

5. **Managed vs external tables**:
   - Managed: Databricks manages both metadata and data
   - External: Databricks manages only metadata
   - DROP managed table: deletes both metadata and data
   - DROP external table: deletes only metadata, data remains
   - Managed: data in default location
   - External: data in specified location

6. **How Delta Lake ensures ACID**:
   - Atomicity: Transaction log ensures all-or-nothing commits
   - Consistency: Schema enforcement and validation
   - Isolation: Snapshot isolation via transaction log
   - Durability: Changes persisted to storage before commit

7. **What happens with VACUUM**:
   - Removes data files no longer referenced by transaction log
   - Frees up storage space
   - Cannot time travel beyond VACUUM retention period
   - Default retention: 7 days (168 hours)
   - Irreversible operation

8. **Schema evolution**:
   - Add columns with ALTER TABLE
   - Automatic merge with mergeSchema option
   - Schema enforcement prevents incompatible writes
   - Old data reads with null for new columns
   - Transaction log tracks schema changes

9. **Z-ordering**:
   - Co-locates related data in same files
   - Improves query performance for filtered columns
   - Use on frequently filtered columns
   - Max 4 columns recommended
   - Run with OPTIMIZE command

10. **Why Delta Lake is important**:
    - Foundation of lakehouse architecture
    - Combines data lake flexibility with data warehouse reliability
    - Enables ACID transactions on data lakes
    - Supports both batch and streaming
    - Provides data versioning and audit trail
    - Essential for production data pipelines


-- =========================================================
-- BONUS CHALLENGES - SOLUTIONS
-- =========================================================

-- Bonus 1: Build a Mini Lakehouse
-- Bronze layer: Raw data
CREATE TABLE products_bronze
USING DELTA
AS SELECT *, current_timestamp() as ingestion_time
FROM products;

-- Silver layer: Cleaned data
CREATE TABLE products_silver
USING DELTA
AS SELECT 
    product_id,
    TRIM(product_name) as product_name,
    UPPER(category) as category,
    CAST(price AS DECIMAL(10,2)) as price,
    in_stock,
    supplier,
    COALESCE(discount, 0.0) as discount
FROM products_bronze
WHERE product_id IS NOT NULL
  AND price > 0;

-- Gold layer: Aggregated data
CREATE TABLE products_gold
USING DELTA
AS SELECT 
    category,
    COUNT(*) as product_count,
    AVG(price) as avg_price,
    SUM(CASE WHEN in_stock THEN 1 ELSE 0 END) as in_stock_count,
    SUM(price) as total_value
FROM products_silver
GROUP BY category;

-- Bonus 2: Implement SCD Type 2
CREATE TABLE products_scd2 (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10,2),
    effective_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN
) USING DELTA;

-- Initial load
INSERT INTO products_scd2
SELECT 
    product_id,
    product_name,
    category,
    price,
    current_timestamp() as effective_date,
    CAST(NULL AS TIMESTAMP) as end_date,
    true as is_current
FROM products;

-- Handle updates (close old record, insert new)
MERGE INTO products_scd2 t
USING (
    SELECT 
        product_id,
        product_name,
        category,
        price,
        current_timestamp() as effective_date
    FROM products
    WHERE product_id = 1  -- Example: product 1 changed
) s
ON t.product_id = s.product_id AND t.is_current = true
WHEN MATCHED AND (t.price != s.price OR t.product_name != s.product_name) THEN
    UPDATE SET 
        t.end_date = current_timestamp(),
        t.is_current = false;

-- Insert new version
INSERT INTO products_scd2
SELECT 
    product_id,
    product_name,
    category,
    price,
    current_timestamp() as effective_date,
    CAST(NULL AS TIMESTAMP) as end_date,
    true as is_current
FROM products
WHERE product_id = 1;

-- Bonus 3: Optimize for Query Patterns
-- Given common queries on category, price, and in_stock
-- Optimize with Z-ordering
OPTIMIZE products ZORDER BY (category, in_stock, price);

-- Consider partitioning by category if very large
CREATE TABLE products_optimized (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10,2),
    in_stock BOOLEAN
)
USING DELTA
PARTITIONED BY (category);

-- Bonus 4: Audit Trail
-- Show all changes to product_id = 1
SELECT 
    version,
    timestamp,
    operation,
    operationParameters
FROM (DESCRIBE HISTORY products)
WHERE operationParameters LIKE '%product_id%1%'
ORDER BY version;

-- Compare versions
SELECT 'Version 0' as version, * FROM products VERSION AS OF 0 WHERE product_id = 1
UNION ALL
SELECT 'Version 1' as version, * FROM products VERSION AS OF 1 WHERE product_id = 1
UNION ALL
SELECT 'Current' as version, * FROM products WHERE product_id = 1;

-- Bonus 5: Data Quality Checks
-- Check for null values
SELECT 
    COUNT(*) as total_rows,
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) as null_product_id,
    SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) as null_product_name,
    SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) as null_price
FROM products;

-- Check for duplicates
SELECT product_id, COUNT(*) as count
FROM products
GROUP BY product_id
HAVING COUNT(*) > 1;

-- Check for invalid prices
SELECT COUNT(*) as invalid_price_count
FROM products
WHERE price <= 0 OR price IS NULL;

-- Implement as constraints (Delta Lake 2.0+)
ALTER TABLE products ADD CONSTRAINT valid_price CHECK (price > 0);
ALTER TABLE products ADD CONSTRAINT valid_product_id CHECK (product_id IS NOT NULL);


-- =========================================================
-- KEY LEARNINGS SUMMARY
-- =========================================================

%md
## Day 3 Key Learnings

### Delta Lake Fundamentals
1. **Delta Lake = Parquet + Transaction Log + ACID**
2. **Transaction log** enables time travel, ACID, and audit trail
3. **Time travel**: VERSION AS OF (version number) or TIMESTAMP AS OF (timestamp)
4. **ACID transactions** ensure data reliability and consistency

### Table Operations
1. **CREATE TABLE**: Managed (default location) vs External (specified location)
2. **INSERT, UPDATE, DELETE**: Standard SQL operations with ACID guarantees
3. **MERGE**: Efficient upsert operations (INSERT + UPDATE)
4. **RESTORE**: Revert table to previous version

### Optimization
1. **OPTIMIZE**: Compact small files for better performance
2. **Z-ORDER**: Co-locate related data (max 4 columns recommended)
3. **VACUUM**: Remove old files (default 7 days retention)
4. **Partitioning**: Organize data by column values

### Schema Evolution
1. **ALTER TABLE**: Add columns dynamically
2. **mergeSchema**: Automatically merge schemas on write
3. **Schema enforcement**: Validates data types on write

### Best Practices
1. Use Delta Lake for all production tables
2. Run OPTIMIZE regularly for read-heavy tables
3. Use Z-ORDER on frequently filtered columns
4. Set appropriate VACUUM retention period
5. Enable Change Data Feed for CDC workloads
6. Partition large tables by date or category
7. Use time travel for debugging and auditing

**Next**: Day 4 - Databricks File System (DBFS)
