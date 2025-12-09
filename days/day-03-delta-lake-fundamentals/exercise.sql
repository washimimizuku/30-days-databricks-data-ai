-- Day 3: Delta Lake Fundamentals - Exercises
-- =========================================================
-- Complete these exercises to master Delta Lake basics

-- =========================================================
-- PART 1: CREATING DELTA TABLES
-- =========================================================

-- Exercise 1: Create a Basic Delta Table
-- TODO: Create a table called 'products' with columns:
-- - product_id (INT)
-- - product_name (STRING)
-- - category (STRING)
-- - price (DECIMAL(10,2))
-- - in_stock (BOOLEAN)

-- YOUR CODE HERE


-- Exercise 2: Create Table with Data (CTAS)
-- TODO: Create a table 'products_backup' from the products table

-- YOUR CODE HERE


-- Exercise 3: Create External Delta Table
-- TODO: Create an external table 'products_external' at location '/tmp/delta/products_external'

-- YOUR CODE HERE


-- Exercise 4: Insert Data into Delta Table
-- TODO: Insert at least 5 products into the products table

-- YOUR CODE HERE


-- Exercise 5: Verify Table Format
-- TODO: Write a query to confirm the table is using Delta format
-- Hint: Use DESCRIBE DETAIL or SHOW CREATE TABLE

-- YOUR CODE HERE


-- =========================================================
-- PART 2: TIME TRAVEL
-- =========================================================

-- Exercise 6: Create Multiple Versions
-- TODO: Perform these operations to create versions:
-- 1. INSERT 3 more products (Version 1)
-- 2. UPDATE prices by increasing 10% (Version 2)
-- 3. DELETE one product (Version 3)

-- Version 1: INSERT
-- YOUR CODE HERE

-- Version 2: UPDATE
-- YOUR CODE HERE

-- Version 3: DELETE
-- YOUR CODE HERE


-- Exercise 7: View Table History
-- TODO: Display the history of the products table

-- YOUR CODE HERE


-- Exercise 8: Query by Version Number
-- TODO: Query the products table as it existed at version 0 (initial state)

-- YOUR CODE HERE


-- Exercise 9: Query by Timestamp
-- TODO: Query the products table as it existed 5 minutes ago
-- Hint: Use current_timestamp() - INTERVAL 5 MINUTES

-- YOUR CODE HERE


-- Exercise 10: Compare Versions
-- TODO: Write a query that shows the difference between version 0 and current version
-- Hint: Use UNION or JOIN with VERSION AS OF

-- YOUR CODE HERE


-- =========================================================
-- PART 3: SCHEMA EVOLUTION
-- =========================================================

-- Exercise 11: Add a Column
-- TODO: Add a 'supplier' column (STRING) to the products table

-- YOUR CODE HERE


-- Exercise 12: Insert Data with New Schema
-- TODO: Insert a product with the new supplier column

-- YOUR CODE HERE


-- Exercise 13: Schema Merge on Write
-- TODO: Create a DataFrame with an additional 'discount' column and append it
-- Use Python cell with mergeSchema option

%python
# YOUR CODE HERE


-- Exercise 14: View Schema History
-- TODO: Query different versions to see schema evolution

-- YOUR CODE HERE


-- =========================================================
-- PART 4: ACID TRANSACTIONS
-- =========================================================

-- Exercise 15: Atomic Transaction
-- TODO: Perform multiple operations that should succeed or fail together
-- Insert a product and update another product's price

-- YOUR CODE HERE


-- Exercise 16: Concurrent Reads
-- TODO: While updating data, query the table from another cell
-- Observe that reads are not blocked by writes

-- Cell 1: Start a long-running update
-- YOUR CODE HERE

-- Cell 2: Query while update is running
-- YOUR CODE HERE


-- Exercise 17: Transaction Isolation
-- TODO: Demonstrate that uncommitted changes are not visible to other queries

-- YOUR CODE HERE


-- =========================================================
-- PART 5: TABLE OPERATIONS
-- =========================================================

-- Exercise 18: UPDATE Operation
-- TODO: Update all products in 'Electronics' category to increase price by 15%

-- YOUR CODE HERE


-- Exercise 19: DELETE Operation
-- TODO: Delete all products that are out of stock (in_stock = false)

-- YOUR CODE HERE


-- Exercise 20: MERGE Operation (UPSERT)
-- TODO: Create a source table and merge it into products
-- - If product_id exists: UPDATE price
-- - If product_id doesn't exist: INSERT new product

-- Create source table
CREATE OR REPLACE TEMP VIEW new_products AS
SELECT * FROM VALUES
    (1, 'Updated Product', 'Electronics', 299.99, true, 'Supplier A'),
    (99, 'New Product', 'Books', 19.99, true, 'Supplier B')
AS t(product_id, product_name, category, price, in_stock, supplier);

-- TODO: Write MERGE statement
-- YOUR CODE HERE


-- =========================================================
-- PART 6: OPTIMIZATION
-- =========================================================

-- Exercise 21: OPTIMIZE Table
-- TODO: Run OPTIMIZE on the products table to compact small files

-- YOUR CODE HERE


-- Exercise 22: Z-ORDER Optimization
-- TODO: Optimize the products table with Z-ORDER on category and price

-- YOUR CODE HERE


-- Exercise 23: Check File Statistics
-- TODO: View the number and size of data files before and after OPTIMIZE

%python
# Before OPTIMIZE
# YOUR CODE HERE

# After OPTIMIZE
# YOUR CODE HERE


-- Exercise 24: VACUUM Old Files
-- TODO: Remove files older than 7 days (168 hours)
-- Note: Be careful with VACUUM in production!

-- YOUR CODE HERE


-- =========================================================
-- PART 7: TRANSACTION LOG
-- =========================================================

-- Exercise 25: Explore Transaction Log
-- TODO: List files in the _delta_log directory

%python
# YOUR CODE HERE


-- Exercise 26: Read Transaction Log
-- TODO: Read and display the content of the first transaction log file

%python
# YOUR CODE HERE


-- Exercise 27: Understand Log Entries
-- TODO: Identify what operations are recorded in the transaction log
-- Look for: add, remove, metaData, protocol entries

-- YOUR CODE HERE


-- =========================================================
-- PART 8: MANAGED VS EXTERNAL TABLES
-- =========================================================

-- Exercise 28: Create Managed Table
-- TODO: Create a managed table and note its location

CREATE TABLE managed_test (id INT, name STRING) USING DELTA;

-- Check location
DESCRIBE DETAIL managed_test;


-- Exercise 29: Create External Table
-- TODO: Create an external table at a specific location

-- YOUR CODE HERE


-- Exercise 30: DROP Table Comparison
-- TODO: Drop both tables and observe the difference
-- For managed table: both metadata and data are deleted
-- For external table: only metadata is deleted, data remains

-- Drop managed table
DROP TABLE managed_test;

-- Check if data files still exist
%python
# YOUR CODE HERE

-- Drop external table
-- YOUR CODE HERE

-- Check if data files still exist
%python
# YOUR CODE HERE


-- =========================================================
-- PART 9: RESTORE TABLE
-- =========================================================

-- Exercise 31: Restore to Previous Version
-- TODO: Restore the products table to version 1

-- YOUR CODE HERE


-- Exercise 32: Restore by Timestamp
-- TODO: Restore the products table to 10 minutes ago

-- YOUR CODE HERE


-- Exercise 33: Verify Restore
-- TODO: Check table history to confirm restore operation

-- YOUR CODE HERE


-- =========================================================
-- PART 10: ADVANCED SCENARIOS
-- =========================================================

-- Exercise 34: Partitioned Delta Table
-- TODO: Create a partitioned Delta table by category

CREATE TABLE products_partitioned (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10,2)
)
USING DELTA
PARTITIONED BY (category);

-- Insert data
-- YOUR CODE HERE


-- Exercise 35: Clone Table (Shallow Copy)
-- TODO: Create a shallow clone of the products table

-- YOUR CODE HERE


-- Exercise 36: Clone Table (Deep Copy)
-- TODO: Create a deep clone of the products table

-- YOUR CODE HERE


-- Exercise 37: Change Data Feed
-- TODO: Enable Change Data Feed on products table

-- YOUR CODE HERE


-- Exercise 38: Read Change Data
-- TODO: Make changes and read the change data feed

-- Make changes
UPDATE products SET price = price * 1.1 WHERE category = 'Electronics';

-- Read changes
-- YOUR CODE HERE


-- Exercise 39: Table Properties
-- TODO: Set custom table properties

-- YOUR CODE HERE


-- Exercise 40: Performance Comparison
-- TODO: Compare query performance on Delta vs Parquet
-- Create same data in both formats and compare query times

-- Create Parquet table
-- YOUR CODE HERE

-- Create Delta table
-- YOUR CODE HERE

-- Compare query performance
-- YOUR CODE HERE


-- =========================================================
-- REFLECTION QUESTIONS
-- =========================================================
-- Answer these in a markdown cell:
--
-- 1. What are the key differences between Delta Lake and Parquet?
-- 2. How does time travel work in Delta Lake?
-- 3. What is the purpose of the transaction log?
-- 4. When should you run OPTIMIZE on a Delta table?
-- 5. What is the difference between managed and external tables?
-- 6. How does Delta Lake ensure ACID properties?
-- 7. What happens when you VACUUM a Delta table?
-- 8. How does schema evolution work in Delta Lake?
-- 9. What is Z-ordering and when should you use it?
-- 10. Why is Delta Lake important for lakehouse architecture?


-- =========================================================
-- BONUS CHALLENGES
-- =========================================================

-- Bonus 1: Build a Mini Lakehouse
-- TODO: Create a three-layer architecture (Bronze, Silver, Gold)
-- - Bronze: Raw data (products_raw)
-- - Silver: Cleaned data (products_clean)
-- - Gold: Aggregated data (products_summary)

-- YOUR CODE HERE


-- Bonus 2: Implement SCD Type 2
-- TODO: Implement Slowly Changing Dimension Type 2 for products
-- Track historical changes with effective_date and end_date

-- YOUR CODE HERE


-- Bonus 3: Optimize for Query Patterns
-- TODO: Given these common queries, optimize the table:
-- - Filter by category
-- - Filter by price range
-- - Filter by in_stock status

-- YOUR CODE HERE


-- Bonus 4: Audit Trail
-- TODO: Create a query that shows all changes made to a specific product
-- Use DESCRIBE HISTORY and time travel

-- YOUR CODE HERE


-- Bonus 5: Data Quality Checks
-- TODO: Implement data quality checks using Delta Lake features
-- - Check for null values
-- - Check for duplicates
-- - Check for invalid prices

-- YOUR CODE HERE


-- =========================================================
-- CLEANUP (Optional)
-- =========================================================

-- Uncomment to clean up after exercises
-- DROP DATABASE IF EXISTS day3_delta CASCADE;
-- DROP TABLE IF EXISTS products;
-- DROP TABLE IF EXISTS products_backup;
-- DROP TABLE IF EXISTS products_external;
-- DROP TABLE IF EXISTS products_partitioned;


-- =========================================================
-- KEY LEARNINGS CHECKLIST
-- =========================================================
-- Mark these as you complete them:
-- [ ] Created Delta tables using multiple methods
-- [ ] Used time travel to query historical data
-- [ ] Implemented schema evolution
-- [ ] Performed OPTIMIZE and VACUUM operations
-- [ ] Understood ACID transaction properties
-- [ ] Explored transaction log structure
-- [ ] Compared managed vs external tables
-- [ ] Used MERGE for upsert operations
-- [ ] Implemented Z-ordering for optimization
-- [ ] Restored tables to previous versions
