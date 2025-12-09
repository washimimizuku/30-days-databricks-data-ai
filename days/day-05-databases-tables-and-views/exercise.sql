-- Day 5: Databases, Tables, and Views - Exercises
-- =========================================================

-- =========================================================
-- PART 1: DATABASE OPERATIONS
-- =========================================================

-- Exercise 1: Create Database
-- TODO: Create a database called 'company_db'

-- YOUR CODE HERE


-- Exercise 2: Create Database with Location
-- TODO: Create a database 'external_db' with location '/FileStore/external_db/'

-- YOUR CODE HERE


-- Exercise 3: List Databases
-- TODO: Show all databases

-- YOUR CODE HERE


-- Exercise 4: Use Database
-- TODO: Switch to company_db

-- YOUR CODE HERE


-- Exercise 5: Describe Database
-- TODO: Get detailed information about company_db

-- YOUR CODE HERE


-- =========================================================
-- PART 2: MANAGED TABLES
-- =========================================================

-- Exercise 6: Create Basic Managed Table
-- TODO: Create a managed table 'customers' with columns:
-- customer_id (INT), name (STRING), email (STRING), city (STRING)

-- YOUR CODE HERE


-- Exercise 7: Insert Data
-- TODO: Insert at least 5 customers

-- YOUR CODE HERE


-- Exercise 8: Create Table from Query (CTAS)
-- TODO: Create 'customers_backup' from customers table

-- YOUR CODE HERE


-- Exercise 9: Check Table Location
-- TODO: Verify the table is in the default warehouse location

-- YOUR CODE HERE


-- Exercise 10: Drop Managed Table
-- TODO: Drop customers_backup and verify data files are deleted

-- YOUR CODE HERE


-- =========================================================
-- PART 3: EXTERNAL TABLES
-- =========================================================

-- Exercise 11: Create External Table
-- TODO: Create external table 'customers_external' at '/FileStore/day5/customers'

-- YOUR CODE HERE


-- Exercise 12: Insert Data into External Table
-- TODO: Copy data from customers to customers_external

-- YOUR CODE HERE


-- Exercise 13: Verify External Location
-- TODO: Check that data is in the specified location

-- YOUR CODE HERE


-- Exercise 14: Drop External Table
-- TODO: Drop customers_external and verify data files remain

-- YOUR CODE HERE


-- Exercise 15: Recreate External Table from Existing Data
-- TODO: Recreate customers_external pointing to the same location

-- YOUR CODE HERE


-- =========================================================
-- PART 4: VIEWS
-- =========================================================

-- Exercise 16: Create Standard View
-- TODO: Create view 'local_customers' for customers in 'New York'

-- YOUR CODE HERE


-- Exercise 17: Create Temporary View
-- TODO: Create temp view 'premium_customers' for customers with orders > $1000

-- YOUR CODE HERE


-- Exercise 18: Create Global Temporary View
-- TODO: Create global temp view 'customer_summary' with count by city

-- YOUR CODE HERE


-- Exercise 19: Query Views
-- TODO: Query all three views

-- YOUR CODE HERE


-- Exercise 20: Drop View
-- TODO: Drop the local_customers view

-- YOUR CODE HERE


-- =========================================================
-- PART 5: TABLE TYPES AND FORMATS
-- =========================================================

-- Exercise 21: Create Delta Table
-- TODO: Create a Delta table 'transactions'

-- YOUR CODE HERE


-- Exercise 22: Create Parquet Table
-- TODO: Create a Parquet table from transactions

-- YOUR CODE HERE


-- Exercise 23: Create CSV Table
-- TODO: Create a CSV table from transactions

-- YOUR CODE HERE


-- Exercise 24: Compare Formats
-- TODO: Compare file sizes and query performance

-- YOUR CODE HERE


-- Exercise 25: Convert to Delta
-- TODO: Convert Parquet table to Delta format

-- YOUR CODE HERE


-- =========================================================
-- PART 6: PARTITIONING
-- =========================================================

-- Exercise 26: Create Partitioned Table
-- TODO: Create 'sales' table partitioned by sale_date

-- YOUR CODE HERE


-- Exercise 27: Insert Partitioned Data
-- TODO: Insert data spanning multiple dates

-- YOUR CODE HERE


-- Exercise 28: Show Partitions
-- TODO: List all partitions

-- YOUR CODE HERE


-- Exercise 29: Query Specific Partition
-- TODO: Query data for a specific date

-- YOUR CODE HERE


-- Exercise 30: Add Partition
-- TODO: Insert data for a new date partition

-- YOUR CODE HERE


-- =========================================================
-- PART 7: TABLE METADATA
-- =========================================================

-- Exercise 31: Describe Table
-- TODO: Get table structure for customers

-- YOUR CODE HERE


-- Exercise 32: Describe Extended
-- TODO: Get detailed table information

-- YOUR CODE HERE


-- Exercise 33: Show Create Table
-- TODO: Display CREATE TABLE statement

-- YOUR CODE HERE


-- Exercise 34: Show Columns
-- TODO: List all columns in customers table

-- YOUR CODE HERE


-- Exercise 35: Show Table Properties
-- TODO: Display table properties

-- YOUR CODE HERE


-- =========================================================
-- PART 8: ALTER TABLE OPERATIONS
-- =========================================================

-- Exercise 36: Add Column
-- TODO: Add 'phone' column to customers

-- YOUR CODE HERE


-- Exercise 37: Rename Column
-- TODO: Rename 'phone' to 'phone_number'

-- YOUR CODE HERE


-- Exercise 38: Add Column Comment
-- TODO: Add comment to email column

-- YOUR CODE HERE


-- Exercise 39: Set Table Properties
-- TODO: Add description property to customers table

-- YOUR CODE HERE


-- Exercise 40: Rename Table
-- TODO: Rename customers to clients

-- YOUR CODE HERE


-- =========================================================
-- REFLECTION QUESTIONS
-- =========================================================
-- Answer these in a markdown cell:
--
-- 1. What is the difference between managed and external tables?
-- 2. What happens when you DROP a managed table?
-- 3. What happens when you DROP an external table?
-- 4. When should you use managed vs external tables?
-- 5. What are the three types of views?
-- 6. What is the scope of a temporary view?
-- 7. How do you access a global temporary view?
-- 8. What is the benefit of partitioning?
-- 9. Where are managed tables stored by default?
-- 10. Why is Delta Lake recommended over other formats?


-- =========================================================
-- BONUS CHALLENGES
-- =========================================================

-- Bonus 1: Create Complete Database Schema
-- TODO: Create a complete e-commerce schema with:
-- - customers table
-- - products table
-- - orders table (partitioned by order_date)
-- - order_items table
-- - Appropriate views

-- YOUR CODE HERE


-- Bonus 2: Implement Slowly Changing Dimension
-- TODO: Create a customer dimension table with history tracking

-- YOUR CODE HERE


-- Bonus 3: Table Comparison Report
-- TODO: Create a view that compares managed vs external tables

-- YOUR CODE HERE


-- Bonus 4: Partition Management
-- TODO: Create a procedure to manage old partitions

-- YOUR CODE HERE


-- Bonus 5: Metadata Catalog
-- TODO: Create a view that catalogs all tables and their properties

-- YOUR CODE HERE


-- =========================================================
-- CLEANUP (Optional)
-- =========================================================

-- Uncomment to clean up
-- DROP DATABASE IF EXISTS company_db CASCADE;
-- DROP DATABASE IF EXISTS external_db CASCADE;
-- DROP DATABASE IF EXISTS day5_practice CASCADE;


-- =========================================================
-- KEY LEARNINGS CHECKLIST
-- =========================================================
-- [ ] Created and managed databases
-- [ ] Created managed tables
-- [ ] Created external tables
-- [ ] Understood DROP behavior differences
-- [ ] Created standard, temp, and global temp views
-- [ ] Worked with different table formats
-- [ ] Implemented partitioning
-- [ ] Used table metadata commands
-- [ ] Performed ALTER TABLE operations
-- [ ] Compared managed vs external tables
