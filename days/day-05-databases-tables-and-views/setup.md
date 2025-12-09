# Day 5 Setup: Databases, Tables, and Views

## Prerequisites

- Completed Days 1-4
- Active Databricks cluster
- Workspace access
- Basic SQL knowledge

## Setup Steps

### Step 1: Verify Cluster and Create Notebook

1. Navigate to **Compute** and ensure your cluster is running
2. Go to **Workspace** → Your user folder → `30-days-bootcamp`
3. Create new notebook:
   - Name: `Day 5 - Databases Tables Views`
   - Language: **SQL**
   - Cluster: Select your `learning-cluster`
4. Click **Create**

### Step 2: Create Test Database

```sql
-- Create database for Day 5
CREATE DATABASE IF NOT EXISTS day5_tables;

-- Use the database
USE day5_tables;

-- Verify current database
SELECT current_database() as current_db;
```

Expected output: `current_db: day5_tables`

### Step 3: Create Sample Managed Table

```sql
-- Create managed table
CREATE TABLE employees (
    employee_id INT,
    first_name STRING,
    last_name STRING,
    department STRING,
    salary DECIMAL(10,2),
    hire_date DATE
) USING DELTA;

-- Insert sample data
INSERT INTO employees VALUES
    (1, 'Alice', 'Johnson', 'Engineering', 95000.00, '2020-01-15'),
    (2, 'Bob', 'Smith', 'Sales', 75000.00, '2021-03-20'),
    (3, 'Charlie', 'Brown', 'Engineering', 105000.00, '2019-06-10'),
    (4, 'Diana', 'Prince', 'Marketing', 85000.00, '2020-09-05'),
    (5, 'Eve', 'Davis', 'Engineering', 98000.00, '2021-01-12'),
    (6, 'Frank', 'Miller', 'Sales', 72000.00, '2022-02-15'),
    (7, 'Grace', 'Lee', 'Engineering', 110000.00, '2019-11-20'),
    (8, 'Henry', 'Wilson', 'Marketing', 88000.00, '2020-07-30');

-- Verify data
SELECT * FROM employees;
```

### Step 4: Check Table Location

```sql
-- Describe table details
DESCRIBE DETAIL employees;

-- Note the location - should be in /user/hive/warehouse/day5_tables.db/employees/
```

### Step 5: Create Sample External Table

```sql
-- Create external table
CREATE TABLE employees_external (
    employee_id INT,
    first_name STRING,
    last_name STRING,
    department STRING
) USING DELTA
LOCATION '/FileStore/day5-external/employees';

-- Insert data
INSERT INTO employees_external
SELECT employee_id, first_name, last_name, department
FROM employees;

-- Verify
SELECT * FROM employees_external;

-- Check location
DESCRIBE DETAIL employees_external;
```

### Step 6: Create Sample Views

```sql
-- Standard view
CREATE VIEW engineering_employees AS
SELECT * FROM employees WHERE department = 'Engineering';

-- Temporary view
CREATE TEMP VIEW high_earners AS
SELECT * FROM employees WHERE salary > 90000;

-- Global temporary view
CREATE GLOBAL TEMP VIEW all_departments AS
SELECT DISTINCT department FROM employees;

-- Query views
SELECT * FROM engineering_employees;
SELECT * FROM high_earners;
SELECT * FROM global_temp.all_departments;
```

### Step 7: Create Partitioned Table

```sql
-- Create partitioned table
CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    amount DECIMAL(10,2),
    order_date DATE
) USING DELTA
PARTITIONED BY (order_date);

-- Insert sample data
INSERT INTO orders VALUES
    (1, 101, 250.00, '2024-01-15'),
    (2, 102, 175.50, '2024-01-15'),
    (3, 103, 420.00, '2024-01-16'),
    (4, 101, 310.25, '2024-01-16'),
    (5, 104, 89.99, '2024-01-17'),
    (6, 102, 550.00, '2024-01-17'),
    (7, 105, 125.75, '2024-01-18'),
    (8, 103, 299.99, '2024-01-18');

-- Verify partitions
SHOW PARTITIONS orders;
```

### Step 8: Explore Table Metadata

```sql
-- List all tables
SHOW TABLES;

-- Describe table
DESCRIBE TABLE employees;
DESCRIBE EXTENDED employees;

-- Show create statement
SHOW CREATE TABLE employees;

-- Show columns
SHOW COLUMNS IN employees;
```

### Step 9: Create Tables with Different Formats

```sql
-- Parquet table
CREATE TABLE employees_parquet
USING PARQUET
AS SELECT * FROM employees;

-- CSV table (for comparison)
CREATE TABLE employees_csv
USING CSV
OPTIONS (header='true')
AS SELECT * FROM employees;

-- Verify formats
DESCRIBE DETAIL employees_parquet;
DESCRIBE DETAIL employees_csv;
```

### Step 10: Set Up for Exercises

```sql
-- Create additional database for exercises
CREATE DATABASE IF NOT EXISTS day5_practice;

-- Create sample products table
USE day5_tables;
CREATE TABLE products (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10,2),
    in_stock BOOLEAN
) USING DELTA;

INSERT INTO products VALUES
    (1, 'Laptop', 'Electronics', 999.99, true),
    (2, 'Mouse', 'Electronics', 29.99, true),
    (3, 'Desk Chair', 'Furniture', 199.99, true),
    (4, 'Monitor', 'Electronics', 299.99, false),
    (5, 'Keyboard', 'Electronics', 79.99, true);

-- Verify setup
SELECT * FROM products;
```

## Verification Checklist

- [ ] Cluster is running
- [ ] Day 5 notebook created
- [ ] Database `day5_tables` created
- [ ] Managed table `employees` created with data
- [ ] External table `employees_external` created
- [ ] Standard view `engineering_employees` created
- [ ] Temporary view `high_earners` created
- [ ] Global temp view `all_departments` created
- [ ] Partitioned table `orders` created
- [ ] Tables with different formats created
- [ ] Can query all tables and views

## Troubleshooting

### Issue: "Database already exists"
**Solution**:
```sql
-- Drop and recreate
DROP DATABASE IF EXISTS day5_tables CASCADE;
CREATE DATABASE day5_tables;
```

### Issue: "Table already exists"
**Solution**:
```sql
-- Drop and recreate
DROP TABLE IF EXISTS employees;
-- Then recreate the table
```

### Issue: Can't access global temp view
**Solution**:
```sql
-- Must use global_temp prefix
SELECT * FROM global_temp.all_departments;
```

### Issue: External table location error
**Solution**:
```sql
-- Use FileStore which is accessible
CREATE TABLE employees_external (...)
USING DELTA
LOCATION '/FileStore/day5-external/employees';
```

## Understanding Table Locations

### Managed Table Location
```
/user/hive/warehouse/day5_tables.db/employees/
├── _delta_log/
│   └── 00000000000000000000.json
└── part-00000-*.parquet
```

### External Table Location
```
/FileStore/day5-external/employees/
├── _delta_log/
│   └── 00000000000000000000.json
└── part-00000-*.parquet
```

### Verify Locations
```python
%python
# Check managed table location
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/day5_tables.db/employees/"))

# Check external table location
display(dbutils.fs.ls("dbfs:/FileStore/day5-external/employees/"))
```

## Quick Reference Commands

```sql
-- Database operations
CREATE DATABASE db_name;
USE db_name;
SHOW DATABASES;
DROP DATABASE db_name CASCADE;

-- Managed table
CREATE TABLE table_name (...) USING DELTA;

-- External table
CREATE TABLE table_name (...) USING DELTA LOCATION '/path/';

-- Views
CREATE VIEW view_name AS SELECT ...;
CREATE TEMP VIEW view_name AS SELECT ...;
CREATE GLOBAL TEMP VIEW view_name AS SELECT ...;

-- Metadata
DESCRIBE TABLE table_name;
DESCRIBE DETAIL table_name;
SHOW TABLES;
SHOW CREATE TABLE table_name;

-- Partitioning
CREATE TABLE table_name (...) PARTITIONED BY (column);
SHOW PARTITIONS table_name;
```

## What's Next?

You're now ready for Day 5 exercises! You'll practice:
- Creating and managing databases
- Creating managed and external tables
- Understanding DROP behavior differences
- Creating and using different view types
- Working with table metadata
- Implementing partitioning strategies
- Comparing table formats

**Tip**: Pay special attention to managed vs external table differences - this is heavily tested on the exam!
