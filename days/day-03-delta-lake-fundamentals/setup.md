# Day 3 Setup: Delta Lake Fundamentals

## Prerequisites

- Completed Days 1-2
- Active Databricks cluster (from Day 2)
- Workspace access
- Basic SQL knowledge

## Setup Steps

### Step 1: Verify Cluster is Running

1. Navigate to **Compute** in left sidebar
2. Check your `learning-cluster` status
3. If terminated, click **Start** and wait 3-5 minutes
4. Verify status shows "Running" (green indicator)

### Step 2: Create Day 3 Notebook

1. Navigate to **Workspace** → Your user folder
2. Open your `30-days-bootcamp` folder
3. Create new notebook:
   - Name: `Day 3 - Delta Lake Fundamentals`
   - Language: **SQL**
   - Cluster: Select your `learning-cluster`
4. Click **Create**

### Step 3: Verify Delta Lake Support

Run this in your notebook to verify Delta Lake is available:

```sql
-- Check Spark version (should be 3.x+)
SELECT current_version() as spark_version;

-- Verify Delta Lake is available
SHOW DATABASES;
```

Expected output: Spark version 3.x or higher

### Step 4: Create Sample Database

Create a database for today's exercises:

```sql
-- Create database for Day 3
CREATE DATABASE IF NOT EXISTS day3_delta;

-- Use the database
USE day3_delta;

-- Verify database is created
SHOW DATABASES LIKE 'day3_delta';
```

### Step 5: Create Sample Data

Create a simple dataset to work with:

```sql
-- Create sample employees table
CREATE TABLE employees (
    id INT,
    name STRING,
    department STRING,
    salary DECIMAL(10,2),
    hire_date DATE
) USING DELTA;

-- Insert sample data
INSERT INTO employees VALUES
    (1, 'Alice Johnson', 'Engineering', 95000.00, '2020-01-15'),
    (2, 'Bob Smith', 'Sales', 75000.00, '2021-03-20'),
    (3, 'Charlie Brown', 'Engineering', 105000.00, '2019-06-10'),
    (4, 'Diana Prince', 'Marketing', 85000.00, '2020-09-05'),
    (5, 'Eve Davis', 'Engineering', 98000.00, '2021-01-12');

-- Verify data
SELECT * FROM employees;
```

Expected output: 5 rows of employee data

### Step 6: Verify Delta Table Creation

Check that the table was created as Delta format:

```sql
-- Describe table details
DESCRIBE DETAIL employees;

-- Check table format (should show 'delta')
SHOW CREATE TABLE employees;
```

Look for `USING DELTA` in the output.

### Step 7: Explore Delta Lake Location

```python
# Switch to Python cell
%python

# Check where Delta table is stored
display(spark.sql("DESCRIBE DETAIL employees"))

# List Delta Lake files
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/day3_delta.db/employees"))

# Check transaction log
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/day3_delta.db/employees/_delta_log"))
```

You should see:
- Parquet data files (part-*.parquet)
- Transaction log directory (_delta_log/)
- JSON transaction files (00000000000000000000.json, etc.)

### Step 8: Create External Delta Table

Create an external table to understand the difference:

```sql
-- Create external table
CREATE TABLE employees_external (
    id INT,
    name STRING,
    department STRING
) USING DELTA
LOCATION '/tmp/delta/employees_external';

-- Insert data
INSERT INTO employees_external VALUES
    (1, 'Test User', 'Test Dept');

-- Verify
SELECT * FROM employees_external;
```

### Step 9: Set Up for Time Travel

Create multiple versions of data for time travel exercises:

```sql
-- Version 0: Initial data (already created)

-- Version 1: Add more employees
INSERT INTO employees VALUES
    (6, 'Frank Miller', 'Sales', 72000.00, '2022-02-15'),
    (7, 'Grace Lee', 'Engineering', 110000.00, '2019-11-20');

-- Version 2: Update salaries
UPDATE employees SET salary = salary * 1.05 WHERE department = 'Engineering';

-- Version 3: Delete an employee
DELETE FROM employees WHERE id = 2;

-- View history
DESCRIBE HISTORY employees;
```

You should now have 4 versions (0-3) to practice time travel.

## Verification Checklist

- [ ] Cluster is running
- [ ] Day 3 notebook created and attached to cluster
- [ ] Database `day3_delta` created
- [ ] Sample `employees` table created with data
- [ ] Table is confirmed as Delta format
- [ ] Transaction log directory exists
- [ ] External table created successfully
- [ ] Multiple versions created for time travel
- [ ] Can view table history

## Troubleshooting

### Issue: "Table already exists" error
**Solution**: 
```sql
-- Drop existing table and recreate
DROP TABLE IF EXISTS employees;
-- Then recreate the table
```

### Issue: Can't see transaction log files
**Solution**:
```python
%python
# Use full path
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/day3_delta.db/employees/_delta_log/"))
```

### Issue: DESCRIBE HISTORY shows no results
**Solution**: 
- Ensure you've made changes to the table (INSERT, UPDATE, DELETE)
- Each operation creates a new version
- Initial CREATE TABLE is version 0

### Issue: Permission denied when creating external table
**Solution**:
```sql
-- Use a different location
CREATE TABLE employees_external (...)
USING DELTA
LOCATION '/FileStore/delta/employees_external';
```

### Issue: Delta Lake features not working
**Solution**:
- Verify Databricks Runtime is 7.0+ (check in cluster configuration)
- Restart cluster if needed
- Ensure using `USING DELTA` in CREATE TABLE statement

## Understanding Delta Lake Structure

After setup, your Delta table structure looks like this:

```
dbfs:/user/hive/warehouse/day3_delta.db/employees/
├── _delta_log/
│   ├── 00000000000000000000.json  (Version 0: CREATE TABLE)
│   ├── 00000000000000000001.json  (Version 1: INSERT)
│   ├── 00000000000000000002.json  (Version 2: UPDATE)
│   └── 00000000000000000003.json  (Version 3: DELETE)
├── part-00000-*.parquet  (Data files)
├── part-00001-*.parquet
└── ...
```

**Transaction Log** (`_delta_log/`):
- Records all changes
- Enables time travel
- Provides ACID guarantees
- JSON format, human-readable

**Data Files** (`.parquet`):
- Actual data in Parquet format
- Columnar storage
- Compressed
- Immutable (new files created for changes)

## Quick Reference Commands

```sql
-- Create Delta table
CREATE TABLE table_name (...) USING DELTA;

-- View table history
DESCRIBE HISTORY table_name;

-- Time travel by version
SELECT * FROM table_name VERSION AS OF 2;

-- Time travel by timestamp
SELECT * FROM table_name TIMESTAMP AS OF '2024-01-01';

-- Optimize table
OPTIMIZE table_name;

-- Vacuum old files
VACUUM table_name RETAIN 168 HOURS;
```

## What's Next?

You're now ready for Day 3 exercises! You'll practice:
- Creating various types of Delta tables
- Using time travel to query historical data
- Implementing schema evolution
- Optimizing Delta tables
- Understanding ACID transactions

**Tip**: Keep your notebook organized with markdown cells to separate different exercises.

## Additional Setup (Optional)

### Create Larger Dataset for Performance Testing

```sql
-- Create larger table for optimization exercises
CREATE TABLE large_orders AS
SELECT 
    id,
    CAST(RAND() * 1000 AS INT) as customer_id,
    CAST(RAND() * 500 + 10 AS DECIMAL(10,2)) as amount,
    DATE_ADD('2024-01-01', CAST(RAND() * 365 AS INT)) as order_date,
    CASE WHEN RAND() > 0.8 THEN 'cancelled'
         WHEN RAND() > 0.5 THEN 'completed'
         ELSE 'pending' END as status
FROM RANGE(100000)
USING DELTA;
```

This creates a 100,000 row table for optimization practice.
