# Day 5: Databases, Tables, and Views

## Learning Objectives

By the end of today, you will:
- Understand databases (schemas) in Databricks
- Create and manage managed tables
- Create and manage external tables
- Understand the differences between managed and external tables
- Create and use views (standard, temporary, global temporary)
- Work with table metadata and properties
- Understand table types and their use cases

## Topics Covered

### 1. Databases (Schemas)

**Definition**: A database (also called schema) is a logical grouping of tables, views, and functions.

**Key Concepts**:
- Databases organize related objects
- Default database is `default`
- Database names are case-insensitive
- Each database has a location in DBFS

**Hierarchy**:
```
Catalog (Unity Catalog)
└── Database/Schema
    ├── Tables
    ├── Views
    └── Functions
```

**Create Database**:
```sql
-- Basic creation
CREATE DATABASE my_database;

-- With IF NOT EXISTS
CREATE DATABASE IF NOT EXISTS my_database;

-- With location
CREATE DATABASE my_database
LOCATION '/mnt/data/my_database';

-- With comment
CREATE DATABASE my_database
COMMENT 'This is my database';
```

**Database Operations**:
```sql
-- List databases
SHOW DATABASES;
SHOW SCHEMAS;  -- Same as SHOW DATABASES

-- Use database
USE my_database;

-- Show current database
SELECT current_database();

-- Describe database
DESCRIBE DATABASE my_database;
DESCRIBE DATABASE EXTENDED my_database;

-- Drop database
DROP DATABASE my_database;
DROP DATABASE IF EXISTS my_database CASCADE;  -- CASCADE drops all tables
```

### 2. Managed Tables

**Definition**: Tables where Databricks manages both metadata and data files.

**Characteristics**:
- Data stored in default location (`/user/hive/warehouse/`)
- DROP TABLE deletes both metadata and data
- Databricks controls data lifecycle
- Best for data owned by Databricks

**Create Managed Table**:
```sql
-- Basic creation
CREATE TABLE employees (
    id INT,
    name STRING,
    department STRING,
    salary DECIMAL(10,2)
) USING DELTA;

-- With partitioning
CREATE TABLE sales (
    order_id INT,
    amount DECIMAL(10,2),
    order_date DATE
) USING DELTA
PARTITIONED BY (order_date);

-- From query (CTAS)
CREATE TABLE employees_backup
AS SELECT * FROM employees;

-- With table properties
CREATE TABLE products (
    product_id INT,
    product_name STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

**Location**:
- Default: `/user/hive/warehouse/<database>.db/<table>/`
- Example: `/user/hive/warehouse/my_database.db/employees/`

### 3. External Tables

**Definition**: Tables where Databricks manages only metadata; data files are in a specified location.

**Characteristics**:
- Data stored in user-specified location
- DROP TABLE deletes only metadata, data remains
- User controls data lifecycle
- Best for data shared across systems

**Create External Table**:
```sql
-- Basic external table
CREATE TABLE employees_external (
    id INT,
    name STRING,
    department STRING
) USING DELTA
LOCATION '/mnt/data/employees';

-- From existing data
CREATE TABLE existing_data
USING DELTA
LOCATION '/mnt/data/existing_delta_table';

-- With partitioning
CREATE TABLE sales_external (
    order_id INT,
    amount DECIMAL(10,2),
    order_date DATE
) USING DELTA
PARTITIONED BY (order_date)
LOCATION '/mnt/data/sales';
```

### 4. Managed vs External Tables

| Aspect | Managed Table | External Table |
|--------|---------------|----------------|
| **Data Location** | Default warehouse location | User-specified location |
| **Metadata** | Managed by Databricks | Managed by Databricks |
| **Data Files** | Managed by Databricks | Managed by user |
| **DROP TABLE** | Deletes metadata + data | Deletes metadata only |
| **Use Case** | Data owned by Databricks | Shared data, external systems |
| **Control** | Databricks controls lifecycle | User controls lifecycle |

**When to Use**:
- **Managed**: Data used only within Databricks, temporary data, development
- **External**: Production data, data shared with other systems, data you want to keep after DROP

### 5. Views

**Definition**: Virtual tables based on SQL queries; don't store data, only the query definition.

**Types of Views**:

**1. Standard Views (Persistent)**:
```sql
-- Create view
CREATE VIEW high_earners AS
SELECT * FROM employees WHERE salary > 100000;

-- Create or replace
CREATE OR REPLACE VIEW high_earners AS
SELECT * FROM employees WHERE salary > 90000;

-- Query view
SELECT * FROM high_earners;

-- Drop view
DROP VIEW high_earners;
```

**2. Temporary Views (Session-scoped)**:
```sql
-- Create temporary view
CREATE TEMP VIEW session_data AS
SELECT * FROM employees WHERE department = 'Engineering';

-- Available only in current session
SELECT * FROM session_data;

-- Automatically dropped when session ends
```

**3. Global Temporary Views (Cluster-scoped)**:
```sql
-- Create global temp view
CREATE GLOBAL TEMP VIEW cluster_data AS
SELECT * FROM employees;

-- Access via global_temp database
SELECT * FROM global_temp.cluster_data;

-- Available across notebooks in same cluster
-- Dropped when cluster terminates
```

**View Comparison**:

| Type | Scope | Persistence | Access |
|------|-------|-------------|--------|
| **Standard View** | Database | Permanent | All users with permissions |
| **Temp View** | Session | Session only | Current session only |
| **Global Temp View** | Cluster | Cluster lifetime | All sessions on cluster |

### 6. Table Types

**1. Delta Tables** (Recommended):
```sql
CREATE TABLE delta_table (...) USING DELTA;
```
- ACID transactions
- Time travel
- Schema evolution
- Best for production

**2. Parquet Tables**:
```sql
CREATE TABLE parquet_table (...) USING PARQUET;
```
- Columnar format
- No ACID guarantees
- Good for read-only data

**3. CSV Tables**:
```sql
CREATE TABLE csv_table (...) USING CSV;
```
- Human-readable
- Slower performance
- Good for small datasets

**4. JSON Tables**:
```sql
CREATE TABLE json_table (...) USING JSON;
```
- Semi-structured data
- Flexible schema
- Good for nested data

### 7. Table Metadata

**View Table Information**:
```sql
-- Describe table structure
DESCRIBE TABLE employees;
DESCRIBE EXTENDED employees;
DESCRIBE FORMATTED employees;

-- Show table details
DESCRIBE DETAIL employees;

-- Show create statement
SHOW CREATE TABLE employees;

-- Show table properties
SHOW TBLPROPERTIES employees;

-- Show columns
SHOW COLUMNS IN employees;

-- Show partitions
SHOW PARTITIONS sales;
```

**Table Properties**:
```sql
-- Set properties
ALTER TABLE employees SET TBLPROPERTIES (
    'description' = 'Employee master data',
    'owner' = 'data_team'
);

-- Unset properties
ALTER TABLE employees UNSET TBLPROPERTIES ('owner');
```

### 8. Table Operations

**Alter Table**:
```sql
-- Add column
ALTER TABLE employees ADD COLUMN email STRING;

-- Rename column
ALTER TABLE employees RENAME COLUMN email TO email_address;

-- Change column comment
ALTER TABLE employees ALTER COLUMN salary COMMENT 'Annual salary in USD';

-- Drop column (Delta Lake)
ALTER TABLE employees DROP COLUMN email_address;

-- Rename table
ALTER TABLE employees RENAME TO staff;
```

**Table Constraints** (Delta Lake 2.0+):
```sql
-- Add constraint
ALTER TABLE employees ADD CONSTRAINT valid_salary CHECK (salary > 0);

-- Drop constraint
ALTER TABLE employees DROP CONSTRAINT valid_salary;
```

### 9. Partitioning

**Purpose**: Organize data into subdirectories for better query performance.

**Create Partitioned Table**:
```sql
CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    amount DECIMAL(10,2),
    order_date DATE
) USING DELTA
PARTITIONED BY (order_date);
```

**Benefits**:
- Faster queries on partition columns
- Partition pruning (skip irrelevant partitions)
- Better organization

**Considerations**:
- Don't over-partition (too many small files)
- Partition on frequently filtered columns
- Typically partition by date/time

### 10. Best Practices

**Database Management**:
1. Use descriptive database names
2. Organize by project or domain
3. Document database purpose
4. Use separate databases for dev/test/prod

**Table Management**:
1. Use Delta Lake for all production tables
2. Use managed tables for Databricks-only data
3. Use external tables for shared data
4. Add comments and descriptions
5. Set appropriate table properties

**View Management**:
1. Use views for complex queries
2. Use temp views for session-specific logic
3. Document view purpose
4. Keep views simple and maintainable

**Partitioning**:
1. Partition large tables (> 1TB)
2. Partition on frequently filtered columns
3. Avoid over-partitioning
4. Monitor partition sizes

## Hands-On Exercises

See `exercise.sql` for practical exercises covering:
- Creating and managing databases
- Creating managed and external tables
- Working with different table types
- Creating and using views
- Table metadata operations
- Partitioning strategies

## Key Takeaways

1. **Databases organize related tables and views**
2. **Managed tables**: Databricks manages data and metadata (DROP deletes both)
3. **External tables**: User manages data, Databricks manages metadata (DROP keeps data)
4. **Views are virtual tables** (standard, temp, global temp)
5. **Delta Lake is recommended** for all production tables
6. **Partitioning improves query performance** on large tables
7. **Use DESCRIBE and SHOW commands** to inspect metadata

## Exam Tips

- **Know managed vs external table differences** (especially DROP behavior)
- **Understand view types** (standard, temp, global temp)
- **Know default table location** (/user/hive/warehouse/)
- **Understand partitioning benefits** and when to use it
- **Know table metadata commands** (DESCRIBE, SHOW)
- **Understand Delta Lake advantages** over other formats
- **Know how to create tables** (multiple methods)

## Common Exam Questions

1. What happens to data when you DROP a managed table?
2. What happens to data when you DROP an external table?
3. Where are managed tables stored by default?
4. What is the difference between a view and a table?
5. How do you create a temporary view?
6. What is the benefit of partitioning?
7. How do you create a table from a query (CTAS)?

## Additional Resources

- [Databricks Tables Documentation](https://docs.databricks.com/tables/index.html)
- [Databases Documentation](https://docs.databricks.com/data/databases.html)
- [Views Documentation](https://docs.databricks.com/views/index.html)

## Next Steps

Tomorrow: [Day 6 - Spark SQL Basics](../day-06-spark-sql-basics/README.md)
