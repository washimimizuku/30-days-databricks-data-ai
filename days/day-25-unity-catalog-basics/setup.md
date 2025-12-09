# Day 25: Unity Catalog Basics - Setup

## Prerequisites

Before starting today's exercises, ensure you have:
- ✅ Completed Days 1-24
- ✅ Access to a Databricks workspace with Unity Catalog enabled
- ✅ Permissions to create catalogs and schemas (or use provided ones)
- ✅ Understanding of SQL basics

## Unity Catalog Requirements

### Workspace Setup

Unity Catalog requires:
1. **Databricks workspace** on AWS, Azure, or GCP
2. **Unity Catalog enabled** (check with your admin)
3. **Metastore attached** to your workspace
4. **Appropriate permissions** to create catalogs/schemas

### Check Unity Catalog Status

```sql
-- Check if Unity Catalog is enabled
SELECT current_metastore();

-- If this returns a value, Unity Catalog is enabled
-- If error, Unity Catalog is not available
```

### Permission Levels

For today's exercises, you need:
- **Full Admin**: Can create catalogs, schemas, tables, and grant permissions
- **Catalog Creator**: Can create schemas and tables in existing catalogs
- **Schema Creator**: Can create tables in existing schemas
- **Table User**: Can only query existing tables

**Note**: If you don't have admin permissions, your instructor will provide pre-created catalogs and schemas.

## Setup Instructions

### Step 1: Create Your Catalog

```sql
-- Create a catalog for today's exercises
-- Replace 'your_username' with your actual username
CREATE CATALOG IF NOT EXISTS uc_day25_<your_username>;

-- Verify creation
SHOW CATALOGS LIKE 'uc_day25_%';

-- Set as default
USE CATALOG uc_day25_<your_username>;
```

**If you can't create catalogs**, use a shared catalog:
```sql
-- Use shared catalog (provided by instructor)
USE CATALOG shared_training;
CREATE SCHEMA IF NOT EXISTS day25_<your_username>;
USE SCHEMA day25_<your_username>;
```

### Step 2: Create Schemas

```sql
-- Create schemas for medallion architecture
CREATE SCHEMA IF NOT EXISTS uc_day25_<your_username>.bronze
COMMENT 'Raw data layer';

CREATE SCHEMA IF NOT EXISTS uc_day25_<your_username>.silver
COMMENT 'Cleaned and validated data';

CREATE SCHEMA IF NOT EXISTS uc_day25_<your_username>.gold
COMMENT 'Business-level aggregates';

-- Verify schemas
SHOW SCHEMAS IN uc_day25_<your_username>;
```

### Step 3: Create Sample Data

```sql
-- Set default schema
USE SCHEMA uc_day25_<your_username>.bronze;

-- Create customers table
CREATE OR REPLACE TABLE customers (
    customer_id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    city STRING,
    state STRING,
    country STRING,
    created_date DATE
) USING DELTA;

-- Insert sample data
INSERT INTO customers VALUES
(1, 'John', 'Doe', 'john.doe@email.com', '555-0101', 'New York', 'NY', 'USA', '2023-01-15'),
(2, 'Jane', 'Smith', 'jane.smith@email.com', '555-0102', 'Los Angeles', 'CA', 'USA', '2023-02-20'),
(3, 'Bob', 'Johnson', 'bob.j@email.com', '555-0103', 'Chicago', 'IL', 'USA', '2023-03-10'),
(4, 'Alice', 'Williams', 'alice.w@email.com', '555-0104', 'Houston', 'TX', 'USA', '2023-04-05'),
(5, 'Charlie', 'Brown', 'charlie.b@email.com', '555-0105', 'Phoenix', 'AZ', 'USA', '2023-05-12'),
(6, 'Diana', 'Davis', 'diana.d@email.com', '555-0106', 'Philadelphia', 'PA', 'USA', '2023-06-18'),
(7, 'Eve', 'Miller', 'eve.m@email.com', '555-0107', 'San Antonio', 'TX', 'USA', '2023-07-22'),
(8, 'Frank', 'Wilson', 'frank.w@email.com', '555-0108', 'San Diego', 'CA', 'USA', '2023-08-30'),
(9, 'Grace', 'Moore', 'grace.m@email.com', '555-0109', 'Dallas', 'TX', 'USA', '2023-09-14'),
(10, 'Henry', 'Taylor', 'henry.t@email.com', '555-0110', 'San Jose', 'CA', 'USA', '2023-10-25');

-- Create orders table
CREATE OR REPLACE TABLE orders (
    order_id INT,
    customer_id INT,
    order_date DATE,
    order_amount DECIMAL(10,2),
    status STRING,
    payment_method STRING
) USING DELTA;

-- Insert sample orders
INSERT INTO orders VALUES
(101, 1, '2024-01-10', 150.00, 'completed', 'credit_card'),
(102, 2, '2024-01-11', 200.50, 'completed', 'paypal'),
(103, 1, '2024-01-12', 75.25, 'completed', 'credit_card'),
(104, 3, '2024-01-13', 300.00, 'pending', 'bank_transfer'),
(105, 4, '2024-01-14', 125.75, 'completed', 'credit_card'),
(106, 2, '2024-01-15', 450.00, 'completed', 'credit_card'),
(107, 5, '2024-01-16', 89.99, 'cancelled', 'paypal'),
(108, 6, '2024-01-17', 275.50, 'completed', 'credit_card'),
(109, 3, '2024-01-18', 199.99, 'completed', 'paypal'),
(110, 7, '2024-01-19', 350.00, 'pending', 'bank_transfer'),
(111, 8, '2024-01-20', 425.25, 'completed', 'credit_card'),
(112, 1, '2024-01-21', 95.00, 'completed', 'paypal'),
(113, 9, '2024-01-22', 180.75, 'completed', 'credit_card'),
(114, 10, '2024-01-23', 520.00, 'completed', 'credit_card'),
(115, 4, '2024-01-24', 145.50, 'cancelled', 'paypal');

-- Create products table
CREATE OR REPLACE TABLE products (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10,2),
    stock_quantity INT,
    supplier STRING
) USING DELTA;

-- Insert sample products
INSERT INTO products VALUES
(1001, 'Laptop Pro', 'Electronics', 1299.99, 50, 'TechCorp'),
(1002, 'Wireless Mouse', 'Electronics', 29.99, 200, 'TechCorp'),
(1003, 'Office Chair', 'Furniture', 249.99, 75, 'FurnitureCo'),
(1004, 'Desk Lamp', 'Furniture', 45.99, 150, 'FurnitureCo'),
(1005, 'Notebook Set', 'Stationery', 12.99, 500, 'PaperPlus'),
(1006, 'Pen Pack', 'Stationery', 8.99, 1000, 'PaperPlus'),
(1007, 'Monitor 27"', 'Electronics', 399.99, 80, 'TechCorp'),
(1008, 'Keyboard Mechanical', 'Electronics', 149.99, 120, 'TechCorp'),
(1009, 'Standing Desk', 'Furniture', 599.99, 30, 'FurnitureCo'),
(1010, 'Webcam HD', 'Electronics', 89.99, 100, 'TechCorp');

-- Create order_items table
CREATE OR REPLACE TABLE order_items (
    order_item_id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2)
) USING DELTA;

-- Insert sample order items
INSERT INTO order_items VALUES
(1, 101, 1002, 2, 29.99),
(2, 101, 1005, 3, 12.99),
(3, 102, 1003, 1, 249.99),
(4, 103, 1006, 5, 8.99),
(5, 104, 1007, 1, 399.99),
(6, 105, 1004, 2, 45.99),
(7, 106, 1001, 1, 1299.99),
(8, 107, 1008, 1, 149.99),
(9, 108, 1009, 1, 599.99),
(10, 109, 1002, 3, 29.99),
(11, 110, 1007, 1, 399.99),
(12, 111, 1001, 1, 1299.99),
(13, 112, 1005, 5, 12.99),
(14, 113, 1008, 1, 149.99),
(15, 114, 1001, 1, 1299.99);

-- Verify data
SELECT 'customers' as table_name, COUNT(*) as row_count FROM customers
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'order_items', COUNT(*) FROM order_items;
```

### Step 4: Create Silver Layer Tables

```sql
-- Switch to silver schema
USE SCHEMA uc_day25_<your_username>.silver;

-- Create cleaned customers table
CREATE OR REPLACE TABLE customers_clean AS
SELECT 
    customer_id,
    CONCAT(first_name, ' ', last_name) as full_name,
    LOWER(email) as email,
    phone,
    city,
    state,
    country,
    created_date
FROM uc_day25_<your_username>.bronze.customers;

-- Create validated orders table
CREATE OR REPLACE TABLE orders_valid AS
SELECT 
    order_id,
    customer_id,
    order_date,
    order_amount,
    status,
    payment_method
FROM uc_day25_<your_username>.bronze.orders
WHERE order_amount > 0
    AND status IN ('completed', 'pending', 'cancelled');

-- Verify silver tables
SHOW TABLES;
```

### Step 5: Create Gold Layer Tables

```sql
-- Switch to gold schema
USE SCHEMA uc_day25_<your_username>.gold;

-- Create customer summary
CREATE OR REPLACE TABLE customer_summary AS
SELECT 
    c.customer_id,
    c.full_name,
    c.email,
    c.state,
    COUNT(o.order_id) as total_orders,
    SUM(o.order_amount) as total_spent,
    AVG(o.order_amount) as avg_order_value,
    MAX(o.order_date) as last_order_date
FROM uc_day25_<your_username>.silver.customers_clean c
LEFT JOIN uc_day25_<your_username>.silver.orders_valid o
    ON c.customer_id = o.customer_id
WHERE o.status = 'completed'
GROUP BY c.customer_id, c.full_name, c.email, c.state;

-- Create daily sales summary
CREATE OR REPLACE TABLE daily_sales AS
SELECT 
    order_date,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(order_id) as total_orders,
    SUM(order_amount) as total_revenue,
    AVG(order_amount) as avg_order_value
FROM uc_day25_<your_username>.silver.orders_valid
WHERE status = 'completed'
GROUP BY order_date
ORDER BY order_date;

-- Verify gold tables
SHOW TABLES;
```

### Step 6: Add Table Comments and Properties

```sql
-- Add comments to tables
COMMENT ON TABLE uc_day25_<your_username>.bronze.customers IS 
'Raw customer data from CRM system';

COMMENT ON TABLE uc_day25_<your_username>.silver.customers_clean IS 
'Cleaned and standardized customer data';

COMMENT ON TABLE uc_day25_<your_username>.gold.customer_summary IS 
'Customer-level aggregates for analytics';

-- Add table properties
ALTER TABLE uc_day25_<your_username>.gold.customer_summary
SET TBLPROPERTIES (
    'data_classification' = 'internal',
    'contains_pii' = 'true',
    'owner' = 'data_engineering',
    'refresh_frequency' = 'daily'
);
```

## Verification

Run these queries to verify your setup:

```sql
-- 1. Check catalog exists
SHOW CATALOGS LIKE 'uc_day25_%';

-- 2. Check all schemas
SHOW SCHEMAS IN uc_day25_<your_username>;

-- 3. Check tables in each schema
SHOW TABLES IN uc_day25_<your_username>.bronze;
SHOW TABLES IN uc_day25_<your_username>.silver;
SHOW TABLES IN uc_day25_<your_username>.gold;

-- 4. Verify data counts
SELECT 
    'bronze.customers' as table_name,
    COUNT(*) as row_count 
FROM uc_day25_<your_username>.bronze.customers
UNION ALL
SELECT 'silver.customers_clean', COUNT(*) 
FROM uc_day25_<your_username>.silver.customers_clean
UNION ALL
SELECT 'gold.customer_summary', COUNT(*) 
FROM uc_day25_<your_username>.gold.customer_summary;

-- 5. Test three-level namespace
SELECT * FROM uc_day25_<your_username>.gold.customer_summary LIMIT 5;
```

Expected output:
- ✅ 1 catalog created
- ✅ 3 schemas (bronze, silver, gold)
- ✅ 4 bronze tables with data
- ✅ 2 silver tables with data
- ✅ 2 gold tables with data

## Troubleshooting

### Issue: "Unity Catalog is not enabled"

**Solution**: Contact your Databricks administrator to enable Unity Catalog for your workspace.

### Issue: "Permission denied to create catalog"

**Solution**: Use a shared catalog provided by your instructor:
```sql
USE CATALOG shared_training;
CREATE SCHEMA day25_<your_username>;
```

### Issue: "Metastore not found"

**Solution**: Your workspace needs a metastore attached. Contact your admin.

### Issue: "Cannot use three-level namespace"

**Solution**: Ensure Unity Catalog is enabled. Legacy workspaces use two-level namespace (schema.table).

## Alternative Setup (Without Admin Access)

If you don't have permissions to create catalogs:

```sql
-- Use existing catalog
USE CATALOG shared_training;

-- Create your own schema
CREATE SCHEMA IF NOT EXISTS day25_<your_username>;
USE SCHEMA day25_<your_username>;

-- Create all tables in this single schema
CREATE TABLE customers (...);
CREATE TABLE orders (...);
-- etc.

-- Use two-level namespace
SELECT * FROM shared_training.day25_<your_username>.customers;
```

## Clean Up (Optional)

To remove all objects created today:

```sql
-- Drop catalog (removes all schemas and tables)
DROP CATALOG IF EXISTS uc_day25_<your_username> CASCADE;

-- Or drop individual schemas
DROP SCHEMA IF EXISTS uc_day25_<your_username>.bronze CASCADE;
DROP SCHEMA IF EXISTS uc_day25_<your_username>.silver CASCADE;
DROP SCHEMA IF EXISTS uc_day25_<your_username>.gold CASCADE;
```

## Ready to Start!

Once you've completed the setup:
1. ✅ Catalog and schemas created
2. ✅ Sample data loaded
3. ✅ Silver and gold tables created
4. ✅ Verification queries successful

You're ready to begin the exercises in `exercise.sql`!

---

**Setup Time**: 10-15 minutes  
**Total Tables Created**: 8 (4 bronze, 2 silver, 2 gold)  
**Total Rows**: ~50 rows across all tables
