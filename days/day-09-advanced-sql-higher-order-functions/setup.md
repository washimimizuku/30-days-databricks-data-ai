# Day 9 Setup: Advanced SQL - Higher-Order Functions

## Prerequisites

- Completed Days 1-8
- Active Databricks cluster
- Understanding of arrays and basic SQL
- Familiarity with window functions (Day 8)

## Setup Steps

### Step 1: Create a New Notebook

1. Navigate to your Workspace
2. Go to your `30-days-databricks` folder
3. Create a new notebook: `day-09-higher-order-functions`
4. Set default language to **SQL**

### Step 2: Create Database

```sql
-- Create database for Day 9 exercises
CREATE DATABASE IF NOT EXISTS day9_higher_order_functions;
USE day9_higher_order_functions;
```

### Step 3: Create Sample Tables

We'll create tables with array, struct, and map columns for practicing higher-order functions.

**Customer Orders Table** (with arrays):
```sql
CREATE TABLE IF NOT EXISTS customer_orders (
    customer_id INT,
    customer_name STRING,
    order_ids ARRAY<INT>,
    order_amounts ARRAY<DECIMAL(10,2)>,
    order_dates ARRAY<DATE>,
    order_statuses ARRAY<STRING>
) USING DELTA;

INSERT INTO customer_orders VALUES
    (101, 'TechCorp', array(1001, 1002, 1003), array(150.00, 200.50, 175.25), array('2024-01-05', '2024-01-15', '2024-02-01'), array('Completed', 'Completed', 'Processing')),
    (102, 'DataSys', array(1004, 1005), array(300.00, 450.75), array('2024-01-10', '2024-01-25'), array('Completed', 'Completed')),
    (103, 'CloudCo', array(1006, 1007, 1008, 1009), array(125.00, 275.50, 180.00, 220.25), array('2024-01-12', '2024-01-20', '2024-02-05', '2024-02-15'), array('Completed', 'Completed', 'Processing', 'Pending')),
    (104, 'FinHub', array(1010), array(500.00), array('2024-01-18'), array('Completed')),
    (105, 'RetailX', array(1011, 1012, 1013), array(95.50, 310.00, 145.75), array('2024-01-22', '2024-02-08', '2024-02-20'), array('Completed', 'Processing', 'Pending')),
    (106, 'HealthT', array(1014, 1015), array(425.00, 380.50), array('2024-01-28', '2024-02-12'), array('Completed', 'Processing')),
    (107, 'EduPlatform', array(1016, 1017, 1018), array(160.00, 240.00, 190.50), array('2024-02-03', '2024-02-18', '2024-03-01'), array('Completed', 'Processing', 'Pending')),
    (108, 'MediaStream', array(1019), array(550.00), array('2024-02-10'), array('Processing')),
    (109, 'AutoDrive', array(1020, 1021), array(380.00, 420.50), array('2024-02-15', '2024-03-05'), array('Completed', 'Pending')),
    (110, 'EnergyPlus', array(1022, 1023, 1024), array(290.00, 315.75, 270.50), array('2024-02-22', '2024-03-08', '2024-03-15'), array('Processing', 'Pending', 'Pending'));
```

**Products Table** (with arrays and structs):
```sql
CREATE TABLE IF NOT EXISTS products (
    product_id INT,
    product_name STRING,
    category STRING,
    prices ARRAY<DECIMAL(10,2)>,
    tags ARRAY<STRING>,
    reviews ARRAY<STRUCT<rating: INT, comment: STRING, verified: BOOLEAN>>,
    attributes MAP<STRING, STRING>
) USING DELTA;

INSERT INTO products VALUES
    (201, 'Laptop Pro', 'Electronics', array(1200.00, 1150.00, 1180.00), array('laptop', 'computer', 'portable'), 
     array(struct(5, 'Excellent!', true), struct(4, 'Good value', true), struct(5, 'Love it', false)),
     map('brand', 'TechBrand', 'warranty', '2 years', 'color', 'silver')),
    (202, 'Wireless Mouse', 'Electronics', array(25.00, 22.50, 24.00), array('mouse', 'wireless', 'accessory'),
     array(struct(4, 'Works well', true), struct(3, 'OK', true)),
     map('brand', 'MouseCo', 'warranty', '1 year', 'color', 'black')),
    (203, 'USB-C Cable', 'Accessories', array(15.00, 14.50, 15.50), array('cable', 'usb-c', 'charging'),
     array(struct(5, 'Perfect', true), struct(5, 'Great quality', true), struct(4, 'Good', true)),
     map('brand', 'CablePro', 'length', '2m', 'color', 'white')),
    (204, 'Monitor 27"', 'Electronics', array(350.00, 340.00, 360.00), array('monitor', 'display', '4k'),
     array(struct(5, 'Amazing display', true), struct(4, 'Good', false)),
     map('brand', 'ViewTech', 'warranty', '3 years', 'resolution', '4K')),
    (205, 'Mechanical Keyboard', 'Electronics', array(120.00, 115.00, 125.00), array('keyboard', 'mechanical', 'gaming'),
     array(struct(5, 'Best keyboard', true), struct(5, 'Love the feel', true), struct(4, 'Great', true)),
     map('brand', 'KeyMaster', 'warranty', '2 years', 'switches', 'cherry-mx'));
```

**User Events Table** (with nested arrays):
```sql
CREATE TABLE IF NOT EXISTS user_events (
    user_id INT,
    username STRING,
    events ARRAY<STRUCT<event_type: STRING, timestamp: TIMESTAMP, amount: DECIMAL(10,2)>>,
    daily_events ARRAY<ARRAY<STRING>>,
    preferences MAP<STRING, STRING>
) USING DELTA;

INSERT INTO user_events VALUES
    (1001, 'alice', 
     array(
         struct('login', cast('2024-01-05 09:00:00' as timestamp), 0.00),
         struct('view', cast('2024-01-05 09:15:00' as timestamp), 0.00),
         struct('purchase', cast('2024-01-05 10:30:00' as timestamp), 150.00)
     ),
     array(array('login', 'view', 'purchase'), array('login', 'view')),
     map('theme', 'dark', 'language', 'en', 'notifications', 'enabled')),
    (1002, 'bob',
     array(
         struct('login', cast('2024-01-06 08:30:00' as timestamp), 0.00),
         struct('view', cast('2024-01-06 08:45:00' as timestamp), 0.00),
         struct('view', cast('2024-01-06 09:00:00' as timestamp), 0.00),
         struct('purchase', cast('2024-01-06 10:00:00' as timestamp), 250.00)
     ),
     array(array('login', 'view', 'view', 'purchase')),
     map('theme', 'light', 'language', 'en', 'notifications', 'disabled')),
    (1003, 'carol',
     array(
         struct('login', cast('2024-01-07 10:00:00' as timestamp), 0.00),
         struct('view', cast('2024-01-07 10:30:00' as timestamp), 0.00)
     ),
     array(array('login', 'view'), array('login')),
     map('theme', 'dark', 'language', 'es', 'notifications', 'enabled'));
```

**Sales Data Table** (with arrays for time-series):
```sql
CREATE TABLE IF NOT EXISTS sales_data (
    product_id INT,
    product_name STRING,
    daily_sales ARRAY<INT>,
    daily_revenue ARRAY<DECIMAL(10,2)>,
    regions ARRAY<STRING>
) USING DELTA;

INSERT INTO sales_data VALUES
    (301, 'Widget A', array(10, 15, 12, 18, 20, 16, 14), array(100.00, 150.00, 120.00, 180.00, 200.00, 160.00, 140.00), array('North', 'South', 'East')),
    (302, 'Widget B', array(8, 12, 10, 15, 18, 14, 12), array(160.00, 240.00, 200.00, 300.00, 360.00, 280.00, 240.00), array('North', 'West')),
    (303, 'Widget C', array(20, 25, 22, 28, 30, 26, 24), array(200.00, 250.00, 220.00, 280.00, 300.00, 260.00, 240.00), array('South', 'East', 'West')),
    (304, 'Widget D', array(5, 8, 6, 10, 12, 9, 7), array(250.00, 400.00, 300.00, 500.00, 600.00, 450.00, 350.00), array('North', 'South', 'East', 'West')),
    (305, 'Widget E', array(15, 18, 16, 20, 22, 19, 17), array(150.00, 180.00, 160.00, 200.00, 220.00, 190.00, 170.00), array('East', 'West'));
```

**Log Entries Table** (with complex nested data):
```sql
CREATE TABLE IF NOT EXISTS log_entries (
    log_id INT,
    application STRING,
    entries ARRAY<STRUCT<level: STRING, message: STRING, timestamp: TIMESTAMP, metadata: MAP<STRING, STRING>>>
) USING DELTA;

INSERT INTO log_entries VALUES
    (1, 'WebApp', array(
        struct('INFO', 'Application started', cast('2024-01-05 08:00:00' as timestamp), map('version', '1.0', 'env', 'prod')),
        struct('ERROR', 'Database connection failed', cast('2024-01-05 08:05:00' as timestamp), map('db', 'main', 'retry', '3')),
        struct('INFO', 'Connection restored', cast('2024-01-05 08:06:00' as timestamp), map('db', 'main'))
    )),
    (2, 'API', array(
        struct('INFO', 'Server started', cast('2024-01-05 09:00:00' as timestamp), map('port', '8080', 'env', 'prod')),
        struct('WARN', 'High memory usage', cast('2024-01-05 09:30:00' as timestamp), map('memory', '85%')),
        struct('ERROR', 'Request timeout', cast('2024-01-05 10:00:00' as timestamp), map('endpoint', '/api/data', 'timeout', '30s'))
    ));
```

### Step 4: Verify Setup

```sql
-- Check all tables exist
SHOW TABLES IN day9_higher_order_functions;

-- Verify row counts
SELECT 'customer_orders' AS table_name, COUNT(*) AS row_count FROM customer_orders
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'user_events', COUNT(*) FROM user_events
UNION ALL
SELECT 'sales_data', COUNT(*) FROM sales_data
UNION ALL
SELECT 'log_entries', COUNT(*) FROM log_entries;

-- Expected output:
-- customer_orders: 10
-- products: 5
-- user_events: 3
-- sales_data: 5
-- log_entries: 2
```

### Step 5: Test Higher-Order Functions

```sql
-- Test TRANSFORM
SELECT 
    customer_name,
    order_amounts,
    TRANSFORM(order_amounts, amt -> amt * 1.1) as amounts_with_tax
FROM customer_orders
LIMIT 3;

-- Test FILTER
SELECT 
    customer_name,
    order_amounts,
    FILTER(order_amounts, amt -> amt > 200) as large_orders
FROM customer_orders
LIMIT 3;

-- Test AGGREGATE
SELECT 
    customer_name,
    AGGREGATE(order_amounts, 0.0, (acc, amt) -> acc + amt) as total_spent
FROM customer_orders
LIMIT 3;
```

## Data Model

```
customer_orders
├── customer_id (INT)
├── order_ids (ARRAY<INT>)
├── order_amounts (ARRAY<DECIMAL>)
└── order_statuses (ARRAY<STRING>)

products
├── product_id (INT)
├── prices (ARRAY<DECIMAL>)
├── tags (ARRAY<STRING>)
├── reviews (ARRAY<STRUCT>)
└── attributes (MAP<STRING, STRING>)

user_events
├── user_id (INT)
├── events (ARRAY<STRUCT>)
├── daily_events (ARRAY<ARRAY<STRING>>)
└── preferences (MAP<STRING, STRING>)

sales_data
├── product_id (INT)
├── daily_sales (ARRAY<INT>)
└── daily_revenue (ARRAY<DECIMAL>)

log_entries
├── log_id (INT)
└── entries (ARRAY<STRUCT>)
```

## Verification Checklist

- [ ] Database `day9_higher_order_functions` created
- [ ] All 5 tables created successfully
- [ ] All data inserted correctly
- [ ] Row counts match expected values
- [ ] Can query array columns
- [ ] Higher-order functions work

## Troubleshooting

### Issue: "Function not found"
**Solution**: Ensure you're using Databricks Runtime 7.0 or higher. Higher-order functions are available in DBR 7.0+.

### Issue: "Cannot resolve 'x'"
**Solution**: Check lambda syntax. Use `x -> expression` not `x => expression`.

### Issue: "Type mismatch"
**Solution**: Ensure array element types match the function. Use CAST if needed:
```sql
TRANSFORM(array('1', '2', '3'), x -> CAST(x AS INT))
```

### Issue: "EXPLODE returns no rows"
**Solution**: Check if array is NULL or empty. Use EXPLODE_OUTER to keep NULL arrays:
```sql
SELECT customer_id, EXPLODE_OUTER(order_ids) as order_id FROM customers;
```

## Quick Reference

**Higher-Order Functions**:
```sql
TRANSFORM(array, x -> expression)
FILTER(array, x -> condition)
AGGREGATE(array, init, (acc, x) -> expression)
EXISTS(array, x -> condition)
FORALL(array, x -> condition)
```

**Array Functions**:
```sql
array_contains(array, value)
array_distinct(array)
array_union(array1, array2)
array_intersect(array1, array2)
array_except(array1, array2)
size(array)
array_sort(array)
array_min(array)
array_max(array)
```

**Explode Functions**:
```sql
EXPLODE(array)
EXPLODE_OUTER(array)
POSEXPLODE(array)
FLATTEN(nested_array)
```

## What's Next?

You're ready for Day 9 exercises! Open `exercise.sql` and start practicing higher-order functions.

**Learning Path**:
1. Start with TRANSFORM (simplest)
2. Practice FILTER for conditional logic
3. Master AGGREGATE for reductions
4. Use EXISTS and FORALL for validation
5. Work with EXPLODE and FLATTEN
6. Combine multiple functions

Good luck! 🚀

