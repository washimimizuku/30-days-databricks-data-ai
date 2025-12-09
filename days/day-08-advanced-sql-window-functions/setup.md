# Day 8 Setup: Advanced SQL - Window Functions

## Prerequisites

- Completed Week 1 (Days 1-7)
- Active Databricks cluster
- Understanding of basic SQL (SELECT, JOIN, GROUP BY)
- Familiarity with aggregation functions

## Setup Steps

### Step 1: Create a New Notebook

1. Navigate to your Workspace
2. Go to your `30-days-databricks` folder
3. Create a new notebook: `day-08-window-functions`
4. Set default language to **SQL**

### Step 2: Create Database

```sql
-- Create database for Day 8 exercises
CREATE DATABASE IF NOT EXISTS day8_window_functions;
USE day8_window_functions;
```

### Step 3: Create Sample Tables

We'll create realistic datasets for practicing window functions.

**Sales Table** (for ranking and running totals):
```sql
CREATE TABLE IF NOT EXISTS sales (
    sale_id INT,
    product_id INT,
    product_name STRING,
    category STRING,
    sale_date DATE,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(12,2),
    region STRING,
    sales_rep STRING
) USING DELTA;

INSERT INTO sales VALUES
    (1, 101, 'Laptop Pro', 'Electronics', '2024-01-05', 2, 1200.00, 2400.00, 'North', 'Alice'),
    (2, 102, 'Mouse', 'Electronics', '2024-01-05', 10, 25.00, 250.00, 'North', 'Alice'),
    (3, 103, 'Keyboard', 'Electronics', '2024-01-06', 5, 75.00, 375.00, 'South', 'Bob'),
    (4, 101, 'Laptop Pro', 'Electronics', '2024-01-07', 3, 1200.00, 3600.00, 'East', 'Carol'),
    (5, 104, 'Monitor', 'Electronics', '2024-01-08', 4, 300.00, 1200.00, 'West', 'David'),
    (6, 105, 'Desk Chair', 'Furniture', '2024-01-08', 2, 250.00, 500.00, 'North', 'Alice'),
    (7, 106, 'Standing Desk', 'Furniture', '2024-01-09', 1, 600.00, 600.00, 'South', 'Bob'),
    (8, 102, 'Mouse', 'Electronics', '2024-01-10', 15, 25.00, 375.00, 'East', 'Carol'),
    (9, 107, 'Office Lamp', 'Furniture', '2024-01-11', 8, 45.00, 360.00, 'West', 'David'),
    (10, 101, 'Laptop Pro', 'Electronics', '2024-01-12', 5, 1200.00, 6000.00, 'North', 'Alice'),
    (11, 103, 'Keyboard', 'Electronics', '2024-01-13', 7, 75.00, 525.00, 'South', 'Bob'),
    (12, 108, 'Bookshelf', 'Furniture', '2024-01-14', 3, 150.00, 450.00, 'East', 'Carol'),
    (13, 104, 'Monitor', 'Electronics', '2024-01-15', 6, 300.00, 1800.00, 'West', 'David'),
    (14, 105, 'Desk Chair', 'Furniture', '2024-01-16', 4, 250.00, 1000.00, 'North', 'Alice'),
    (15, 109, 'Webcam', 'Electronics', '2024-01-17', 12, 80.00, 960.00, 'South', 'Bob'),
    (16, 106, 'Standing Desk', 'Furniture', '2024-01-18', 2, 600.00, 1200.00, 'East', 'Carol'),
    (17, 110, 'Headphones', 'Electronics', '2024-01-19', 20, 150.00, 3000.00, 'West', 'David'),
    (18, 107, 'Office Lamp', 'Furniture', '2024-01-20', 10, 45.00, 450.00, 'North', 'Alice'),
    (19, 102, 'Mouse', 'Electronics', '2024-01-21', 18, 25.00, 450.00, 'South', 'Bob'),
    (20, 101, 'Laptop Pro', 'Electronics', '2024-01-22', 4, 1200.00, 4800.00, 'East', 'Carol');
```

**Employees Table** (for ranking by salary):
```sql
CREATE TABLE IF NOT EXISTS employees (
    employee_id INT,
    employee_name STRING,
    department STRING,
    hire_date DATE,
    salary DECIMAL(10,2),
    performance_score INT
) USING DELTA;

INSERT INTO employees VALUES
    (1, 'Alice Johnson', 'Engineering', '2020-01-15', 95000.00, 92),
    (2, 'Bob Smith', 'Engineering', '2019-03-20', 105000.00, 88),
    (3, 'Carol Williams', 'Engineering', '2021-06-10', 85000.00, 95),
    (4, 'David Brown', 'Sales', '2018-09-05', 75000.00, 85),
    (5, 'Emma Davis', 'Sales', '2022-01-12', 65000.00, 90),
    (6, 'Frank Miller', 'Sales', '2020-07-22', 80000.00, 87),
    (7, 'Grace Wilson', 'Marketing', '2021-11-18', 70000.00, 91),
    (8, 'Henry Moore', 'Marketing', '2019-05-30', 78000.00, 89),
    (9, 'Iris Taylor', 'Marketing', '2022-03-15', 68000.00, 93),
    (10, 'Jack Anderson', 'Engineering', '2020-10-08', 92000.00, 86),
    (11, 'Kate Thomas', 'Sales', '2021-02-25', 72000.00, 88),
    (12, 'Leo Jackson', 'Engineering', '2019-08-14', 110000.00, 94),
    (13, 'Mia White', 'Marketing', '2022-05-20', 66000.00, 90),
    (14, 'Noah Harris', 'Sales', '2020-12-03', 77000.00, 92),
    (15, 'Olivia Martin', 'Engineering', '2021-09-17', 88000.00, 89);
```

**Stock Prices Table** (for time-series analysis):
```sql
CREATE TABLE IF NOT EXISTS stock_prices (
    ticker STRING,
    trade_date DATE,
    open_price DECIMAL(10,2),
    close_price DECIMAL(10,2),
    high_price DECIMAL(10,2),
    low_price DECIMAL(10,2),
    volume BIGINT
) USING DELTA;

INSERT INTO stock_prices VALUES
    ('AAPL', '2024-01-02', 185.50, 187.20, 188.00, 185.00, 50000000),
    ('AAPL', '2024-01-03', 187.00, 186.50, 188.50, 186.00, 48000000),
    ('AAPL', '2024-01-04', 186.80, 189.30, 190.00, 186.50, 52000000),
    ('AAPL', '2024-01-05', 189.50, 188.00, 190.50, 187.50, 51000000),
    ('AAPL', '2024-01-08', 188.20, 191.50, 192.00, 188.00, 55000000),
    ('AAPL', '2024-01-09', 191.00, 190.20, 192.50, 189.50, 53000000),
    ('AAPL', '2024-01-10', 190.50, 193.00, 194.00, 190.00, 58000000),
    ('GOOGL', '2024-01-02', 140.20, 141.50, 142.00, 140.00, 30000000),
    ('GOOGL', '2024-01-03', 141.30, 140.80, 142.50, 140.50, 29000000),
    ('GOOGL', '2024-01-04', 141.00, 143.20, 144.00, 140.80, 32000000),
    ('GOOGL', '2024-01-05', 143.50, 142.50, 144.50, 142.00, 31000000),
    ('GOOGL', '2024-01-08', 142.80, 145.00, 146.00, 142.50, 35000000),
    ('GOOGL', '2024-01-09', 144.50, 144.20, 146.50, 143.50, 33000000),
    ('GOOGL', '2024-01-10', 144.00, 146.50, 147.50, 143.80, 36000000),
    ('MSFT', '2024-01-02', 375.00, 378.50, 380.00, 374.50, 25000000),
    ('MSFT', '2024-01-03', 378.00, 377.20, 380.50, 376.50, 24000000),
    ('MSFT', '2024-01-04', 377.50, 381.00, 382.50, 377.00, 27000000),
    ('MSFT', '2024-01-05', 381.20, 379.50, 383.00, 379.00, 26000000),
    ('MSFT', '2024-01-08', 379.80, 384.00, 385.50, 379.50, 29000000),
    ('MSFT', '2024-01-09', 383.50, 383.00, 386.00, 382.50, 28000000),
    ('MSFT', '2024-01-10', 383.20, 386.50, 388.00, 383.00, 31000000);
```

**Customer Orders Table** (for cohort analysis):
```sql
CREATE TABLE IF NOT EXISTS customer_orders (
    order_id INT,
    customer_id INT,
    customer_name STRING,
    order_date DATE,
    order_amount DECIMAL(10,2),
    order_status STRING
) USING DELTA;

INSERT INTO customer_orders VALUES
    (1, 101, 'TechCorp', '2024-01-05', 15000.00, 'Completed'),
    (2, 102, 'DataSys', '2024-01-10', 22000.00, 'Completed'),
    (3, 101, 'TechCorp', '2024-01-15', 18500.00, 'Completed'),
    (4, 103, 'CloudCo', '2024-01-20', 31000.00, 'Completed'),
    (5, 102, 'DataSys', '2024-01-25', 12500.00, 'Completed'),
    (6, 104, 'FinHub', '2024-02-01', 28000.00, 'Completed'),
    (7, 101, 'TechCorp', '2024-02-05', 19500.00, 'Completed'),
    (8, 105, 'RetailX', '2024-02-10', 24000.00, 'Completed'),
    (9, 103, 'CloudCo', '2024-02-15', 16500.00, 'Completed'),
    (10, 102, 'DataSys', '2024-02-20', 33000.00, 'Processing'),
    (11, 101, 'TechCorp', '2024-02-25', 21000.00, 'Processing'),
    (12, 104, 'FinHub', '2024-03-01', 27500.00, 'Pending'),
    (13, 106, 'HealthT', '2024-03-05', 35000.00, 'Pending'),
    (14, 105, 'RetailX', '2024-03-10', 29000.00, 'Pending'),
    (15, 103, 'CloudCo', '2024-03-15', 22500.00, 'Pending');
```

**Monthly Revenue Table** (for trend analysis):
```sql
CREATE TABLE IF NOT EXISTS monthly_revenue (
    month_date DATE,
    region STRING,
    revenue DECIMAL(12,2),
    expenses DECIMAL(12,2)
) USING DELTA;

INSERT INTO monthly_revenue VALUES
    ('2023-01-01', 'North', 150000.00, 80000.00),
    ('2023-02-01', 'North', 165000.00, 85000.00),
    ('2023-03-01', 'North', 180000.00, 90000.00),
    ('2023-04-01', 'North', 175000.00, 88000.00),
    ('2023-05-01', 'North', 195000.00, 95000.00),
    ('2023-06-01', 'North', 210000.00, 100000.00),
    ('2023-01-01', 'South', 120000.00, 70000.00),
    ('2023-02-01', 'South', 135000.00, 75000.00),
    ('2023-03-01', 'South', 145000.00, 78000.00),
    ('2023-04-01', 'South', 140000.00, 76000.00),
    ('2023-05-01', 'South', 160000.00, 82000.00),
    ('2023-06-01', 'South', 175000.00, 88000.00),
    ('2023-01-01', 'East', 180000.00, 95000.00),
    ('2023-02-01', 'East', 190000.00, 98000.00),
    ('2023-03-01', 'East', 205000.00, 102000.00),
    ('2023-04-01', 'East', 200000.00, 100000.00),
    ('2023-05-01', 'East', 220000.00, 108000.00),
    ('2023-06-01', 'East', 240000.00, 115000.00);
```

### Step 4: Verify Setup

```sql
-- Check all tables exist
SHOW TABLES IN day8_window_functions;

-- Verify row counts
SELECT 'sales' AS table_name, COUNT(*) AS row_count FROM sales
UNION ALL
SELECT 'employees', COUNT(*) FROM employees
UNION ALL
SELECT 'stock_prices', COUNT(*) FROM stock_prices
UNION ALL
SELECT 'customer_orders', COUNT(*) FROM customer_orders
UNION ALL
SELECT 'monthly_revenue', COUNT(*) FROM monthly_revenue;

-- Expected output:
-- sales: 20
-- employees: 15
-- stock_prices: 21
-- customer_orders: 15
-- monthly_revenue: 18
```

### Step 5: Test Window Functions

```sql
-- Test basic window function
SELECT 
    product_name,
    category,
    total_amount,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY total_amount DESC) as rank_in_category
FROM sales
LIMIT 10;

-- Test running total
SELECT 
    sale_date,
    total_amount,
    SUM(total_amount) OVER (ORDER BY sale_date) as running_total
FROM sales
ORDER BY sale_date
LIMIT 10;
```

## Data Model

```
sales
├── sale_id (PK)
├── product_id
├── category
├── sale_date
└── total_amount

employees
├── employee_id (PK)
├── department
├── salary
└── performance_score

stock_prices
├── ticker
├── trade_date
└── close_price

customer_orders
├── order_id (PK)
├── customer_id
├── order_date
└── order_amount

monthly_revenue
├── month_date
├── region
└── revenue
```

## Verification Checklist

- [ ] Database `day8_window_functions` created
- [ ] All 5 tables created successfully
- [ ] All data inserted correctly
- [ ] Row counts match expected values
- [ ] Can query each table individually
- [ ] Basic window functions work

## Troubleshooting

### Issue: "Window function not recognized"
**Solution**: Ensure you're using Databricks Runtime 7.0 or higher. Window functions are fully supported in all modern versions.

### Issue: "ORDER BY required"
**Solution**: Some window functions (like ROW_NUMBER, RANK, LAG, LEAD) require ORDER BY in the OVER clause.

### Issue: "LAST_VALUE returns unexpected results"
**Solution**: LAST_VALUE requires explicit frame specification:
```sql
LAST_VALUE(column) OVER (
    ORDER BY column
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
```

### Issue: "Performance is slow"
**Solution**: 
- Add PARTITION BY to limit window size
- Ensure ORDER BY columns are indexed
- Consider materializing results for complex calculations

## Quick Reference

**Ranking Functions**:
```sql
ROW_NUMBER() OVER (ORDER BY column)
RANK() OVER (ORDER BY column)
DENSE_RANK() OVER (ORDER BY column)
NTILE(n) OVER (ORDER BY column)
```

**Analytical Functions**:
```sql
LAG(column, offset, default) OVER (ORDER BY column)
LEAD(column, offset, default) OVER (ORDER BY column)
FIRST_VALUE(column) OVER (PARTITION BY col ORDER BY col)
LAST_VALUE(column) OVER (PARTITION BY col ORDER BY col ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
```

**Aggregate Functions**:
```sql
SUM(column) OVER (ORDER BY column)
AVG(column) OVER (PARTITION BY col ORDER BY col)
COUNT(*) OVER (PARTITION BY col)
```

## What's Next?

You're ready for Day 8 exercises! Open `exercise.sql` and start practicing window functions.

**Learning Path**:
1. Start with ranking functions (ROW_NUMBER, RANK, DENSE_RANK)
2. Practice LAG and LEAD for time-series analysis
3. Master running totals and moving averages
4. Work on window frames (ROWS BETWEEN)
5. Apply to real-world analytics problems

Good luck! 🚀

