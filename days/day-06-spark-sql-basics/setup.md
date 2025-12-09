# Day 6 Setup: Spark SQL Basics

## Prerequisites

- Completed Days 1-5
- Active Databricks cluster (any size)
- Workspace access with notebook creation permissions
- Understanding of databases and tables from Day 5

## Setup Steps

### Step 1: Create a New Notebook

1. Navigate to your Workspace
2. Go to your `30-days-databricks` folder
3. Create a new notebook: `day-06-spark-sql-basics`
4. Set default language to **SQL**

### Step 2: Create Practice Database

```sql
-- Create database for Day 6 exercises
CREATE DATABASE IF NOT EXISTS day6_sql_practice;
USE day6_sql_practice;
```

### Step 3: Create Sample Tables

We'll create a realistic e-commerce dataset with multiple related tables.

**Employees Table**:
```sql
CREATE TABLE IF NOT EXISTS employees (
    employee_id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    hire_date DATE,
    job_title STRING,
    salary DECIMAL(10,2),
    manager_id INT,
    department_id INT
) USING DELTA;

INSERT INTO employees VALUES
    (1, 'John', 'Smith', 'john.smith@company.com', '555-0101', '2020-01-15', 'CEO', 150000.00, NULL, 1),
    (2, 'Sarah', 'Johnson', 'sarah.j@company.com', '555-0102', '2020-02-01', 'VP Engineering', 130000.00, 1, 1),
    (3, 'Mike', 'Williams', 'mike.w@company.com', '555-0103', '2020-03-15', 'VP Sales', 125000.00, 1, 2),
    (4, 'Emily', 'Brown', 'emily.b@company.com', '555-0104', '2020-04-01', 'Senior Engineer', 95000.00, 2, 1),
    (5, 'David', 'Jones', 'david.j@company.com', '555-0105', '2020-05-15', 'Engineer', 85000.00, 2, 1),
    (6, 'Lisa', 'Garcia', 'lisa.g@company.com', '555-0106', '2020-06-01', 'Engineer', 82000.00, 2, 1),
    (7, 'James', 'Miller', 'james.m@company.com', '555-0107', '2020-07-15', 'Sales Manager', 90000.00, 3, 2),
    (8, 'Maria', 'Davis', 'maria.d@company.com', '555-0108', '2020-08-01', 'Sales Rep', 65000.00, 7, 2),
    (9, 'Robert', 'Rodriguez', 'robert.r@company.com', '555-0109', '2020-09-15', 'Sales Rep', 62000.00, 7, 2),
    (10, 'Jennifer', 'Martinez', 'jennifer.m@company.com', '555-0110', '2020-10-01', 'Marketing Manager', 88000.00, 1, 3),
    (11, 'William', 'Hernandez', 'william.h@company.com', '555-0111', '2021-01-15', 'Marketing Specialist', 58000.00, 10, 3),
    (12, 'Linda', 'Lopez', 'linda.l@company.com', '555-0112', '2021-02-01', 'Data Analyst', 72000.00, 2, 1),
    (13, 'Richard', 'Gonzalez', 'richard.g@company.com', '555-0113', '2021-03-15', 'Junior Engineer', 68000.00, 4, 1),
    (14, 'Patricia', 'Wilson', 'patricia.w@company.com', '555-0114', '2021-04-01', 'Sales Rep', 60000.00, 7, 2),
    (15, 'Charles', 'Anderson', 'charles.a@company.com', '555-0115', '2021-05-15', 'HR Manager', 85000.00, 1, 4);
```

**Departments Table**:
```sql
CREATE TABLE IF NOT EXISTS departments (
    department_id INT,
    department_name STRING,
    location_id INT,
    budget DECIMAL(12,2)
) USING DELTA;

INSERT INTO departments VALUES
    (1, 'Engineering', 1, 500000.00),
    (2, 'Sales', 2, 300000.00),
    (3, 'Marketing', 2, 200000.00),
    (4, 'Human Resources', 1, 150000.00),
    (5, 'Finance', 1, 250000.00);
```

**Locations Table**:
```sql
CREATE TABLE IF NOT EXISTS locations (
    location_id INT,
    city STRING,
    state STRING,
    country STRING
) USING DELTA;

INSERT INTO locations VALUES
    (1, 'San Francisco', 'CA', 'USA'),
    (2, 'New York', 'NY', 'USA'),
    (3, 'Austin', 'TX', 'USA'),
    (4, 'Seattle', 'WA', 'USA');
```

**Customers Table**:
```sql
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT,
    customer_name STRING,
    email STRING,
    city STRING,
    state STRING,
    registration_date DATE
) USING DELTA;

INSERT INTO customers VALUES
    (101, 'Acme Corp', 'contact@acme.com', 'Los Angeles', 'CA', '2020-01-10'),
    (102, 'TechStart Inc', 'info@techstart.com', 'Boston', 'MA', '2020-02-15'),
    (103, 'Global Solutions', 'hello@global.com', 'Chicago', 'IL', '2020-03-20'),
    (104, 'Innovation Labs', 'contact@innovation.com', 'Austin', 'TX', '2020-04-25'),
    (105, 'DataCorp', 'info@datacorp.com', 'Seattle', 'WA', '2020-05-30'),
    (106, 'CloudTech', 'hello@cloudtech.com', 'San Francisco', 'CA', '2020-06-15'),
    (107, 'AI Systems', 'contact@aisystems.com', 'New York', 'NY', '2020-07-20'),
    (108, 'Smart Analytics', 'info@smartanalytics.com', 'Denver', 'CO', '2020-08-25'),
    (109, 'Future Tech', 'hello@futuretech.com', 'Miami', 'FL', '2020-09-30'),
    (110, 'Digital Dynamics', 'contact@digital.com', 'Portland', 'OR', '2020-10-15');
```

**Orders Table**:
```sql
CREATE TABLE IF NOT EXISTS orders (
    order_id INT,
    customer_id INT,
    employee_id INT,
    order_date DATE,
    ship_date DATE,
    order_total DECIMAL(10,2),
    status STRING
) USING DELTA;

INSERT INTO orders VALUES
    (1001, 101, 8, '2024-01-05', '2024-01-07', 15000.00, 'Delivered'),
    (1002, 102, 8, '2024-01-10', '2024-01-12', 22000.00, 'Delivered'),
    (1003, 103, 9, '2024-01-15', '2024-01-17', 18500.00, 'Delivered'),
    (1004, 104, 8, '2024-01-20', '2024-01-22', 31000.00, 'Delivered'),
    (1005, 105, 9, '2024-01-25', '2024-01-27', 12500.00, 'Delivered'),
    (1006, 106, 14, '2024-02-01', '2024-02-03', 28000.00, 'Delivered'),
    (1007, 107, 8, '2024-02-05', '2024-02-07', 19500.00, 'Delivered'),
    (1008, 108, 9, '2024-02-10', '2024-02-12', 24000.00, 'Delivered'),
    (1009, 109, 14, '2024-02-15', '2024-02-17', 16500.00, 'Delivered'),
    (1010, 110, 8, '2024-02-20', NULL, 33000.00, 'Processing'),
    (1011, 101, 9, '2024-02-25', NULL, 21000.00, 'Processing'),
    (1012, 103, 14, '2024-03-01', NULL, 27500.00, 'Pending');
```

**Products Table**:
```sql
CREATE TABLE IF NOT EXISTS products (
    product_id INT,
    product_name STRING,
    category STRING,
    unit_price DECIMAL(10,2),
    stock_quantity INT
) USING DELTA;

INSERT INTO products VALUES
    (201, 'Laptop Pro', 'Electronics', 1200.00, 50),
    (202, 'Wireless Mouse', 'Electronics', 25.00, 200),
    (203, 'USB-C Cable', 'Accessories', 15.00, 500),
    (204, 'Monitor 27"', 'Electronics', 350.00, 75),
    (205, 'Keyboard Mechanical', 'Electronics', 120.00, 100),
    (206, 'Webcam HD', 'Electronics', 80.00, 150),
    (207, 'Headphones', 'Electronics', 150.00, 120),
    (208, 'Desk Lamp', 'Furniture', 45.00, 80),
    (209, 'Office Chair', 'Furniture', 300.00, 40),
    (210, 'Standing Desk', 'Furniture', 600.00, 25);
```

**Order Items Table**:
```sql
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2)
) USING DELTA;

INSERT INTO order_items VALUES
    (1, 1001, 201, 10, 1200.00),
    (2, 1001, 204, 5, 350.00),
    (3, 1002, 201, 15, 1200.00),
    (4, 1002, 205, 20, 120.00),
    (5, 1003, 209, 50, 300.00),
    (6, 1003, 208, 100, 45.00),
    (7, 1004, 201, 20, 1200.00),
    (8, 1004, 204, 15, 350.00),
    (9, 1005, 207, 50, 150.00),
    (10, 1005, 206, 50, 80.00),
    (11, 1006, 201, 20, 1200.00),
    (12, 1006, 209, 10, 300.00),
    (13, 1007, 210, 25, 600.00),
    (14, 1007, 208, 100, 45.00),
    (15, 1008, 201, 18, 1200.00),
    (16, 1008, 205, 20, 120.00);
```

### Step 4: Verify Data

```sql
-- Check row counts
SELECT 'employees' AS table_name, COUNT(*) AS row_count FROM employees
UNION ALL
SELECT 'departments', COUNT(*) FROM departments
UNION ALL
SELECT 'locations', COUNT(*) FROM locations
UNION ALL
SELECT 'customers', COUNT(*) FROM customers
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'order_items', COUNT(*) FROM order_items;

-- Expected output:
-- employees: 15
-- departments: 5
-- locations: 4
-- customers: 10
-- orders: 12
-- products: 10
-- order_items: 16
```

### Step 5: Explore Table Relationships

```sql
-- View table relationships
DESCRIBE EXTENDED employees;
DESCRIBE EXTENDED departments;
DESCRIBE EXTENDED orders;

-- Sample data from each table
SELECT * FROM employees LIMIT 5;
SELECT * FROM departments;
SELECT * FROM locations;
SELECT * FROM customers LIMIT 5;
SELECT * FROM orders LIMIT 5;
```

## Data Model

```
employees
├── employee_id (PK)
├── manager_id (FK → employees.employee_id)
└── department_id (FK → departments.department_id)

departments
├── department_id (PK)
└── location_id (FK → locations.location_id)

locations
└── location_id (PK)

customers
└── customer_id (PK)

orders
├── order_id (PK)
├── customer_id (FK → customers.customer_id)
└── employee_id (FK → employees.employee_id)

products
└── product_id (PK)

order_items
├── order_item_id (PK)
├── order_id (FK → orders.order_id)
└── product_id (FK → products.product_id)
```

## Verification Checklist

- [ ] Database `day6_sql_practice` created
- [ ] All 7 tables created successfully
- [ ] Data inserted into all tables
- [ ] Row counts match expected values
- [ ] Can query each table individually
- [ ] Understand table relationships

## Troubleshooting

### Issue: "Database already exists"
**Solution**: Use `CREATE DATABASE IF NOT EXISTS` or drop and recreate:
```sql
DROP DATABASE IF EXISTS day6_sql_practice CASCADE;
CREATE DATABASE day6_sql_practice;
```

### Issue: "Table already exists"
**Solution**: Use `CREATE TABLE IF NOT EXISTS` or drop and recreate:
```sql
DROP TABLE IF EXISTS employees;
-- Then recreate the table
```

### Issue: "Cannot insert duplicate values"
**Solution**: Clear existing data first:
```sql
DELETE FROM employees;
-- Then insert data
```

### Issue: "Foreign key constraint violation"
**Solution**: Insert data in the correct order:
1. locations (no dependencies)
2. departments (depends on locations)
3. employees (depends on departments)
4. customers (no dependencies)
5. products (no dependencies)
6. orders (depends on customers and employees)
7. order_items (depends on orders and products)

### Issue: Cluster not running
**Solution**: 
1. Go to Compute
2. Start your cluster
3. Wait for it to reach "Running" state
4. Refresh your notebook

## Quick Reference

**Common Commands**:
```sql
-- Switch database
USE day6_sql_practice;

-- List tables
SHOW TABLES;

-- View table structure
DESCRIBE employees;

-- Count rows
SELECT COUNT(*) FROM employees;

-- View sample data
SELECT * FROM employees LIMIT 10;
```

## What's Next?

You're ready for Day 6 exercises! Open `exercise.sql` and start practicing SQL queries.

**Learning Path**:
1. Start with basic SELECT statements
2. Practice different JOIN types
3. Work on aggregations
4. Try subqueries and CTEs
5. Experiment with set operations
6. Complete bonus challenges

Good luck! 🚀

