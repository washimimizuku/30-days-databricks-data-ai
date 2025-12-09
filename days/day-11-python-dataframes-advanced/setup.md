# Day 11 Setup: Python DataFrames Advanced

## Prerequisites

- Completed Day 10 (Python DataFrames Basics)
- Active Databricks cluster
- Workspace access with notebook creation permissions
- Understanding of basic DataFrame operations

## Setup Steps

### Step 1: Create Notebook

1. In your Databricks workspace, navigate to **Workspace**
2. Click **Create** → **Notebook**
3. Name it: `Day-11-Python-DataFrames-Advanced`
4. Language: **Python**
5. Cluster: Select your running cluster
6. Click **Create**

### Step 2: Create Data Directory

```python
# Create directory for Day 11 exercises
dbutils.fs.mkdirs("/FileStore/day11-exercises/")
print("Directory created!")
```

### Step 3: Create Employees Data

```python
# Create employees data for joins and window functions
employees_data = """emp_id,name,department,salary,hire_date,manager_id,city
1,Alice Johnson,Engineering,95000,2020-01-15,5,Seattle
2,Bob Smith,Sales,75000,2019-06-20,6,New York
3,Charlie Brown,Engineering,88000,2021-03-10,5,Seattle
4,Diana Prince,Marketing,82000,2020-08-05,7,Los Angeles
5,Eve Davis,Engineering,110000,2018-11-30,,Seattle
6,Frank Miller,Sales,98000,2017-09-25,,New York
7,Grace Lee,Marketing,105000,2019-05-18,,Chicago
8,Henry Wilson,Engineering,92000,2022-02-14,5,Seattle
9,Ivy Chen,Sales,79000,2021-07-08,6,New York
10,Jack Taylor,Marketing,85000,2020-12-03,7,Los Angeles
11,Kate Brown,Engineering,97000,2019-10-22,5,Seattle
12,Leo Martinez,Sales,81000,2021-04-15,6,Chicago
13,Mia Anderson,Engineering,90000,2020-06-30,5,Seattle
14,Noah Davis,Marketing,87000,2021-09-12,7,Los Angeles
15,Olivia Wilson,Sales,83000,2022-01-20,6,New York"""

with open("/dbfs/FileStore/day11-exercises/employees.csv", "w") as f:
    f.write(employees_data)

print("employees.csv created!")
```

### Step 4: Create Departments Data

```python
# Create departments data for joins
departments_data = """dept_id,dept_name,location,budget
1,Engineering,Seattle,500000
2,Sales,New York,300000
3,Marketing,Los Angeles,250000
4,HR,Chicago,150000
5,Finance,Boston,200000"""

with open("/dbfs/FileStore/day11-exercises/departments.csv", "w") as f:
    f.write(departments_data)

print("departments.csv created!")
```

### Step 5: Create Sales Data

```python
# Create sales data for window functions
sales_data = """sale_id,emp_id,product,amount,sale_date,region
1001,2,Laptop,1200,2024-01-05,East
1002,9,Mouse,25,2024-01-06,East
1003,12,Keyboard,75,2024-01-07,Central
1004,2,Monitor,300,2024-01-08,East
1005,9,Laptop,1200,2024-01-10,East
1006,12,Mouse,25,2024-01-12,Central
1007,2,Keyboard,75,2024-01-15,East
1008,9,Monitor,300,2024-01-18,East
1009,12,Laptop,1200,2024-01-20,Central
1010,2,Mouse,25,2024-01-22,East
1011,9,Keyboard,75,2024-01-25,East
1012,12,Monitor,300,2024-01-28,Central
1013,2,Laptop,1200,2024-02-02,East
1014,9,Mouse,25,2024-02-05,East
1015,12,Keyboard,75,2024-02-08,Central"""

with open("/dbfs/FileStore/day11-exercises/sales.csv", "w") as f:
    f.write(sales_data)

print("sales.csv created!")
```

### Step 6: Create Products Data

```python
# Create products data
products_data = """product_id,product_name,category,price,stock
101,Laptop,Electronics,1200,50
102,Mouse,Electronics,25,200
103,Keyboard,Electronics,75,150
104,Monitor,Electronics,300,75
105,Desk Chair,Furniture,250,30
106,Desk,Furniture,400,20
107,Notebook,Stationery,5,500
108,Pen Set,Stationery,15,300"""

with open("/dbfs/FileStore/day11-exercises/products.csv", "w") as f:
    f.write(products_data)

print("products.csv created!")
```

### Step 7: Create Orders Data

```python
# Create orders data for complex joins
orders_data = """order_id,customer_id,product_id,quantity,order_date,status
2001,501,101,2,2024-01-15,Delivered
2002,502,102,5,2024-01-16,Delivered
2003,503,103,3,2024-01-17,Shipped
2004,501,104,1,2024-01-18,Delivered
2005,504,105,2,2024-01-19,Processing
2006,505,106,1,2024-01-20,Delivered
2007,502,107,10,2024-01-21,Delivered
2008,506,108,4,2024-01-22,Shipped
2009,503,101,1,2024-01-23,Delivered
2010,507,102,3,2024-01-24,Processing"""

with open("/dbfs/FileStore/day11-exercises/orders.csv", "w") as f:
    f.write(orders_data)

print("orders.csv created!")
```

### Step 8: Create Customers Data

```python
# Create customers data
customers_data = """customer_id,name,email,city,state,join_date
501,John Doe,john@email.com,Seattle,WA,2023-01-10
502,Jane Smith,jane@email.com,New York,NY,2023-02-15
503,Bob Johnson,bob@email.com,Los Angeles,CA,2023-03-20
504,Alice Williams,alice@email.com,Chicago,IL,2023-04-25
505,Charlie Brown,charlie@email.com,Houston,TX,2023-05-30
506,Diana Prince,diana@email.com,Miami,FL,2023-06-15
507,Eve Davis,eve@email.com,Boston,MA,2023-07-20"""

with open("/dbfs/FileStore/day11-exercises/customers.csv", "w") as f:
    f.write(customers_data)

print("customers.csv created!")
```

### Step 9: Create Time Series Data

```python
# Create time series data for window functions
timeseries_data = """date,product,sales,units
2024-01-01,Laptop,12000,10
2024-01-02,Laptop,15000,12
2024-01-03,Laptop,18000,15
2024-01-04,Laptop,14000,11
2024-01-05,Laptop,16000,13
2024-01-06,Laptop,19000,16
2024-01-07,Laptop,17000,14
2024-01-01,Mouse,500,20
2024-01-02,Mouse,600,24
2024-01-03,Mouse,550,22
2024-01-04,Mouse,700,28
2024-01-05,Mouse,650,26
2024-01-06,Mouse,750,30
2024-01-07,Mouse,800,32"""

with open("/dbfs/FileStore/day11-exercises/timeseries.csv", "w") as f:
    f.write(timeseries_data)

print("timeseries.csv created!")
```

### Step 10: Create Database

```python
# Create database for Day 11
spark.sql("CREATE DATABASE IF NOT EXISTS day11_db")
spark.sql("USE day11_db")
print("Database day11_db created and set as default!")
```

### Step 11: Load Data into DataFrames

```python
# Load all datasets
df_employees = spark.read.csv("/FileStore/day11-exercises/employees.csv", header=True, inferSchema=True)
df_departments = spark.read.csv("/FileStore/day11-exercises/departments.csv", header=True, inferSchema=True)
df_sales = spark.read.csv("/FileStore/day11-exercises/sales.csv", header=True, inferSchema=True)
df_products = spark.read.csv("/FileStore/day11-exercises/products.csv", header=True, inferSchema=True)
df_orders = spark.read.csv("/FileStore/day11-exercises/orders.csv", header=True, inferSchema=True)
df_customers = spark.read.csv("/FileStore/day11-exercises/customers.csv", header=True, inferSchema=True)
df_timeseries = spark.read.csv("/FileStore/day11-exercises/timeseries.csv", header=True, inferSchema=True)

print("All DataFrames loaded successfully!")
```

## Verification Checklist

Run this verification code:

```python
# Verification script
checks = []

# Check 1: Directory exists
try:
    dbutils.fs.ls("/FileStore/day11-exercises/")
    checks.append(("✅", "Directory /FileStore/day11-exercises/ exists"))
except:
    checks.append(("❌", "Directory /FileStore/day11-exercises/ NOT found"))

# Check 2: Files exist
required_files = [
    "employees.csv", "departments.csv", "sales.csv", "products.csv",
    "orders.csv", "customers.csv", "timeseries.csv"
]
files = [f.name for f in dbutils.fs.ls("/FileStore/day11-exercises/")]

for req_file in required_files:
    if req_file in files:
        checks.append(("✅", f"File {req_file} exists"))
    else:
        checks.append(("❌", f"File {req_file} NOT found"))

# Check 3: Database exists
databases = [db.database for db in spark.sql("SHOW DATABASES").collect()]
if "day11_db" in databases:
    checks.append(("✅", "Database day11_db exists"))
else:
    checks.append(("❌", "Database day11_db NOT found"))

# Check 4: Can read files
try:
    df = spark.read.csv("/FileStore/day11-exercises/employees.csv", header=True, inferSchema=True)
    count = df.count()
    checks.append(("✅", f"Can read employees.csv ({count} rows)"))
except Exception as e:
    checks.append(("❌", f"Cannot read employees.csv: {str(e)}"))

# Check 5: DataFrames loaded
try:
    df_employees = spark.read.csv("/FileStore/day11-exercises/employees.csv", header=True, inferSchema=True)
    df_departments = spark.read.csv("/FileStore/day11-exercises/departments.csv", header=True, inferSchema=True)
    checks.append(("✅", f"DataFrames loaded: {df_employees.count()} employees, {df_departments.count()} departments"))
except Exception as e:
    checks.append(("❌", f"Cannot load DataFrames: {str(e)}"))

# Print results
print("=" * 60)
print("VERIFICATION RESULTS")
print("=" * 60)
for status, message in checks:
    print(f"{status} {message}")
print("=" * 60)

# Summary
passed = sum(1 for status, _ in checks if status == "✅")
total = len(checks)
print(f"\nPassed: {passed}/{total}")

if passed == total:
    print("\n🎉 All checks passed! You're ready for Day 11 exercises!")
else:
    print("\n⚠️  Some checks failed. Please review the setup steps above.")
```

Expected output:
```
============================================================
VERIFICATION RESULTS
============================================================
✅ Directory /FileStore/day11-exercises/ exists
✅ File employees.csv exists
✅ File departments.csv exists
✅ File sales.csv exists
✅ File products.csv exists
✅ File orders.csv exists
✅ File customers.csv exists
✅ File timeseries.csv exists
✅ Database day11_db exists
✅ Can read employees.csv (15 rows)
✅ DataFrames loaded: 15 employees, 5 departments
============================================================

Passed: 11/11

🎉 All checks passed! You're ready for Day 11 exercises!
```

## Troubleshooting

### Issue: "Directory already exists" error

**Solution:**
```python
# Remove existing directory and recreate
dbutils.fs.rm("/FileStore/day11-exercises/", recurse=True)
dbutils.fs.mkdirs("/FileStore/day11-exercises/")
```

### Issue: "Database already exists" error

**Solution:**
```python
# Drop and recreate database
spark.sql("DROP DATABASE IF EXISTS day11_db CASCADE")
spark.sql("CREATE DATABASE day11_db")
```

### Issue: Cannot read CSV files

**Solution:**
1. Verify the file was created:
   ```python
   dbutils.fs.ls("/FileStore/day11-exercises/")
   ```
2. Check file contents:
   ```python
   print(dbutils.fs.head("/FileStore/day11-exercises/employees.csv"))
   ```
3. Recreate the file using the setup steps above

### Issue: Join returns unexpected results

**Solution:**
- Check for duplicate column names
- Use aliases: `df1.alias("a").join(df2.alias("b"), ...)`
- Verify join conditions are correct
- Check for null values in join keys

### Issue: Window function errors

**Solution:**
- Ensure window specification includes orderBy for ranking functions
- Check partition column exists
- Verify frame specification syntax

### Issue: UDF not working

**Solution:**
- Check return type matches actual return value
- Handle None/null values in UDF
- Verify UDF is registered correctly
- Use Pandas UDF for better performance

## Quick Reset

If you need to start fresh:

```python
# Complete reset script
print("Resetting Day 11 environment...")

# Remove files
try:
    dbutils.fs.rm("/FileStore/day11-exercises/", recurse=True)
    print("✅ Removed old files")
except:
    print("⚠️  No files to remove")

# Drop database
spark.sql("DROP DATABASE IF EXISTS day11_db CASCADE")
print("✅ Dropped database")

# Recreate directory
dbutils.fs.mkdirs("/FileStore/day11-exercises/")
print("✅ Created directory")

# Recreate database
spark.sql("CREATE DATABASE day11_db")
print("✅ Created database")

print("\n✅ Reset complete! Run setup steps 3-9 to recreate data files.")
```

## What's Next?

You're ready for Day 11 exercises! Open `exercise.py` and start working through the 80 hands-on exercises.

**Tips for Success:**
1. Understand join types before starting
2. Practice window function syntax
3. Start with built-in functions before UDFs
4. Use display() to visualize results
5. Test each exercise incrementally
6. Pay attention to performance implications
7. Review the README for syntax reference

Good luck! 🚀
