# Day 10 Setup: Python DataFrames Basics

## Prerequisites

- Completed Days 1-9
- Active Databricks cluster (any size)
- Workspace access with notebook creation permissions
- Basic Python knowledge

## Setup Steps

### Step 1: Create Notebook

1. In your Databricks workspace, navigate to **Workspace**
2. Click **Create** → **Notebook**
3. Name it: `Day-10-Python-DataFrames-Basics`
4. Language: **Python**
5. Cluster: Select your running cluster
6. Click **Create**

### Step 2: Create Sample Data Directory

Run this code in your notebook to create the data directory:

```python
# Create directory for Day 10 exercises
dbutils.fs.mkdirs("/FileStore/day10-exercises/")
print("Directory created!")
```

### Step 3: Create Sample CSV Data

```python
# Create sample employees CSV
csv_data = """id,name,department,salary,hire_date,city
1,Alice Johnson,Engineering,95000,2020-01-15,Seattle
2,Bob Smith,Sales,75000,2019-06-20,New York
3,Charlie Brown,Engineering,88000,2021-03-10,Seattle
4,Diana Prince,Marketing,82000,2020-08-05,Los Angeles
5,Eve Davis,Sales,79000,2021-01-12,New York
6,Frank Miller,Engineering,92000,2018-11-30,Seattle
7,Grace Lee,HR,71000,2020-05-18,Chicago
8,Henry Wilson,Marketing,85000,2019-09-25,Los Angeles
9,Ivy Chen,Engineering,98000,2022-02-14,Seattle
10,Jack Taylor,Sales,77000,2021-07-08,New York"""

# Write to DBFS
with open("/dbfs/FileStore/day10-exercises/employees.csv", "w") as f:
    f.write(csv_data)

print("employees.csv created!")
```

### Step 4: Create Sample JSON Data

```python
# Create sample products JSON
import json

products = [
    {"product_id": 101, "product_name": "Laptop", "category": "Electronics", "price": 999.99, "stock": 50},
    {"product_id": 102, "product_name": "Mouse", "category": "Electronics", "price": 29.99, "stock": 200},
    {"product_id": 103, "product_name": "Keyboard", "category": "Electronics", "price": 79.99, "stock": 150},
    {"product_id": 104, "product_name": "Monitor", "category": "Electronics", "price": 299.99, "stock": 75},
    {"product_id": 105, "product_name": "Desk Chair", "category": "Furniture", "price": 249.99, "stock": 30},
    {"product_id": 106, "product_name": "Desk", "category": "Furniture", "price": 399.99, "stock": 20},
    {"product_id": 107, "product_name": "Notebook", "category": "Stationery", "price": 4.99, "stock": 500},
    {"product_id": 108, "product_name": "Pen Set", "category": "Stationery", "price": 12.99, "stock": 300}
]

with open("/dbfs/FileStore/day10-exercises/products.json", "w") as f:
    for product in products:
        f.write(json.dumps(product) + "\n")

print("products.json created!")
```

### Step 5: Create Sample Sales Data

```python
# Create sample sales CSV
sales_data = """order_id,customer_id,product_id,quantity,order_date,amount
1001,501,101,2,2024-01-15,1999.98
1002,502,102,5,2024-01-16,149.95
1003,503,103,3,2024-01-17,239.97
1004,501,104,1,2024-01-18,299.99
1005,504,105,2,2024-01-19,499.98
1006,505,106,1,2024-01-20,399.99
1007,502,107,10,2024-01-21,49.90
1008,506,108,4,2024-01-22,51.96
1009,503,101,1,2024-01-23,999.99
1010,507,102,3,2024-01-24,89.97"""

with open("/dbfs/FileStore/day10-exercises/sales.csv", "w") as f:
    f.write(sales_data)

print("sales.csv created!")
```

### Step 6: Create Sample Customer Data with Nulls

```python
# Create sample customers CSV with null values
customers_data = """customer_id,name,email,phone,city,state
501,John Doe,john@email.com,555-0101,Seattle,WA
502,Jane Smith,jane@email.com,,New York,NY
503,Bob Johnson,bob@email.com,555-0103,Los Angeles,CA
504,Alice Williams,,555-0104,Chicago,IL
505,Charlie Brown,charlie@email.com,555-0105,,TX
506,Diana Prince,diana@email.com,555-0106,Miami,FL
507,Eve Davis,,,,CA"""

with open("/dbfs/FileStore/day10-exercises/customers.csv", "w") as f:
    f.write(customers_data)

print("customers.csv created!")
```

### Step 7: Create Database for Exercises

```python
# Create database for Day 10
spark.sql("CREATE DATABASE IF NOT EXISTS day10_db")
spark.sql("USE day10_db")
print("Database day10_db created and set as default!")
```

### Step 8: Verify Setup

```python
# Verify all files were created
files = dbutils.fs.ls("/FileStore/day10-exercises/")
print("Files created:")
for file in files:
    print(f"  - {file.name}")

# Verify database
databases = spark.sql("SHOW DATABASES").collect()
print("\nDatabases:")
for db in databases:
    print(f"  - {db.database}")
```

## Verification Checklist

Run this verification code:

```python
# Verification script
checks = []

# Check 1: Directory exists
try:
    dbutils.fs.ls("/FileStore/day10-exercises/")
    checks.append(("✅", "Directory /FileStore/day10-exercises/ exists"))
except:
    checks.append(("❌", "Directory /FileStore/day10-exercises/ NOT found"))

# Check 2: Files exist
required_files = ["employees.csv", "products.json", "sales.csv", "customers.csv"]
files = [f.name for f in dbutils.fs.ls("/FileStore/day10-exercises/")]

for req_file in required_files:
    if req_file in files:
        checks.append(("✅", f"File {req_file} exists"))
    else:
        checks.append(("❌", f"File {req_file} NOT found"))

# Check 3: Database exists
databases = [db.database for db in spark.sql("SHOW DATABASES").collect()]
if "day10_db" in databases:
    checks.append(("✅", "Database day10_db exists"))
else:
    checks.append(("❌", "Database day10_db NOT found"))

# Check 4: Can read CSV
try:
    df = spark.read.csv("/FileStore/day10-exercises/employees.csv", header=True)
    count = df.count()
    checks.append(("✅", f"Can read employees.csv ({count} rows)"))
except Exception as e:
    checks.append(("❌", f"Cannot read employees.csv: {str(e)}"))

# Check 5: Can read JSON
try:
    df = spark.read.json("/FileStore/day10-exercises/products.json")
    count = df.count()
    checks.append(("✅", f"Can read products.json ({count} rows)"))
except Exception as e:
    checks.append(("❌", f"Cannot read products.json: {str(e)}"))

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
    print("\n🎉 All checks passed! You're ready for Day 10 exercises!")
else:
    print("\n⚠️  Some checks failed. Please review the setup steps above.")
```

Expected output:
```
============================================================
VERIFICATION RESULTS
============================================================
✅ Directory /FileStore/day10-exercises/ exists
✅ File employees.csv exists
✅ File products.json exists
✅ File sales.csv exists
✅ File customers.csv exists
✅ Database day10_db exists
✅ Can read employees.csv (10 rows)
✅ Can read products.json (8 rows)
============================================================

Passed: 8/8

🎉 All checks passed! You're ready for Day 10 exercises!
```

## Troubleshooting

### Issue: "Directory already exists" error

**Solution:**
```python
# Remove existing directory and recreate
dbutils.fs.rm("/FileStore/day10-exercises/", recurse=True)
dbutils.fs.mkdirs("/FileStore/day10-exercises/")
```

### Issue: "Database already exists" error

**Solution:**
```python
# Drop and recreate database
spark.sql("DROP DATABASE IF EXISTS day10_db CASCADE")
spark.sql("CREATE DATABASE day10_db")
```

### Issue: Cannot read CSV files

**Symptoms:** Error like "Path does not exist" or "File not found"

**Solution:**
1. Verify the file was created:
   ```python
   dbutils.fs.ls("/FileStore/day10-exercises/")
   ```
2. Check file contents:
   ```python
   print(dbutils.fs.head("/FileStore/day10-exercises/employees.csv"))
   ```
3. Recreate the file using Step 3 above

### Issue: Cluster not responding

**Solution:**
1. Check cluster status in the Clusters page
2. Restart the cluster if needed
3. Reattach notebook to cluster

### Issue: Permission denied errors

**Solution:**
- Ensure you have workspace user permissions
- Contact your workspace administrator if needed
- Try using `/tmp/day10-exercises/` instead of `/FileStore/day10-exercises/`

### Issue: Python syntax errors

**Solution:**
- Ensure notebook language is set to **Python** (not SQL or Scala)
- Check for proper indentation
- Verify you're using Python 3 syntax

## Quick Reset

If you need to start fresh:

```python
# Complete reset script
print("Resetting Day 10 environment...")

# Remove files
try:
    dbutils.fs.rm("/FileStore/day10-exercises/", recurse=True)
    print("✅ Removed old files")
except:
    print("⚠️  No files to remove")

# Drop database
spark.sql("DROP DATABASE IF EXISTS day10_db CASCADE")
print("✅ Dropped database")

# Recreate directory
dbutils.fs.mkdirs("/FileStore/day10-exercises/")
print("✅ Created directory")

# Recreate database
spark.sql("CREATE DATABASE day10_db")
print("✅ Created database")

print("\n✅ Reset complete! Run setup steps 3-6 to recreate data files.")
```

## What's Next?

You're ready for Day 10 exercises! Open `exercise.py` and start working through the 40 hands-on exercises.

**Tips for Success:**
1. Read each exercise carefully
2. Try to solve it yourself before looking at solutions
3. Run your code frequently to test
4. Use `display()` to visualize DataFrames
5. Experiment with different approaches
6. Take notes on key concepts

Good luck! 🚀
