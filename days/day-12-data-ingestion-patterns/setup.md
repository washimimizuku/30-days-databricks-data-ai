# Day 12 Setup: Data Ingestion Patterns

## Prerequisites

- Completed Days 1-11
- Active Databricks cluster
- Workspace access with table creation permissions
- Understanding of DataFrame operations

## Setup Steps

### Step 1: Create Notebook

1. In your Databricks workspace, navigate to **Workspace**
2. Click **Create** → **Notebook**
3. Name it: `Day-12-Data-Ingestion-Patterns`
4. Language: **Python**
5. Cluster: Select your running cluster
6. Click **Create**

### Step 2: Create Data Directories

```python
# Create directories for Day 12 exercises
dbutils.fs.mkdirs("/FileStore/day12-ingestion/csv/")
dbutils.fs.mkdirs("/FileStore/day12-ingestion/json/")
dbutils.fs.mkdirs("/FileStore/day12-ingestion/parquet/")
dbutils.fs.mkdirs("/FileStore/day12-ingestion/incremental/")
dbutils.fs.mkdirs("/FileStore/day12-ingestion/checkpoints/")
dbutils.fs.mkdirs("/FileStore/day12-ingestion/schemas/")
print("Directories created!")
```

### Step 3: Create Sample CSV Files

```python
# Create sample CSV data - Batch 1
csv_data_1 = """id,name,email,amount,date
1,Alice Johnson,alice@email.com,1500.50,2024-01-15
2,Bob Smith,bob@email.com,2300.75,2024-01-15
3,Charlie Brown,charlie@email.com,1800.00,2024-01-15
4,Diana Prince,diana@email.com,2100.25,2024-01-15
5,Eve Davis,eve@email.com,1950.00,2024-01-15"""

with open("/dbfs/FileStore/day12-ingestion/csv/batch1.csv", "w") as f:
    f.write(csv_data_1)

# Create sample CSV data - Batch 2
csv_data_2 = """id,name,email,amount,date
6,Frank Miller,frank@email.com,2500.00,2024-01-16
7,Grace Lee,grace@email.com,1700.50,2024-01-16
8,Henry Wilson,henry@email.com,2200.75,2024-01-16
9,Ivy Chen,ivy@email.com,1900.00,2024-01-16
10,Jack Taylor,jack@email.com,2400.25,2024-01-16"""

with open("/dbfs/FileStore/day12-ingestion/csv/batch2.csv", "w") as f:
    f.write(csv_data_2)

# Create sample CSV data - Batch 3 (with schema change)
csv_data_3 = """id,name,email,amount,date,category
11,Kate Brown,kate@email.com,1600.00,2024-01-17,Premium
12,Leo Martinez,leo@email.com,2300.50,2024-01-17,Standard
13,Mia Anderson,mia@email.com,1850.75,2024-01-17,Premium
14,Noah Davis,noah@email.com,2100.00,2024-01-17,Standard
15,Olivia Wilson,olivia@email.com,1950.25,2024-01-17,Premium"""

with open("/dbfs/FileStore/day12-ingestion/csv/batch3.csv", "w") as f:
    f.write(csv_data_3)

print("CSV files created!")
```

### Step 4: Create Sample JSON Files

```python
import json

# Create JSON data - File 1
json_data_1 = [
    {"order_id": 1001, "customer_id": 501, "product": "Laptop", "quantity": 2, "price": 1200.00, "order_date": "2024-01-15"},
    {"order_id": 1002, "customer_id": 502, "product": "Mouse", "quantity": 5, "price": 25.00, "order_date": "2024-01-15"},
    {"order_id": 1003, "customer_id": 503, "product": "Keyboard", "quantity": 3, "price": 75.00, "order_date": "2024-01-15"}
]

with open("/dbfs/FileStore/day12-ingestion/json/orders1.json", "w") as f:
    for record in json_data_1:
        f.write(json.dumps(record) + "\n")

# Create JSON data - File 2
json_data_2 = [
    {"order_id": 1004, "customer_id": 501, "product": "Monitor", "quantity": 1, "price": 300.00, "order_date": "2024-01-16"},
    {"order_id": 1005, "customer_id": 504, "product": "Desk", "quantity": 2, "price": 400.00, "order_date": "2024-01-16"},
    {"order_id": 1006, "customer_id": 505, "product": "Chair", "quantity": 1, "price": 250.00, "order_date": "2024-01-16"}
]

with open("/dbfs/FileStore/day12-ingestion/json/orders2.json", "w") as f:
    for record in json_data_2:
        f.write(json.dumps(record) + "\n")

# Create JSON data - File 3 (with nested structure)
json_data_3 = [
    {
        "order_id": 1007,
        "customer": {"id": 506, "name": "John Doe", "email": "john@email.com"},
        "items": [{"product": "Laptop", "quantity": 1, "price": 1200.00}],
        "order_date": "2024-01-17"
    },
    {
        "order_id": 1008,
        "customer": {"id": 507, "name": "Jane Smith", "email": "jane@email.com"},
        "items": [{"product": "Mouse", "quantity": 3, "price": 25.00}, {"product": "Keyboard", "quantity": 2, "price": 75.00}],
        "order_date": "2024-01-17"
    }
]

with open("/dbfs/FileStore/day12-ingestion/json/orders3.json", "w") as f:
    for record in json_data_3:
        f.write(json.dumps(record) + "\n")

print("JSON files created!")
```

### Step 5: Create Sample Parquet Files

```python
from pyspark.sql import Row

# Create Parquet data - File 1
parquet_data_1 = [
    Row(product_id=101, product_name="Laptop", category="Electronics", price=1200.00, stock=50),
    Row(product_id=102, product_name="Mouse", category="Electronics", price=25.00, stock=200),
    Row(product_id=103, product_name="Keyboard", category="Electronics", price=75.00, stock=150)
]

df_parquet_1 = spark.createDataFrame(parquet_data_1)
df_parquet_1.write.mode("overwrite").parquet("/FileStore/day12-ingestion/parquet/products1.parquet")

# Create Parquet data - File 2
parquet_data_2 = [
    Row(product_id=104, product_name="Monitor", category="Electronics", price=300.00, stock=75),
    Row(product_id=105, product_name="Desk", category="Furniture", price=400.00, stock=30),
    Row(product_id=106, product_name="Chair", category="Furniture", price=250.00, stock=40)
]

df_parquet_2 = spark.createDataFrame(parquet_data_2)
df_parquet_2.write.mode("overwrite").parquet("/FileStore/day12-ingestion/parquet/products2.parquet")

print("Parquet files created!")
```

### Step 6: Create Incremental Data Files

```python
# Create incremental CSV files for Auto Loader testing
for i in range(1, 6):
    csv_data = f"""id,name,amount,timestamp
{i*100+1},User{i*100+1},{1000+i*100},2024-01-{15+i:02d}T10:00:00
{i*100+2},User{i*100+2},{1100+i*100},2024-01-{15+i:02d}T10:05:00
{i*100+3},User{i*100+3},{1200+i*100},2024-01-{15+i:02d}T10:10:00"""
    
    with open(f"/dbfs/FileStore/day12-ingestion/incremental/data_{i}.csv", "w") as f:
        f.write(csv_data)

print("Incremental files created!")
```

### Step 7: Create Database

```python
# Create database for Day 12
spark.sql("CREATE DATABASE IF NOT EXISTS day12_db")
spark.sql("USE day12_db")
print("Database day12_db created and set as default!")
```

### Step 8: Create Target Tables

```python
# Create target table for COPY INTO exercises
spark.sql("""
CREATE TABLE IF NOT EXISTS day12_db.customers_copy (
    id INT,
    name STRING,
    email STRING,
    amount DOUBLE,
    date STRING
)
USING DELTA
""")

# Create target table for Auto Loader exercises
spark.sql("""
CREATE TABLE IF NOT EXISTS day12_db.customers_autoloader (
    id INT,
    name STRING,
    email STRING,
    amount DOUBLE,
    date STRING
)
USING DELTA
""")

# Create target table for orders
spark.sql("""
CREATE TABLE IF NOT EXISTS day12_db.orders (
    order_id INT,
    customer_id INT,
    product STRING,
    quantity INT,
    price DOUBLE,
    order_date STRING
)
USING DELTA
""")

print("Target tables created!")
```

## Verification Checklist

Run this verification code:

```python
# Verification script
checks = []

# Check 1: Directories exist
directories = [
    "/FileStore/day12-ingestion/csv/",
    "/FileStore/day12-ingestion/json/",
    "/FileStore/day12-ingestion/parquet/",
    "/FileStore/day12-ingestion/incremental/"
]

for directory in directories:
    try:
        dbutils.fs.ls(directory)
        checks.append(("✅", f"Directory {directory} exists"))
    except:
        checks.append(("❌", f"Directory {directory} NOT found"))

# Check 2: CSV files exist
try:
    csv_files = [f.name for f in dbutils.fs.ls("/FileStore/day12-ingestion/csv/")]
    if len(csv_files) >= 3:
        checks.append(("✅", f"CSV files created ({len(csv_files)} files)"))
    else:
        checks.append(("❌", f"Not enough CSV files ({len(csv_files)} found, need 3)"))
except Exception as e:
    checks.append(("❌", f"Cannot list CSV files: {str(e)}"))

# Check 3: JSON files exist
try:
    json_files = [f.name for f in dbutils.fs.ls("/FileStore/day12-ingestion/json/")]
    if len(json_files) >= 3:
        checks.append(("✅", f"JSON files created ({len(json_files)} files)"))
    else:
        checks.append(("❌", f"Not enough JSON files ({len(json_files)} found, need 3)"))
except Exception as e:
    checks.append(("❌", f"Cannot list JSON files: {str(e)}"))

# Check 4: Parquet files exist
try:
    parquet_dirs = [f.name for f in dbutils.fs.ls("/FileStore/day12-ingestion/parquet/")]
    if len(parquet_dirs) >= 2:
        checks.append(("✅", f"Parquet files created ({len(parquet_dirs)} directories)"))
    else:
        checks.append(("❌", f"Not enough Parquet files ({len(parquet_dirs)} found, need 2)"))
except Exception as e:
    checks.append(("❌", f"Cannot list Parquet files: {str(e)}"))

# Check 5: Incremental files exist
try:
    inc_files = [f.name for f in dbutils.fs.ls("/FileStore/day12-ingestion/incremental/")]
    if len(inc_files) >= 5:
        checks.append(("✅", f"Incremental files created ({len(inc_files)} files)"))
    else:
        checks.append(("❌", f"Not enough incremental files ({len(inc_files)} found, need 5)"))
except Exception as e:
    checks.append(("❌", f"Cannot list incremental files: {str(e)}"))

# Check 6: Database exists
databases = [db.database for db in spark.sql("SHOW DATABASES").collect()]
if "day12_db" in databases:
    checks.append(("✅", "Database day12_db exists"))
else:
    checks.append(("❌", "Database day12_db NOT found"))

# Check 7: Tables exist
try:
    tables = [t.tableName for t in spark.sql("SHOW TABLES IN day12_db").collect()]
    expected_tables = ["customers_copy", "customers_autoloader", "orders"]
    for table in expected_tables:
        if table in tables:
            checks.append(("✅", f"Table {table} exists"))
        else:
            checks.append(("❌", f"Table {table} NOT found"))
except Exception as e:
    checks.append(("❌", f"Cannot list tables: {str(e)}"))

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
    print("\n🎉 All checks passed! You're ready for Day 12 exercises!")
else:
    print("\n⚠️  Some checks failed. Please review the setup steps above.")
```

Expected output:
```
============================================================
VERIFICATION RESULTS
============================================================
✅ Directory /FileStore/day12-ingestion/csv/ exists
✅ Directory /FileStore/day12-ingestion/json/ exists
✅ Directory /FileStore/day12-ingestion/parquet/ exists
✅ Directory /FileStore/day12-ingestion/incremental/ exists
✅ CSV files created (3 files)
✅ JSON files created (3 files)
✅ Parquet files created (2 directories)
✅ Incremental files created (5 files)
✅ Database day12_db exists
✅ Table customers_copy exists
✅ Table customers_autoloader exists
✅ Table orders exists
============================================================

Passed: 12/12

🎉 All checks passed! You're ready for Day 12 exercises!
```

## Troubleshooting

### Issue: "Directory already exists" error

**Solution:**
```python
# Remove existing directories and recreate
dbutils.fs.rm("/FileStore/day12-ingestion/", recurse=True)
# Then run setup steps 2-6 again
```

### Issue: "Database already exists" error

**Solution:**
```python
# Drop and recreate database
spark.sql("DROP DATABASE IF EXISTS day12_db CASCADE")
spark.sql("CREATE DATABASE day12_db")
```

### Issue: Cannot read CSV files

**Solution:**
1. Verify the file was created:
   ```python
   dbutils.fs.ls("/FileStore/day12-ingestion/csv/")
   ```
2. Check file contents:
   ```python
   print(dbutils.fs.head("/FileStore/day12-ingestion/csv/batch1.csv"))
   ```
3. Recreate the file using Step 3

### Issue: COPY INTO fails

**Symptoms:** Error like "Table not found" or "Path does not exist"

**Solution:**
1. Verify table exists:
   ```python
   spark.sql("SHOW TABLES IN day12_db").show()
   ```
2. Verify file path:
   ```python
   dbutils.fs.ls("/FileStore/day12-ingestion/csv/")
   ```
3. Use full path in COPY INTO command

### Issue: Auto Loader checkpoint errors

**Solution:**
- Remove checkpoint directory and restart:
  ```python
  dbutils.fs.rm("/FileStore/day12-ingestion/checkpoints/", recurse=True)
  ```
- Ensure checkpoint location is unique for each stream

### Issue: Schema evolution not working

**Solution:**
- Verify schema location is set:
  ```python
  .option("cloudFiles.schemaLocation", "/FileStore/day12-ingestion/schemas/")
  ```
- Check schema evolution mode:
  ```python
  .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
  ```

## Quick Reset

If you need to start fresh:

```python
# Complete reset script
print("Resetting Day 12 environment...")

# Remove files
try:
    dbutils.fs.rm("/FileStore/day12-ingestion/", recurse=True)
    print("✅ Removed old files")
except:
    print("⚠️  No files to remove")

# Drop database
spark.sql("DROP DATABASE IF EXISTS day12_db CASCADE")
print("✅ Dropped database")

# Recreate directories
dbutils.fs.mkdirs("/FileStore/day12-ingestion/csv/")
dbutils.fs.mkdirs("/FileStore/day12-ingestion/json/")
dbutils.fs.mkdirs("/FileStore/day12-ingestion/parquet/")
dbutils.fs.mkdirs("/FileStore/day12-ingestion/incremental/")
dbutils.fs.mkdirs("/FileStore/day12-ingestion/checkpoints/")
dbutils.fs.mkdirs("/FileStore/day12-ingestion/schemas/")
print("✅ Created directories")

# Recreate database
spark.sql("CREATE DATABASE day12_db")
print("✅ Created database")

print("\n✅ Reset complete! Run setup steps 3-8 to recreate data files and tables.")
```

## What's Next?

You're ready for Day 12 exercises! Open `exercise.py` and start working through the 60 hands-on exercises.

**Tips for Success:**
1. Start with COPY INTO before Auto Loader
2. Test with small datasets first
3. Monitor checkpoint locations
4. Pay attention to schema evolution
5. Use display() to verify results
6. Understand idempotency guarantees
7. Practice both SQL and Python approaches

Good luck! 🚀
