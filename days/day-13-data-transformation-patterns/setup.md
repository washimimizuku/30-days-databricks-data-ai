# Day 13 Setup: Data Transformation Patterns

## Prerequisites

- Completed Days 1-12
- Active Databricks cluster
- Workspace access
- Understanding of DataFrame operations

## Setup Steps

### Step 1: Create Notebook

1. Navigate to **Workspace** in Databricks
2. Click **Create** → **Notebook**
3. Name: `Day-13-Data-Transformation-Patterns`
4. Language: **Python**
5. Select your cluster
6. Click **Create**

### Step 2: Create Directories

```python
# Create directories for Day 13
dbutils.fs.mkdirs("/FileStore/day13-transformation/")
print("Directory created!")
```

### Step 3: Create Messy Sample Data

```python
# Create messy data with various data quality issues
messy_data = """id,name,email,phone,amount,date,status
1,  Alice Johnson  ,ALICE@EMAIL.COM,(555) 123-4567,$1,500.50,2024-01-15,active
2,Bob Smith,bob@email,555-234-5678,2300.75,01/16/2024,Active
3,Charlie Brown,charlie@email.com,5553456789,invalid,2024-01-17,ACTIVE
4,  Diana Prince,diana@email.com,555.456.7890,2100.25,2024/01/18,pending
5,Eve Davis,eve@EMAIL.COM,555-567-8901,$1,950,15-01-2024,completed
6,Frank Miller,frank@email.com,555-678-9012,2500.00,2024-01-20,active
7,  Grace Lee  ,grace@email,555 789 0123,1700.50%,2024-01-21,PENDING
8,Henry Wilson,henry@email.com,5558901234,2200.75,2024-01-22,active
9,Ivy Chen,ivy@email.com,555-901-2345,NULL,2024-01-23,cancelled
10,Jack Taylor,jack@email.com,555-012-3456,2400.25,2024-01-24,active
11,Alice Johnson,alice@email.com,555-123-4567,1500.50,2024-01-15,active
12,,missing@email.com,555-234-5678,1800.00,2024-01-25,active
13,Mia Anderson,mia@email,555-345-6789,-500.00,2024-01-26,active
14,Noah Davis,noah@email.com,invalid-phone,2100.00,invalid-date,pending
15,Olivia Wilson,olivia@email.com,555-567-8901,999999.99,2024-01-28,active"""

with open("/dbfs/FileStore/day13-transformation/messy_data.csv", "w") as f:
    f.write(messy_data)

print("Messy data created!")
```

### Step 4: Create Data with Nulls

```python
# Create data with various null representations
null_data = """id,name,email,amount,category
1,Alice,alice@email.com,1000.00,A
2,Bob,,2000.00,B
3,Charlie,charlie@email.com,NULL,A
4,Diana,diana@email.com,3000.00,
5,,eve@email.com,4000.00,C
6,Frank,frank@email.com,NA,B
7,Grace,grace@email.com,5000.00,null
8,Henry,,6000.00,A
9,Ivy,ivy@email.com,N/A,B
10,Jack,jack@email.com,7000.00,None"""

with open("/dbfs/FileStore/day13-transformation/null_data.csv", "w") as f:
    f.write(null_data)

print("Null data created!")
```

### Step 5: Create Date Format Variations

```python
# Create data with various date formats
date_data = """id,name,date1,date2,date3,timestamp
1,Alice,2024-01-15,01/15/2024,15-Jan-2024,2024-01-15 10:30:00
2,Bob,2024-01-16,01/16/2024,16-Jan-2024,2024-01-16 14:45:30
3,Charlie,2024-01-17,01/17/2024,17-Jan-2024,2024-01-17 09:15:45
4,Diana,2024-01-18,01/18/2024,18-Jan-2024,2024-01-18 16:20:10
5,Eve,2024-01-19,01/19/2024,19-Jan-2024,2024-01-19 11:05:25"""

with open("/dbfs/FileStore/day13-transformation/date_data.csv", "w") as f:
    f.write(date_data)

print("Date data created!")
```

### Step 6: Create Duplicate Data

```python
# Create data with duplicates
duplicate_data = """id,email,name,amount,timestamp
1,alice@email.com,Alice Johnson,1000.00,2024-01-15 10:00:00
2,bob@email.com,Bob Smith,2000.00,2024-01-15 11:00:00
3,alice@email.com,Alice Johnson,1000.00,2024-01-15 12:00:00
4,charlie@email.com,Charlie Brown,3000.00,2024-01-15 13:00:00
5,bob@email.com,Bob Smith,2000.00,2024-01-15 14:00:00
6,alice@email.com,Alice J.,1000.00,2024-01-15 15:00:00
7,diana@email.com,Diana Prince,4000.00,2024-01-15 16:00:00
8,charlie@email.com,Charlie Brown,3000.00,2024-01-15 17:00:00"""

with open("/dbfs/FileStore/day13-transformation/duplicate_data.csv", "w") as f:
    f.write(duplicate_data)

print("Duplicate data created!")
```

### Step 7: Create Database and Tables

```python
# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS day13_db")
spark.sql("USE day13_db")

# Create target table for cleaned data
spark.sql("""
CREATE TABLE IF NOT EXISTS day13_db.cleaned_data (
    id INT,
    name STRING,
    email STRING,
    phone STRING,
    amount DOUBLE,
    date DATE,
    status STRING,
    processed_at TIMESTAMP
) USING DELTA
""")

print("Database and tables created!")
```

### Step 8: Load Sample DataFrames

```python
# Load messy data
df_messy = spark.read.csv(
    "/FileStore/day13-transformation/messy_data.csv",
    header=True,
    inferSchema=True
)

# Load null data
df_nulls = spark.read.csv(
    "/FileStore/day13-transformation/null_data.csv",
    header=True,
    inferSchema=True
)

# Load date data
df_dates = spark.read.csv(
    "/FileStore/day13-transformation/date_data.csv",
    header=True,
    inferSchema=True
)

# Load duplicate data
df_duplicates = spark.read.csv(
    "/FileStore/day13-transformation/duplicate_data.csv",
    header=True,
    inferSchema=True
)

print("Sample DataFrames loaded!")
print(f"Messy data: {df_messy.count()} rows")
print(f"Null data: {df_nulls.count()} rows")
print(f"Date data: {df_dates.count()} rows")
print(f"Duplicate data: {df_duplicates.count()} rows")
```

## Verification Checklist

Run this verification code:

```python
# Verification script
checks = []

# Check 1: Directory exists
try:
    dbutils.fs.ls("/FileStore/day13-transformation/")
    checks.append(("✅", "Directory exists"))
except:
    checks.append(("❌", "Directory NOT found"))

# Check 2: Files exist
required_files = ["messy_data.csv", "null_data.csv", "date_data.csv", "duplicate_data.csv"]
try:
    files = [f.name for f in dbutils.fs.ls("/FileStore/day13-transformation/")]
    for req_file in required_files:
        if req_file in files:
            checks.append(("✅", f"File {req_file} exists"))
        else:
            checks.append(("❌", f"File {req_file} NOT found"))
except Exception as e:
    checks.append(("❌", f"Cannot list files: {str(e)}"))

# Check 3: Database exists
databases = [db.database for db in spark.sql("SHOW DATABASES").collect()]
if "day13_db" in databases:
    checks.append(("✅", "Database day13_db exists"))
else:
    checks.append(("❌", "Database day13_db NOT found"))

# Check 4: Can read files
try:
    df = spark.read.csv("/FileStore/day13-transformation/messy_data.csv", header=True)
    checks.append(("✅", f"Can read messy_data.csv ({df.count()} rows)"))
except Exception as e:
    checks.append(("❌", f"Cannot read files: {str(e)}"))

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
    print("\n🎉 All checks passed! You're ready for Day 13 exercises!")
else:
    print("\n⚠️  Some checks failed. Please review the setup steps above.")
```

## Troubleshooting

### Issue: Files not created

**Solution:**
```python
# Recreate files
dbutils.fs.rm("/FileStore/day13-transformation/", recurse=True)
dbutils.fs.mkdirs("/FileStore/day13-transformation/")
# Then run steps 3-6 again
```

### Issue: Cannot read CSV

**Solution:**
```python
# Check file contents
print(dbutils.fs.head("/FileStore/day13-transformation/messy_data.csv"))
```

### Issue: Type conversion errors

**Solution:**
- Use inferSchema=False and handle types manually
- Check for invalid values in data
- Use try-except in transformations

## Quick Reset

```python
# Complete reset
print("Resetting Day 13 environment...")

# Remove files
try:
    dbutils.fs.rm("/FileStore/day13-transformation/", recurse=True)
    print("✅ Removed old files")
except:
    print("⚠️  No files to remove")

# Drop database
spark.sql("DROP DATABASE IF EXISTS day13_db CASCADE")
print("✅ Dropped database")

# Recreate
dbutils.fs.mkdirs("/FileStore/day13-transformation/")
spark.sql("CREATE DATABASE day13_db")
print("✅ Reset complete! Run setup steps 3-7 to recreate data.")
```

## What's Next?

You're ready for Day 13 exercises! Practice data cleaning and transformation techniques.

**Tips for Success:**
1. Always inspect data before transforming
2. Handle edge cases (nulls, empty strings, invalid formats)
3. Test transformations with sample data
4. Use display() to verify results
5. Build reusable transformation functions
6. Document your transformation logic

Good luck! 🚀
