# Day 4 Setup: Databricks File System (DBFS)

## Prerequisites

- Completed Days 1-3
- Active Databricks cluster
- Workspace access
- Basic Python knowledge

## Setup Steps

### Step 1: Verify Cluster and Create Notebook

1. Navigate to **Compute** and ensure your cluster is running
2. Go to **Workspace** → Your user folder → `30-days-bootcamp`
3. Create new notebook:
   - Name: `Day 4 - DBFS`
   - Language: **Python** (we'll use Python for dbutils)
   - Cluster: Select your `learning-cluster`
4. Click **Create**

### Step 2: Verify dbutils Access

Run this in your notebook to verify dbutils is available:

```python
# Check dbutils is available
print("dbutils is available!")
print(f"dbutils.fs commands: {dir(dbutils.fs)}")

# List available dbutils modules
print("\nAvailable dbutils modules:")
print("- dbutils.fs: File system utilities")
print("- dbutils.notebook: Notebook utilities")
print("- dbutils.widgets: Widget utilities")
print("- dbutils.secrets: Secret management")
```

Expected output: List of dbutils.fs commands (ls, cp, mv, rm, etc.)

### Step 3: Explore DBFS Root

```python
# List root DBFS directories
display(dbutils.fs.ls("/"))

# Common directories you'll see:
# - databricks-datasets/
# - FileStore/
# - tmp/
# - user/
```

### Step 4: Explore Sample Datasets

```python
# List Databricks sample datasets
display(dbutils.fs.ls("/databricks-datasets/"))

# Explore a specific dataset
display(dbutils.fs.ls("/databricks-datasets/samples/"))

# View file details
files = dbutils.fs.ls("/databricks-datasets/samples/")
for file in files:
    print(f"Name: {file.name}, Size: {file.size} bytes")
```

### Step 5: Create Working Directory

```python
# Create a directory for Day 4 exercises
dbutils.fs.mkdirs("/FileStore/day4-exercises/")

# Verify creation
display(dbutils.fs.ls("/FileStore/"))
```

### Step 6: Create Sample Files

```python
# Create a sample CSV file
sample_data = """id,name,department,salary
1,Alice,Engineering,95000
2,Bob,Sales,75000
3,Charlie,Engineering,105000
4,Diana,Marketing,85000
5,Eve,Engineering,98000"""

# Write to DBFS using local file system access
with open("/dbfs/FileStore/day4-exercises/employees.csv", "w") as f:
    f.write(sample_data)

# Verify file was created
display(dbutils.fs.ls("/FileStore/day4-exercises/"))

# Read file to verify content
print(dbutils.fs.head("/FileStore/day4-exercises/employees.csv"))
```

### Step 7: Create Sample JSON File

```python
import json

# Create sample JSON data
json_data = [
    {"product_id": 1, "product_name": "Laptop", "price": 999.99},
    {"product_id": 2, "product_name": "Mouse", "price": 29.99},
    {"product_id": 3, "product_name": "Keyboard", "price": 79.99}
]

# Write to DBFS
with open("/dbfs/FileStore/day4-exercises/products.json", "w") as f:
    for item in json_data:
        f.write(json.dumps(item) + "\n")

# Verify
print(dbutils.fs.head("/FileStore/day4-exercises/products.json"))
```

### Step 8: Test File Operations

```python
# Test copy
dbutils.fs.cp(
    "/FileStore/day4-exercises/employees.csv",
    "/FileStore/day4-exercises/employees_backup.csv"
)

# Test list
display(dbutils.fs.ls("/FileStore/day4-exercises/"))

# Test file info
files = dbutils.fs.ls("/FileStore/day4-exercises/")
for file in files:
    print(f"File: {file.name}")
    print(f"  Path: {file.path}")
    print(f"  Size: {file.size} bytes")
    print(f"  Modified: {file.modificationTime}")
    print()
```

### Step 9: Test Reading with Spark

```python
# Read CSV into DataFrame
df_csv = spark.read.csv(
    "/FileStore/day4-exercises/employees.csv",
    header=True,
    inferSchema=True
)

display(df_csv)

# Read JSON into DataFrame
df_json = spark.read.json("/FileStore/day4-exercises/products.json")

display(df_json)
```

### Step 10: Explore Default Table Location

```python
# Check default warehouse location
warehouse_location = spark.conf.get("spark.sql.warehouse.dir")
print(f"Default warehouse location: {warehouse_location}")

# List tables directory
try:
    display(dbutils.fs.ls("/user/hive/warehouse/"))
except Exception as e:
    print("Warehouse directory may not exist yet (no tables created)")

# If you created tables in Day 3, you'll see them here
```

## Verification Checklist

- [ ] Cluster is running
- [ ] Day 4 notebook created with Python as default language
- [ ] dbutils.fs commands are accessible
- [ ] Can list DBFS root directories
- [ ] Created `/FileStore/day4-exercises/` directory
- [ ] Created sample CSV file
- [ ] Created sample JSON file
- [ ] Successfully copied files
- [ ] Can read files with Spark
- [ ] Explored sample datasets

## Troubleshooting

### Issue: dbutils not found
**Solution**:
- Ensure you're running code in a Databricks notebook (not local Python)
- Verify cluster is attached to notebook
- Restart cluster if needed

### Issue: Permission denied when writing files
**Solution**:
```python
# Use FileStore which is accessible to all users
dbutils.fs.mkdirs("/FileStore/your-folder/")

# Or use tmp directory
dbutils.fs.mkdirs("/tmp/your-folder/")
```

### Issue: Can't see uploaded files
**Solution**:
```python
# Refresh file system
dbutils.fs.ls("/FileStore/")

# Check exact path
display(dbutils.fs.ls("/FileStore/"))
```

### Issue: File already exists error
**Solution**:
```python
# Remove existing file first
dbutils.fs.rm("/path/to/file", recurse=True)

# Then create new file
```

### Issue: Can't access /dbfs/ path
**Solution**:
- `/dbfs/` paths only work within cluster nodes
- Use `dbfs:/` paths for dbutils.fs commands
- Use `/dbfs/` paths for Python file operations

## Understanding DBFS Paths

### Path Formats

```python
# DBFS path (for dbutils.fs and Spark)
"dbfs:/FileStore/my-file.csv"
"/FileStore/my-file.csv"  # dbfs:/ is implied

# Local file system path (for Python file operations)
"/dbfs/FileStore/my-file.csv"

# Examples:
# ✅ Correct
dbutils.fs.ls("/FileStore/")
dbutils.fs.ls("dbfs:/FileStore/")
spark.read.csv("/FileStore/my-file.csv")

# ✅ Correct for Python file operations
with open("/dbfs/FileStore/my-file.csv", "r") as f:
    content = f.read()

# ❌ Incorrect
dbutils.fs.ls("/dbfs/FileStore/")  # Don't use /dbfs/ with dbutils
```

### Common DBFS Locations

```python
# Sample datasets (read-only)
"/databricks-datasets/"

# User uploads (web accessible)
"/FileStore/"

# Temporary files (may be deleted)
"/tmp/"

# Default table location
"/user/hive/warehouse/"

# Mount points (if configured)
"/mnt/"
```

## Quick Reference Commands

```python
# List files
dbutils.fs.ls("/path/")
display(dbutils.fs.ls("/path/"))

# Copy file
dbutils.fs.cp("/source", "/dest")
dbutils.fs.cp("/source", "/dest", recurse=True)

# Move file
dbutils.fs.mv("/old/path", "/new/path")

# Remove file
dbutils.fs.rm("/path/to/file")
dbutils.fs.rm("/path/to/dir", recurse=True)

# Create directory
dbutils.fs.mkdirs("/path/to/dir")

# Read file (first 65536 bytes)
dbutils.fs.head("/path/to/file")
dbutils.fs.head("/path/to/file", 1000)

# File info
files = dbutils.fs.ls("/path/")
for f in files:
    print(f.name, f.size, f.modificationTime)
```

## Sample Data for Exercises

The setup created these files in `/FileStore/day4-exercises/`:

1. **employees.csv** - Employee data (5 rows)
2. **products.json** - Product data (3 items)
3. **employees_backup.csv** - Copy of employees.csv

You can access these files:
```python
# Via Spark
df = spark.read.csv("/FileStore/day4-exercises/employees.csv", header=True)

# Via dbutils
content = dbutils.fs.head("/FileStore/day4-exercises/employees.csv")

# Via Python (local file system)
with open("/dbfs/FileStore/day4-exercises/employees.csv", "r") as f:
    data = f.read()
```

## What's Next?

You're now ready for Day 4 exercises! You'll practice:
- Navigating DBFS with dbutils.fs
- File operations (copy, move, delete)
- Reading and writing various file formats
- Working with FileStore
- Understanding path formats
- Managing directories

**Tip**: Keep the Quick Reference Commands handy while doing exercises!
