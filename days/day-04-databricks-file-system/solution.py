# Day 4: Databricks File System (DBFS) - Solutions
# =========================================================

# =========================================================
# PART 1: EXPLORING DBFS
# =========================================================

# Exercise 1: List Root Directories
display(dbutils.fs.ls("/"))

# Exercise 2: Explore Sample Datasets
display(dbutils.fs.ls("/databricks-datasets/"))

# Exercise 3: Find Specific Dataset
display(dbutils.fs.ls("/databricks-datasets/samples/"))

# Exercise 4: File Information
files = dbutils.fs.ls("/databricks-datasets/samples/")
for file in files:
    print(f"Name: {file.name}")
    print(f"Path: {file.path}")
    print(f"Size: {file.size} bytes ({file.size / 1024 / 1024:.2f} MB)")
    print(f"Modified: {file.modificationTime}")
    print("-" * 50)

# Exercise 5: Count Files
files = dbutils.fs.ls("/FileStore/day4-exercises/")
file_count = len([f for f in files if not f.isDir()])
print(f"Number of files: {file_count}")


# =========================================================
# PART 2: FILE OPERATIONS
# =========================================================

# Exercise 6: Create Directory
dbutils.fs.mkdirs("/FileStore/my-workspace/")
print("Directory created!")

# Exercise 7: Create Nested Directories
dbutils.fs.mkdirs("/FileStore/projects/2024/data/")
print("Nested directories created!")

# Exercise 8: Copy File
dbutils.fs.cp(
    "/FileStore/day4-exercises/employees.csv",
    "/FileStore/my-workspace/employees.csv"
)
print("File copied!")

# Exercise 9: Move File
dbutils.fs.mv(
    "/FileStore/day4-exercises/employees_backup.csv",
    "/FileStore/my-workspace/employees_backup.csv"
)
print("File moved!")

# Exercise 10: Rename File
dbutils.fs.mv(
    "/FileStore/my-workspace/employees.csv",
    "/FileStore/my-workspace/staff.csv"
)
print("File renamed!")


# =========================================================
# PART 3: READING FILES
# =========================================================

# Exercise 11: Read File Head
content = dbutils.fs.head("/FileStore/day4-exercises/employees.csv", 500)
print(content)

# Exercise 12: Read Entire File
with open("/dbfs/FileStore/day4-exercises/employees.csv", "r") as f:
    content = f.read()
    print(content)

# Exercise 13: Read CSV with Spark
df_csv = spark.read.csv(
    "/FileStore/day4-exercises/employees.csv",
    header=True,
    inferSchema=True
)
display(df_csv)

# Exercise 14: Read JSON with Spark
df_json = spark.read.json("/FileStore/day4-exercises/products.json")
display(df_json)

# Exercise 15: Read Multiple Files
df_all_csv = spark.read.csv(
    "/FileStore/day4-exercises/*.csv",
    header=True,
    inferSchema=True
)
display(df_all_csv)


# =========================================================
# PART 4: WRITING FILES
# =========================================================

# Exercise 16: Write Text File
from datetime import datetime

text_content = f"Name: Your Name\nDate: {datetime.now().strftime('%Y-%m-%d')}"
with open("/dbfs/FileStore/my-workspace/info.txt", "w") as f:
    f.write(text_content)
print("Text file created!")

# Exercise 17: Write CSV File
from pyspark.sql import Row

data = [
    Row(id=1, name="Alice", age=30),
    Row(id=2, name="Bob", age=25),
    Row(id=3, name="Charlie", age=35)
]
df = spark.createDataFrame(data)
df.write.csv(
    "/FileStore/my-workspace/output.csv",
    header=True,
    mode="overwrite"
)
print("CSV file created!")

# Exercise 18: Write JSON File
df.write.json(
    "/FileStore/my-workspace/output.json",
    mode="overwrite"
)
print("JSON file created!")

# Exercise 19: Write Parquet File
df.write.parquet(
    "/FileStore/my-workspace/output.parquet",
    mode="overwrite"
)
print("Parquet file created!")

# Exercise 20: Write Delta Table
df.write.format("delta").save(
    "/FileStore/my-workspace/delta/people",
    mode="overwrite"
)
print("Delta table created!")


# =========================================================
# PART 5: FILE SYSTEM UTILITIES
# =========================================================

# Exercise 21: Check if File Exists
def file_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False

# Test
print(file_exists("/FileStore/day4-exercises/employees.csv"))  # True
print(file_exists("/FileStore/nonexistent.csv"))  # False

# Exercise 22: Get File Size
def get_file_size_mb(path):
    try:
        files = dbutils.fs.ls(path)
        if files and not files[0].isDir():
            size_bytes = files[0].size
            size_mb = size_bytes / (1024 * 1024)
            return round(size_mb, 2)
        return 0
    except Exception as e:
        return f"Error: {str(e)}"

# Test
print(f"File size: {get_file_size_mb('/FileStore/day4-exercises/employees.csv')} MB")

# Exercise 23: List Files Recursively
def list_files_recursive(path, indent=0):
    try:
        items = dbutils.fs.ls(path)
        for item in items:
            print("  " * indent + f"{'[DIR]' if item.isDir() else '[FILE]'} {item.name}")
            if item.isDir():
                list_files_recursive(item.path, indent + 1)
    except Exception as e:
        print(f"Error: {str(e)}")

# Test
list_files_recursive("/FileStore/my-workspace/")

# Exercise 24: Find Files by Extension
def find_files_by_extension(path, extension):
    try:
        files = dbutils.fs.ls(path)
        matching_files = [f.path for f in files if f.name.endswith(extension) and not f.isDir()]
        return matching_files
    except Exception as e:
        return []

# Test
csv_files = find_files_by_extension("/FileStore/day4-exercises/", ".csv")
print(f"CSV files found: {csv_files}")

# Exercise 25: Calculate Directory Size
def calculate_directory_size(path):
    try:
        total_size = 0
        items = dbutils.fs.ls(path)
        for item in items:
            if not item.isDir():
                total_size += item.size
            else:
                total_size += calculate_directory_size(item.path)
        return total_size
    except Exception:
        return 0

# Test
size_bytes = calculate_directory_size("/FileStore/day4-exercises/")
size_mb = size_bytes / (1024 * 1024)
print(f"Directory size: {size_mb:.2f} MB")


# =========================================================
# PART 6: WORKING WITH FILESTORE
# =========================================================

# Exercise 26: Upload to FileStore
sample_data = [
    {"id": 1, "product": "Laptop", "price": 999.99},
    {"id": 2, "product": "Mouse", "price": 29.99},
    {"id": 3, "product": "Keyboard", "price": 79.99}
]

df_products = spark.createDataFrame(sample_data)
df_products.write.csv(
    "/FileStore/my-products.csv",
    header=True,
    mode="overwrite"
)
print("Uploaded to FileStore!")

# Exercise 27: Generate Public URL
def generate_public_url(dbfs_path, databricks_instance):
    # Remove /FileStore/ prefix and construct URL
    if "/FileStore/" in dbfs_path:
        file_path = dbfs_path.split("/FileStore/")[1]
        return f"https://{databricks_instance}/files/{file_path}"
    return None

# Example (replace with your instance)
url = generate_public_url(
    "/FileStore/my-products.csv",
    "your-workspace.cloud.databricks.com"
)
print(f"Public URL: {url}")

# Exercise 28: Download from FileStore
content = dbutils.fs.head("/FileStore/my-products.csv")
print("File contents:")
print(content)


# =========================================================
# PART 7: PATH OPERATIONS
# =========================================================

# Exercise 29: Convert DBFS Path to Local Path
def dbfs_to_local(dbfs_path):
    if dbfs_path.startswith("dbfs:/"):
        return dbfs_path.replace("dbfs:", "/dbfs")
    elif dbfs_path.startswith("/"):
        return "/dbfs" + dbfs_path
    return dbfs_path

# Test
print(dbfs_to_local("dbfs:/FileStore/my-file.csv"))
# Output: /dbfs/FileStore/my-file.csv

# Exercise 30: Convert Local Path to DBFS Path
def local_to_dbfs(local_path):
    if local_path.startswith("/dbfs/"):
        return "dbfs:" + local_path.replace("/dbfs", "")
    return local_path

# Test
print(local_to_dbfs("/dbfs/FileStore/my-file.csv"))
# Output: dbfs:/FileStore/my-file.csv

# Exercise 31: Extract Filename from Path
def get_filename(path):
    return path.split("/")[-1]

# Test
print(get_filename("/FileStore/day4-exercises/employees.csv"))
# Output: employees.csv

# Exercise 32: Get Parent Directory
def get_parent_directory(path):
    parts = path.rstrip("/").split("/")
    return "/".join(parts[:-1]) if len(parts) > 1 else "/"

# Test
print(get_parent_directory("/FileStore/day4-exercises/employees.csv"))
# Output: /FileStore/day4-exercises


# =========================================================
# PART 8: DATA PROCESSING PIPELINE
# =========================================================

# Exercise 33: Build ETL Pipeline
# 1. Read CSV
df_input = spark.read.csv(
    "/FileStore/day4-exercises/employees.csv",
    header=True,
    inferSchema=True
)

# 2. Transform (add bonus column)
from pyspark.sql.functions import col, when

df_transformed = df_input.withColumn(
    "bonus",
    when(col("salary") > 90000, col("salary") * 0.10)
    .otherwise(col("salary") * 0.05)
)

# 3. Write as Parquet
df_transformed.write.parquet(
    "/FileStore/my-workspace/employees_with_bonus.parquet",
    mode="overwrite"
)
print("ETL pipeline completed!")

# Exercise 34: Process Multiple Files
# Read all CSV files
df_combined = spark.read.csv(
    "/FileStore/day4-exercises/*.csv",
    header=True,
    inferSchema=True
)

# Write as single Delta table
df_combined.write.format("delta").save(
    "/FileStore/my-workspace/delta/combined_data",
    mode="overwrite"
)
print("Multiple files combined into Delta table!")

# Exercise 35: File Format Conversion
# Get all CSV files
csv_files = find_files_by_extension("/FileStore/day4-exercises/", ".csv")

for csv_file in csv_files:
    # Read CSV
    df = spark.read.csv(csv_file, header=True, inferSchema=True)
    
    # Generate Parquet filename
    parquet_file = csv_file.replace(".csv", ".parquet")
    
    # Write as Parquet
    df.write.parquet(parquet_file, mode="overwrite")
    print(f"Converted {csv_file} to {parquet_file}")


# =========================================================
# PART 9: CLEANUP OPERATIONS
# =========================================================

# Exercise 36: Remove Single File
dbutils.fs.rm("/FileStore/my-workspace/info.txt")
print("File removed!")

# Exercise 37: Remove Directory
dbutils.fs.rm("/FileStore/my-workspace/", recurse=True)
print("Directory removed!")

# Exercise 38: Clean Temporary Files
# Simulation (in practice, check modification time)
try:
    temp_files = dbutils.fs.ls("/tmp/")
    for file in temp_files:
        if not file.isDir():
            # In real scenario, check if older than 1 day
            # dbutils.fs.rm(file.path)
            print(f"Would remove: {file.path}")
except Exception:
    print("No temp files to clean")


# =========================================================
# PART 10: ADVANCED SCENARIOS
# =========================================================

# Exercise 39: Backup Files
# Create backup directory
dbutils.fs.mkdirs("/FileStore/backup/")

# Copy all files
source_files = dbutils.fs.ls("/FileStore/day4-exercises/")
for file in source_files:
    if not file.isDir():
        dest_path = file.path.replace("/day4-exercises/", "/backup/")
        dbutils.fs.cp(file.path, dest_path)
        print(f"Backed up: {file.name}")

# Exercise 40: File Comparison
def compare_csv_files(file1, file2):
    df1 = spark.read.csv(file1, header=True, inferSchema=True)
    df2 = spark.read.csv(file2, header=True, inferSchema=True)
    
    # Compare row counts
    count1 = df1.count()
    count2 = df2.count()
    
    print(f"File 1 rows: {count1}")
    print(f"File 2 rows: {count2}")
    
    # Find differences
    if count1 == count2:
        # Check for content differences
        diff = df1.subtract(df2)
        diff_count = diff.count()
        if diff_count == 0:
            print("Files are identical!")
        else:
            print(f"Found {diff_count} different rows")
            display(diff)
    else:
        print("Files have different row counts")

# Test
compare_csv_files(
    "/FileStore/day4-exercises/employees.csv",
    "/FileStore/backup/employees.csv"
)


# =========================================================
# REFLECTION QUESTIONS - ANSWERS
# =========================================================

"""
1. What is the difference between dbfs:/ and /dbfs/ paths?
   - dbfs:/ is the DBFS path format used with dbutils.fs and Spark APIs
   - /dbfs/ is the local file system path used within cluster for Python file operations
   - Example: dbutils.fs.ls("dbfs:/FileStore/") vs open("/dbfs/FileStore/file.txt")

2. When should you use FileStore vs other DBFS locations?
   - FileStore: Small files, web-accessible files, quick uploads
   - /tmp/: Temporary files that can be deleted
   - /mnt/: Mounted cloud storage for large datasets
   - /user/hive/warehouse/: Managed tables (automatic)

3. What are the benefits of using mount points?
   - Simplified paths (use /mnt/ instead of full cloud URLs)
   - Centralized credential management
   - Consistent access across notebooks
   - Better security (credentials not in code)
   - Easier to change storage locations

4. How does DBFS relate to cloud storage?
   - DBFS is an abstraction layer over cloud object storage
   - Provides file system semantics for object storage
   - Data stored in S3 (AWS), ADLS (Azure), or GCS (Google Cloud)
   - DBFS makes cloud storage look like a file system

5. What happens to files in /tmp/ when cluster terminates?
   - Files in /tmp/ may be deleted when cluster terminates
   - Not guaranteed to persist
   - Use for truly temporary data only
   - For persistent data, use other DBFS locations

6. How do you make a file publicly accessible?
   - Upload to /FileStore/
   - Access via: https://<databricks-instance>/files/<path-after-FileStore>
   - Example: /FileStore/my-file.csv → https://workspace.databricks.com/files/my-file.csv

7. What is the default location for managed tables?
   - /user/hive/warehouse/
   - Format: /user/hive/warehouse/<database>.db/<table>/
   - Databricks manages both metadata and data
   - DROP TABLE deletes both metadata and data

8. How do you recursively copy a directory?
   - dbutils.fs.cp("/source/", "/dest/", recurse=True)
   - The recurse=True parameter copies all subdirectories and files

9. What file formats can Spark read from DBFS?
   - CSV, JSON, Parquet, Delta, Avro, ORC, Text
   - Also: Images, binary files, XML (with libraries)
   - Delta Lake is recommended for structured data

10. Why is Delta Lake preferred over raw files?
    - ACID transactions
    - Time travel
    - Schema evolution
    - Better performance (data skipping, Z-ordering)
    - Audit trail
    - Concurrent reads/writes
"""


# =========================================================
# BONUS CHALLENGES - SOLUTIONS
# =========================================================

# Bonus 1: File Monitoring System
def monitor_directory(path):
    try:
        files = dbutils.fs.ls(path)
        
        total_files = 0
        total_size = 0
        file_types = {}
        largest_file = None
        largest_size = 0
        
        for file in files:
            if not file.isDir():
                total_files += 1
                total_size += file.size
                
                # Track file types
                ext = file.name.split(".")[-1] if "." in file.name else "no_extension"
                file_types[ext] = file_types.get(ext, 0) + 1
                
                # Track largest file
                if file.size > largest_size:
                    largest_size = file.size
                    largest_file = file.name
        
        print(f"Directory: {path}")
        print(f"Total files: {total_files}")
        print(f"Total size: {total_size / (1024 * 1024):.2f} MB")
        print(f"File types: {file_types}")
        print(f"Largest file: {largest_file} ({largest_size / 1024:.2f} KB)")
        
    except Exception as e:
        print(f"Error: {str(e)}")

# Test
monitor_directory("/FileStore/day4-exercises/")

# Bonus 2: Automated Backup System
from datetime import datetime

def backup_with_timestamp(source_path, backup_path):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        files = dbutils.fs.ls(source_path)
        dbutils.fs.mkdirs(backup_path)
        
        for file in files:
            if not file.isDir():
                filename = file.name
                name, ext = filename.rsplit(".", 1) if "." in filename else (filename, "")
                new_filename = f"{name}_{timestamp}.{ext}" if ext else f"{name}_{timestamp}"
                
                dest = f"{backup_path}/{new_filename}"
                dbutils.fs.cp(file.path, dest)
                print(f"Backed up: {filename} → {new_filename}")
                
    except Exception as e:
        print(f"Error: {str(e)}")

# Test
backup_with_timestamp("/FileStore/day4-exercises/", "/FileStore/backup_timestamped/")

# Bonus 3: File Organization System
def organize_by_extension(source_path):
    try:
        files = dbutils.fs.ls(source_path)
        
        for file in files:
            if not file.isDir():
                ext = file.name.split(".")[-1] if "." in file.name else "other"
                
                # Create directory for extension
                ext_dir = f"{source_path}/{ext}_files"
                dbutils.fs.mkdirs(ext_dir)
                
                # Move file
                dest = f"{ext_dir}/{file.name}"
                dbutils.fs.mv(file.path, dest)
                print(f"Moved {file.name} to {ext}_files/")
                
    except Exception as e:
        print(f"Error: {str(e)}")

# Test (be careful, this moves files!)
# organize_by_extension("/FileStore/test-organize/")

# Bonus 4: Data Lake Structure
def create_data_lake_structure(base_path):
    layers = ["bronze", "silver", "gold"]
    zones = ["raw", "processed", "curated"]
    
    for layer in layers:
        layer_path = f"{base_path}/{layer}"
        dbutils.fs.mkdirs(layer_path)
        print(f"Created: {layer_path}")
        
        # Create subdirectories
        for zone in ["data", "checkpoints", "schemas"]:
            sub_path = f"{layer_path}/{zone}"
            dbutils.fs.mkdirs(sub_path)
            print(f"  Created: {sub_path}")

# Test
create_data_lake_structure("/FileStore/data-lake")

# Bonus 5: File Validation System
def validate_csv_file(file_path, expected_columns):
    try:
        # Read CSV
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        
        # Check columns
        actual_columns = df.columns
        missing_columns = set(expected_columns) - set(actual_columns)
        extra_columns = set(actual_columns) - set(expected_columns)
        
        # Check row count
        row_count = df.count()
        
        # Report
        print(f"File: {file_path}")
        print(f"Row count: {row_count}")
        print(f"Columns: {actual_columns}")
        
        if missing_columns:
            print(f"❌ Missing columns: {missing_columns}")
            return False
        if extra_columns:
            print(f"⚠️  Extra columns: {extra_columns}")
        if row_count == 0:
            print(f"❌ File is empty!")
            return False
            
        print("✅ Validation passed!")
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

# Test
validate_csv_file(
    "/FileStore/day4-exercises/employees.csv",
    ["id", "name", "department", "salary"]
)


# =========================================================
# KEY LEARNINGS SUMMARY
# =========================================================

"""
## Day 4 Key Learnings

### DBFS Fundamentals
1. DBFS is an abstraction layer over cloud object storage
2. Provides file system semantics for object storage
3. Data persists beyond cluster lifecycle
4. Accessible via dbutils, Spark, and local file paths

### Path Formats
1. dbfs:/ - DBFS path (for dbutils.fs and Spark)
2. /dbfs/ - Local file system path (for Python file operations)
3. Always use correct format for the operation

### dbutils.fs Commands
1. ls() - List files
2. cp() - Copy files (use recurse=True for directories)
3. mv() - Move/rename files
4. rm() - Remove files (use recurse=True for directories)
5. mkdirs() - Create directories
6. head() - Read file preview

### FileStore
1. Special location for user uploads
2. Web-accessible via public URLs
3. Good for small files and quick uploads
4. Path: /FileStore/

### Best Practices
1. Use Delta Lake for structured data
2. Organize with clear directory structure
3. Use mount points for cloud storage
4. Clean up temporary files regularly
5. Use appropriate locations (/tmp/ vs /FileStore/ vs /mnt/)
6. Avoid hardcoding credentials

**Next**: Day 5 - Databases, Tables, and Views
"""
