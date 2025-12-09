# Day 4: Databricks File System (DBFS)

## Learning Objectives

By the end of today, you will:
- Understand DBFS architecture and structure
- Navigate and manage files using dbutils.fs
- Upload and download files to/from DBFS
- Understand DBFS paths and mounts
- Work with FileStore for file uploads
- Integrate DBFS with cloud storage (S3, ADLS, GCS)

## Topics Covered

### 1. What is DBFS?

**Definition**: Databricks File System (DBFS) is a distributed file system mounted into a Databricks workspace that allows you to interact with object storage using file system semantics.

**Key Features**:
- **Abstraction Layer**: Unified interface for cloud storage
- **Distributed**: Scales across cluster nodes
- **Persistent**: Data persists beyond cluster lifecycle
- **Accessible**: Via dbutils, Spark APIs, and local file paths
- **Integrated**: Works seamlessly with Delta Lake and Spark

**Architecture**:
```
DBFS
├── /databricks-datasets/     (Sample datasets)
├── /FileStore/                (User uploads, public access)
├── /mnt/                      (Mount points for cloud storage)
├── /tmp/                      (Temporary files)
├── /user/hive/warehouse/      (Default table location)
└── /...                       (Custom directories)
```

### 2. DBFS Paths

**DBFS Path Formats**:
```python
# DBFS path (Databricks notation)
"dbfs:/path/to/file"

# Local file path (within cluster)
"/dbfs/path/to/file"

# Relative path (from current directory)
"path/to/file"
```

**Common DBFS Locations**:
- `dbfs:/databricks-datasets/` - Sample datasets provided by Databricks
- `dbfs:/FileStore/` - User-uploaded files (accessible via web)
- `dbfs:/mnt/` - Mounted cloud storage
- `dbfs:/tmp/` - Temporary files (may be deleted)
- `dbfs:/user/hive/warehouse/` - Default managed table location

### 3. dbutils.fs Commands

**List Files**:
```python
# List files in directory
dbutils.fs.ls("/path/")

# Display in table format
display(dbutils.fs.ls("/databricks-datasets/"))
```

**Copy Files**:
```python
# Copy file
dbutils.fs.cp("/source/file.csv", "/dest/file.csv")

# Copy directory recursively
dbutils.fs.cp("/source/dir/", "/dest/dir/", recurse=True)
```

**Move Files**:
```python
# Move/rename file
dbutils.fs.mv("/old/path/file.csv", "/new/path/file.csv")
```

**Remove Files**:
```python
# Remove file
dbutils.fs.rm("/path/to/file.csv")

# Remove directory recursively
dbutils.fs.rm("/path/to/dir/", recurse=True)
```

**Create Directory**:
```python
# Create directory
dbutils.fs.mkdirs("/path/to/new/dir/")
```

**Read File**:
```python
# Read first 65536 bytes
dbutils.fs.head("/path/to/file.txt")

# Read with custom byte count
dbutils.fs.head("/path/to/file.txt", 1000)
```

**File Information**:
```python
# Get file info
files = dbutils.fs.ls("/path/")
for file in files:
    print(f"Name: {file.name}")
    print(f"Path: {file.path}")
    print(f"Size: {file.size}")
    print(f"Modified: {file.modificationTime}")
```

### 4. FileStore

**Purpose**: Special DBFS location for user uploads with public web access

**Upload Methods**:
1. **UI Upload**: Data → Add Data → Upload File
2. **API Upload**: REST API or CLI
3. **Programmatic**: Using dbutils or Spark

**Access Patterns**:
```python
# FileStore path
"dbfs:/FileStore/my-file.csv"

# Web URL (public access)
"https://<databricks-instance>/files/my-file.csv"
```

**Use Cases**:
- Upload CSV/JSON files for analysis
- Store images for ML models
- Share files via public URLs
- Import libraries or JARs

### 5. Working with Cloud Storage

**AWS S3**:
```python
# Direct access (with credentials)
df = spark.read.csv("s3a://bucket-name/path/to/file.csv")

# Via mount point
dbutils.fs.mount(
    source="s3a://bucket-name",
    mount_point="/mnt/my-bucket",
    extra_configs={"fs.s3a.access.key": access_key,
                   "fs.s3a.secret.key": secret_key}
)
```

**Azure Data Lake Storage (ADLS)**:
```python
# Direct access
df = spark.read.csv("abfss://container@account.dfs.core.windows.net/path/")

# Via mount point
dbutils.fs.mount(
    source="abfss://container@account.dfs.core.windows.net/",
    mount_point="/mnt/my-adls"
)
```

**Google Cloud Storage (GCS)**:
```python
# Direct access
df = spark.read.csv("gs://bucket-name/path/to/file.csv")

# Via mount point
dbutils.fs.mount(
    source="gs://bucket-name",
    mount_point="/mnt/my-gcs"
)
```

### 6. Mount Points

**Purpose**: Create persistent connections to cloud storage

**Benefits**:
- Simplified paths (use `/mnt/` instead of full cloud URLs)
- Centralized credential management
- Consistent access across notebooks
- Better security (credentials not in code)

**Mount Operations**:
```python
# Mount
dbutils.fs.mount(
    source="s3a://my-bucket",
    mount_point="/mnt/my-data",
    extra_configs={"fs.s3a.access.key": "...",
                   "fs.s3a.secret.key": "..."}
)

# List mounts
dbutils.fs.mounts()

# Unmount
dbutils.fs.unmount("/mnt/my-data")

# Refresh mounts
dbutils.fs.refreshMounts()
```

### 7. Reading and Writing Files

**Read Files**:
```python
# CSV
df = spark.read.csv("dbfs:/path/to/file.csv", header=True)

# JSON
df = spark.read.json("dbfs:/path/to/file.json")

# Parquet
df = spark.read.parquet("dbfs:/path/to/file.parquet")

# Delta
df = spark.read.format("delta").load("dbfs:/path/to/delta/")

# Text
df = spark.read.text("dbfs:/path/to/file.txt")
```

**Write Files**:
```python
# CSV
df.write.csv("dbfs:/path/to/output/", header=True, mode="overwrite")

# JSON
df.write.json("dbfs:/path/to/output/", mode="overwrite")

# Parquet
df.write.parquet("dbfs:/path/to/output/", mode="overwrite")

# Delta
df.write.format("delta").save("dbfs:/path/to/delta/")
```

### 8. Local File System Access

**Within Cluster**:
```python
# Access DBFS as local file system
with open("/dbfs/path/to/file.txt", "r") as f:
    content = f.read()

# Write to DBFS as local file
with open("/dbfs/path/to/output.txt", "w") as f:
    f.write("Hello DBFS!")
```

**Note**: `/dbfs/` prefix converts DBFS paths to local file system paths within the cluster.

### 9. Best Practices

1. **Use Delta Lake for tables** - Better than raw files
2. **Organize with folders** - Clear directory structure
3. **Use mounts for cloud storage** - Avoid hardcoding credentials
4. **Clean up temporary files** - Use `/tmp/` and clean regularly
5. **Use FileStore for small files** - Not for large datasets
6. **Avoid `/tmp/` for persistent data** - May be deleted
7. **Use Unity Catalog volumes** - For file management (newer approach)
8. **Set appropriate permissions** - Control access to sensitive data

### 10. Common Patterns

**Upload and Process CSV**:
```python
# 1. Upload via UI to FileStore
# 2. Read into DataFrame
df = spark.read.csv("dbfs:/FileStore/my-data.csv", header=True, inferSchema=True)
# 3. Process
df_processed = df.filter(df.value > 100)
# 4. Save as Delta
df_processed.write.format("delta").save("dbfs:/delta/processed-data/")
```

**Copy from Cloud Storage**:
```python
# Copy from S3 to DBFS
dbutils.fs.cp("s3a://my-bucket/data/", "dbfs:/data/", recurse=True)

# Process from DBFS
df = spark.read.parquet("dbfs:/data/")
```

**Temporary File Processing**:
```python
# Write temporary results
df.write.parquet("dbfs:/tmp/intermediate-results/")

# Read and continue processing
df_temp = spark.read.parquet("dbfs:/tmp/intermediate-results/")

# Clean up
dbutils.fs.rm("dbfs:/tmp/intermediate-results/", recurse=True)
```

## Hands-On Exercises

See `exercise.py` for practical exercises covering:
- Navigating DBFS with dbutils.fs
- Uploading and managing files
- Working with FileStore
- Reading and writing various file formats
- Using mount points
- File operations (copy, move, delete)

## Key Takeaways

1. **DBFS is an abstraction layer** over cloud object storage
2. **dbutils.fs provides file system operations** (ls, cp, mv, rm, etc.)
3. **FileStore enables web-accessible uploads** for small files
4. **Mount points simplify cloud storage access** and secure credentials
5. **DBFS paths use `dbfs:/` prefix**, local paths use `/dbfs/`
6. **Data persists beyond cluster lifecycle** (except `/tmp/`)
7. **Use Delta Lake for structured data**, not raw files

## Exam Tips

- **Know dbutils.fs commands** (ls, cp, mv, rm, mkdirs, head)
- **Understand DBFS path formats** (dbfs:/ vs /dbfs/)
- **Know FileStore location** (dbfs:/FileStore/)
- **Understand mount points** (purpose and benefits)
- **Know default table location** (dbfs:/user/hive/warehouse/)
- **Understand temporary vs persistent storage** (/tmp/ vs other locations)
- **Know how to access cloud storage** (direct vs mount)

## Common Exam Questions

1. What is the DBFS path for FileStore?
2. How do you list files in a directory using dbutils?
3. What is the difference between `dbfs:/` and `/dbfs/` paths?
4. Where are managed tables stored by default?
5. How do you recursively copy a directory?
6. What is the purpose of mount points?
7. How do you access DBFS files as local files within a cluster?

## Additional Resources

- [DBFS Documentation](https://docs.databricks.com/dbfs/index.html)
- [dbutils.fs Documentation](https://docs.databricks.com/dev-tools/databricks-utils.html#file-system-utilities)
- [FileStore Documentation](https://docs.databricks.com/files/filestore.html)

## Next Steps

Tomorrow: [Day 5 - Databases, Tables, and Views](../day-05-databases-tables-and-views/README.md)
