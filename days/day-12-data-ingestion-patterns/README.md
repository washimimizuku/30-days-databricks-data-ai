# Day 12: Data Ingestion Patterns

## Learning Objectives

By the end of today, you will:
- Master COPY INTO for efficient batch data ingestion
- Implement Auto Loader for scalable streaming ingestion
- Understand schema inference and evolution strategies
- Handle various file formats (CSV, JSON, Parquet, Avro)
- Implement idempotent and incremental ingestion patterns
- Apply data quality checks during ingestion
- Choose the right ingestion pattern for different scenarios

## Topics Covered

### 1. Data Ingestion Overview

Data ingestion is the process of loading data from external sources into your lakehouse. Databricks provides two primary patterns:

**COPY INTO** (Batch):
- SQL-based batch ingestion
- Idempotent (safe to re-run)
- Tracks processed files
- Good for scheduled batch loads

**Auto Loader** (Streaming):
- Structured Streaming-based
- Scalable file discovery
- Automatic schema inference and evolution
- Ideal for continuous ingestion

### 2. COPY INTO Command

COPY INTO is a SQL command for loading data from files into Delta tables.

#### Basic Syntax

```sql
COPY INTO target_table
FROM 'source_path'
FILEFORMAT = format_name
[FORMAT_OPTIONS (option1 = value1, option2 = value2)]
[COPY_OPTIONS (option1 = value1, option2 = value2)]
```

#### Simple Examples

```sql
-- Copy CSV files
COPY INTO my_table
FROM '/mnt/data/csv_files/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');

-- Copy JSON files
COPY INTO my_table
FROM '/mnt/data/json_files/'
FILEFORMAT = JSON;

-- Copy Parquet files
COPY INTO my_table
FROM '/mnt/data/parquet_files/'
FILEFORMAT = PARQUET;
```

#### File Format Options

```sql
-- CSV with custom options
COPY INTO my_table
FROM '/path/to/csv/'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'header' = 'true',
  'inferSchema' = 'true',
  'delimiter' = ',',
  'quote' = '"',
  'escape' = '\\',
  'nullValue' = 'NULL',
  'dateFormat' = 'yyyy-MM-dd'
);

-- JSON with multiline support
COPY INTO my_table
FROM '/path/to/json/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('multiLine' = 'true');
```

#### Copy Options

```sql
-- With copy options
COPY INTO my_table
FROM '/path/to/files/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
COPY_OPTIONS (
  'mergeSchema' = 'true',        -- Merge new columns
  'force' = 'false'               -- Skip already loaded files
);
```

#### Pattern Matching

```sql
-- Load specific files using pattern
COPY INTO my_table
FROM '/path/to/files/'
FILEFORMAT = CSV
PATTERN = '*.csv'
FORMAT_OPTIONS ('header' = 'true');

-- Load files from specific date
COPY INTO my_table
FROM '/path/to/files/'
FILEFORMAT = CSV
PATTERN = '2024-01-*.csv';
```

#### Tracking Loaded Files

COPY INTO automatically tracks which files have been loaded:

```sql
-- View loaded files
DESCRIBE HISTORY my_table;

-- Force reload (use with caution)
COPY INTO my_table
FROM '/path/to/files/'
FILEFORMAT = CSV
COPY_OPTIONS ('force' = 'true');
```

### 3. Auto Loader (cloudFiles)

Auto Loader incrementally and efficiently processes new files as they arrive in cloud storage.

#### Basic Auto Loader Pattern

```python
# Read with Auto Loader
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("header", "true") \
    .load("/path/to/files/")

# Write to Delta table
df.writeStream \
    .option("checkpointLocation", "/path/to/checkpoint/") \
    .trigger(availableNow=True) \
    .table("target_table")
```

#### Schema Inference

```python
# Automatic schema inference
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/path/to/schema/") \
    .load("/path/to/files/")
```

#### Schema Evolution

```python
# Enable schema evolution
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/path/to/schema/") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .option("header", "true") \
    .load("/path/to/files/")
```

#### Schema Hints

```python
# Provide schema hints
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/path/to/schema/") \
    .option("cloudFiles.schemaHints", "id INT, amount DOUBLE") \
    .option("header", "true") \
    .load("/path/to/files/")
```

#### File Notification Modes

```python
# Directory listing (default, good for small datasets)
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.useNotifications", "false") \
    .load("/path/to/files/")

# File notification (scalable for large datasets)
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.useNotifications", "true") \
    .load("/path/to/files/")
```

#### Trigger Options

```python
# Process all available files once
df.writeStream \
    .trigger(availableNow=True) \
    .option("checkpointLocation", "/checkpoint/") \
    .table("target_table")

# Continuous processing
df.writeStream \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "/checkpoint/") \
    .table("target_table")

# Process once and stop
df.writeStream \
    .trigger(once=True) \
    .option("checkpointLocation", "/checkpoint/") \
    .table("target_table")
```

### 4. Schema Management

#### Explicit Schema Definition

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("amount", DoubleType(), True)
])

# Use with COPY INTO
spark.read.schema(schema).csv("/path/").write.saveAsTable("my_table")

# Use with Auto Loader
df = spark.readStream \
    .format("cloudFiles") \
    .schema(schema) \
    .option("cloudFiles.format", "csv") \
    .load("/path/")
```

#### Schema Evolution Modes

```python
# Add new columns (default)
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")

# Fail on schema changes
.option("cloudFiles.schemaEvolutionMode", "failOnNewColumns")

# Rescue data (save unparseable data)
.option("cloudFiles.schemaEvolutionMode", "rescue")
```

### 5. Data Quality and Validation

#### Column Constraints

```sql
-- Add constraints during table creation
CREATE TABLE my_table (
  id INT NOT NULL,
  email STRING,
  amount DOUBLE CHECK (amount >= 0),
  CONSTRAINT valid_email CHECK (email LIKE '%@%.%')
);
```

#### Data Validation in Python

```python
from pyspark.sql.functions import col, when

# Validate and filter
df_validated = df \
    .filter(col("amount") > 0) \
    .filter(col("email").isNotNull()) \
    .withColumn("is_valid", 
        when((col("amount") > 0) & (col("email").isNotNull()), True)
        .otherwise(False)
    )

# Separate valid and invalid records
df_valid = df_validated.filter(col("is_valid") == True)
df_invalid = df_validated.filter(col("is_valid") == False)
```

#### Quarantine Pattern

```python
# Write valid records to main table
df_valid.write.mode("append").saveAsTable("main_table")

# Write invalid records to quarantine table
df_invalid.write.mode("append").saveAsTable("quarantine_table")
```

### 6. Incremental Ingestion Patterns

#### Watermark-based Ingestion

```python
# Process only new files based on timestamp
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/schema/") \
    .load("/path/to/files/")

df_with_watermark = df.withWatermark("event_time", "1 hour")
```

#### Idempotent Ingestion

```python
# COPY INTO is idempotent by default
COPY INTO my_table
FROM '/path/to/files/'
FILEFORMAT = CSV;

# Auto Loader with checkpoint ensures idempotency
df.writeStream \
    .option("checkpointLocation", "/checkpoint/") \
    .table("my_table")
```

### 7. Performance Optimization

#### Partitioning During Ingestion

```python
# Partition by date
df.write \
    .partitionBy("date") \
    .mode("append") \
    .saveAsTable("partitioned_table")
```

#### File Size Optimization

```python
# Control file size with maxFilesPerTrigger
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("maxFilesPerTrigger", "1000") \
    .load("/path/")
```

#### Parallel Processing

```python
# Increase parallelism
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.maxFilesPerTrigger", "100") \
    .load("/path/")
```

### 8. COPY INTO vs Auto Loader

| Feature | COPY INTO | Auto Loader |
|---------|-----------|-------------|
| **Type** | Batch (SQL) | Streaming (Python) |
| **Scalability** | Good for moderate data | Excellent for large-scale |
| **File Discovery** | Directory listing | Efficient file notification |
| **Schema Evolution** | Manual | Automatic |
| **Idempotency** | Built-in | Via checkpoints |
| **Use Case** | Scheduled batch loads | Continuous ingestion |
| **Cost** | Lower for small datasets | More efficient at scale |

**When to use COPY INTO:**
- Scheduled batch jobs
- Smaller datasets
- SQL-based workflows
- Simple ingestion requirements

**When to use Auto Loader:**
- Continuous data arrival
- Large-scale ingestion
- Need schema evolution
- Complex transformations

## Hands-On Exercises

See `exercise.py` for 60 practical exercises covering:
- COPY INTO with various file formats
- Auto Loader implementation
- Schema management and evolution
- Data quality validation
- Incremental ingestion patterns
- Performance optimization

## Key Takeaways

1. COPY INTO is ideal for batch ingestion with built-in idempotency
2. Auto Loader scales better for continuous, large-scale ingestion
3. Schema inference reduces manual schema definition effort
4. Schema evolution allows handling changing data structures
5. Checkpoints ensure exactly-once processing in Auto Loader
6. Data quality checks should be part of ingestion pipeline
7. Choose ingestion pattern based on data volume and arrival pattern
8. Both patterns integrate seamlessly with Delta Lake

## Exam Tips

- Know the syntax for COPY INTO command
- Understand when to use COPY INTO vs Auto Loader
- Remember that COPY INTO tracks loaded files automatically
- Know schema evolution options in Auto Loader
- Understand checkpoint location importance
- Be familiar with trigger options (once, availableNow, processingTime)
- Know file format options for CSV, JSON, Parquet
- Understand idempotency guarantees
- Remember that cloudFiles is the format for Auto Loader
- Know how to handle schema changes

## Common Pitfalls

1. Forgetting checkpoint location in Auto Loader
2. Not handling schema evolution
3. Using force=true in COPY INTO unnecessarily
4. Not validating data quality during ingestion
5. Choosing wrong ingestion pattern for use case
6. Not monitoring ingestion failures
7. Ignoring file size optimization
8. Not testing with sample data first
9. Forgetting to handle null values
10. Not documenting schema expectations

## Additional Resources

- [COPY INTO Documentation](https://docs.databricks.com/sql/language-manual/delta-copy-into.html)
- [Auto Loader Documentation](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Schema Evolution Guide](https://docs.databricks.com/ingestion/auto-loader/schema.html)
- [Data Ingestion Best Practices](https://docs.databricks.com/ingestion/index.html)

## Next Steps

Tomorrow: [Day 13 - Data Transformation Patterns](../day-13-data-transformation-patterns/README.md)

Master these ingestion patterns - they're fundamental to building robust data pipelines!
