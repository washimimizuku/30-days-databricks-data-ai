# Databricks Data Engineer Associate - Cheat Sheet

Quick reference for the Databricks Certified Data Engineer Associate exam.

---

## Delta Lake

### Time Travel
```sql
-- Query by version
SELECT * FROM table_name VERSION AS OF 5;

-- Query by timestamp
SELECT * FROM table_name TIMESTAMP AS OF '2024-01-01';
SELECT * FROM table_name TIMESTAMP AS OF '2024-01-01 12:00:00';

-- View table history
DESCRIBE HISTORY table_name;
DESCRIBE HISTORY table_name LIMIT 10;

-- Restore to previous version
RESTORE TABLE table_name TO VERSION AS OF 5;
RESTORE TABLE table_name TO TIMESTAMP AS OF '2024-01-01';
```

### Table Operations
```sql
-- Create Delta table
CREATE TABLE table_name (
    id INT,
    name STRING,
    created_at TIMESTAMP
) USING DELTA
LOCATION '/mnt/delta/table_name';

-- Create table from query
CREATE TABLE table_name
USING DELTA
AS SELECT * FROM source_table;

-- Insert data
INSERT INTO table_name VALUES (1, 'Alice', current_timestamp());
INSERT INTO table_name SELECT * FROM source_table;

-- Update data
UPDATE table_name SET name = 'Bob' WHERE id = 1;

-- Delete data
DELETE FROM table_name WHERE id = 1;
```

### MERGE (UPSERT)
```sql
-- Basic MERGE
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *;

-- MERGE with conditions
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED AND s.status = 'active' THEN
    UPDATE SET t.name = s.name, t.updated_at = current_timestamp()
WHEN MATCHED AND s.status = 'deleted' THEN
    DELETE
WHEN NOT MATCHED THEN
    INSERT (id, name, created_at) VALUES (s.id, s.name, current_timestamp());

-- MERGE with specific columns
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED THEN
    UPDATE SET t.name = s.name, t.amount = s.amount
WHEN NOT MATCHED THEN
    INSERT (id, name, amount) VALUES (s.id, s.name, s.amount);
```

### Optimization
```sql
-- Optimize (compact small files)
OPTIMIZE table_name;

-- Z-order (co-locate related data)
OPTIMIZE table_name ZORDER BY (column1, column2);
-- Note: Max 4 columns recommended

-- Vacuum (remove old files)
VACUUM table_name;
VACUUM table_name RETAIN 168 HOURS;  -- Default: 7 days
-- Note: Cannot vacuum files needed for time travel

-- Analyze table statistics
ANALYZE TABLE table_name COMPUTE STATISTICS;
ANALYZE TABLE table_name COMPUTE STATISTICS FOR COLUMNS column1, column2;
```

### Schema Evolution
```sql
-- Enable schema evolution
SET spark.databricks.delta.schema.autoMerge.enabled = true;

-- Merge schema on write
df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save("/path/to/table")

-- Add column
ALTER TABLE table_name ADD COLUMN new_column STRING;

-- Change column comment
ALTER TABLE table_name ALTER COLUMN column_name COMMENT 'New comment';

-- Drop column (requires rewrite)
ALTER TABLE table_name DROP COLUMN column_name;
```

### Change Data Feed
```sql
-- Enable Change Data Feed
ALTER TABLE table_name SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Create table with CDF enabled
CREATE TABLE table_name (id INT, name STRING)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Read changes
SELECT * FROM table_changes('table_name', 2, 5);  -- Versions 2 to 5
SELECT * FROM table_changes('table_name', '2024-01-01', '2024-01-02');  -- By timestamp
```

---

## Structured Streaming

### Read Stream
```python
# Read from files (Auto Loader)
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/mnt/schema/location") \
    .load("/mnt/data/input/")

# Read from Delta table
df = spark.readStream \
    .format("delta") \
    .table("source_table")

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host:port") \
    .option("subscribe", "topic") \
    .load()
```

### Write Stream
```python
# Write to Delta table
query = df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/checkpoint/") \
    .table("target_table")

# Write to files
query = df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/checkpoint/") \
    .start("/mnt/data/output/")

# Write to console (testing)
query = df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()
```

### Output Modes
```python
# Append: Only new rows (default, most common)
.outputMode("append")

# Complete: Entire result table (for aggregations)
.outputMode("complete")

# Update: Only changed rows (for aggregations with watermark)
.outputMode("update")
```

### Triggers
```python
# Process once (batch-like)
.trigger(once=True)

# Continuous (experimental, low latency)
.trigger(continuous="1 second")

# Micro-batch (default)
.trigger(processingTime="10 seconds")

# Available now (process all available data)
.trigger(availableNow=True)
```

### Watermarking
```python
from pyspark.sql.functions import window, col

# Add watermark for late data
df_with_watermark = df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("user_id")
    ) \
    .count()
```

### Windowing
```python
from pyspark.sql.functions import window

# Tumbling window (non-overlapping)
df.groupBy(window(col("timestamp"), "10 minutes")).count()

# Sliding window (overlapping)
df.groupBy(window(col("timestamp"), "10 minutes", "5 minutes")).count()

# Session window (gap-based)
df.groupBy(session_window(col("timestamp"), "5 minutes")).count()
```

---

## Data Ingestion

### COPY INTO
```sql
-- Basic COPY INTO
COPY INTO target_table
FROM '/mnt/data/source/'
FILEFORMAT = CSV;

-- With options
COPY INTO target_table
FROM '/mnt/data/source/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'delimiter' = ',')
COPY_OPTIONS ('mergeSchema' = 'true');

-- With file pattern
COPY INTO target_table
FROM '/mnt/data/source/'
FILEFORMAT = PARQUET
FILES = ('file1.parquet', 'file2.parquet');

-- Incremental load (idempotent)
COPY INTO target_table
FROM '/mnt/data/source/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('inferSchema' = 'true');
-- Note: Automatically skips already loaded files
```

### Auto Loader (Cloud Files)
```python
# Basic Auto Loader
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/mnt/schema/") \
    .load("/mnt/data/input/")

# With schema inference
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/mnt/schema/") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("header", "true") \
    .load("/mnt/data/input/")

# With schema evolution
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "/mnt/schema/") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .load("/mnt/data/input/")
```

---

## SQL Functions

### Window Functions
```sql
-- Ranking
SELECT 
    id,
    name,
    salary,
    ROW_NUMBER() OVER (ORDER BY salary DESC) as row_num,
    RANK() OVER (ORDER BY salary DESC) as rank,
    DENSE_RANK() OVER (ORDER BY salary DESC) as dense_rank
FROM employees;

-- Partition by
SELECT 
    id,
    department,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
    AVG(salary) OVER (PARTITION BY department) as dept_avg_salary
FROM employees;

-- LAG and LEAD
SELECT 
    id,
    order_date,
    amount,
    LAG(amount, 1) OVER (ORDER BY order_date) as prev_amount,
    LEAD(amount, 1) OVER (ORDER BY order_date) as next_amount
FROM orders;

-- Running totals
SELECT 
    id,
    order_date,
    amount,
    SUM(amount) OVER (ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total
FROM orders;

-- Moving average
SELECT 
    id,
    order_date,
    amount,
    AVG(amount) OVER (ORDER BY order_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg_3
FROM orders;
```

### Higher-Order Functions
```sql
-- TRANSFORM (apply function to array elements)
SELECT TRANSFORM(array(1, 2, 3), x -> x * 2) as doubled;
SELECT TRANSFORM(items, item -> item.price * 1.1) as prices_with_tax FROM orders;

-- FILTER (filter array elements)
SELECT FILTER(array(1, 2, 3, 4, 5), x -> x > 2) as filtered;
SELECT FILTER(items, item -> item.quantity > 0) as available_items FROM orders;

-- AGGREGATE (reduce array to single value)
SELECT AGGREGATE(array(1, 2, 3, 4), 0, (acc, x) -> acc + x) as sum;

-- EXISTS (check if condition exists in array)
SELECT EXISTS(items, item -> item.price > 100) as has_expensive_items FROM orders;

-- FORALL (check if all elements satisfy condition)
SELECT FORALL(items, item -> item.quantity > 0) as all_in_stock FROM orders;
```

### Array Functions
```sql
-- EXPLODE (array to rows)
SELECT id, EXPLODE(items) as item FROM orders;

-- EXPLODE_OUTER (includes nulls)
SELECT id, EXPLODE_OUTER(items) as item FROM orders;

-- POSEXPLODE (with position)
SELECT id, pos, item FROM orders LATERAL VIEW POSEXPLODE(items) as pos, item;

-- Array operations
SELECT 
    array(1, 2, 3) as arr,
    array_contains(array(1, 2, 3), 2) as contains_2,
    array_distinct(array(1, 2, 2, 3)) as distinct_arr,
    array_union(array(1, 2), array(2, 3)) as union_arr,
    array_intersect(array(1, 2), array(2, 3)) as intersect_arr,
    size(array(1, 2, 3)) as arr_size;

-- FLATTEN (nested arrays)
SELECT FLATTEN(array(array(1, 2), array(3, 4))) as flattened;
```

---

## PySpark DataFrame API

### Basic Operations
```python
from pyspark.sql.functions import col, lit, when

# Select columns
df.select("col1", "col2")
df.select(col("col1"), col("col2"))

# Filter rows
df.filter(col("age") > 18)
df.filter("age > 18")
df.where(col("status") == "active")

# Add/modify columns
df.withColumn("new_col", col("old_col") * 2)
df.withColumn("category", when(col("amount") > 100, "high").otherwise("low"))

# Rename columns
df.withColumnRenamed("old_name", "new_name")

# Drop columns
df.drop("col1", "col2")

# Sort
df.orderBy("col1")
df.orderBy(col("col1").desc())
df.sort(col("col1").asc(), col("col2").desc())
```

### Aggregations
```python
from pyspark.sql.functions import sum, avg, count, min, max

# Group by
df.groupBy("department").count()
df.groupBy("department").agg(
    sum("salary").alias("total_salary"),
    avg("salary").alias("avg_salary"),
    count("*").alias("employee_count")
)

# Multiple aggregations
df.groupBy("department", "location").agg({
    "salary": "sum",
    "age": "avg",
    "id": "count"
})
```

### Joins
```python
# Inner join (default)
df1.join(df2, df1.id == df2.id)
df1.join(df2, "id")  # If column name is same

# Left join
df1.join(df2, df1.id == df2.id, "left")

# Right join
df1.join(df2, df1.id == df2.id, "right")

# Full outer join
df1.join(df2, df1.id == df2.id, "outer")

# Cross join
df1.crossJoin(df2)

# Anti join (rows in df1 not in df2)
df1.join(df2, df1.id == df2.id, "anti")

# Semi join (rows in df1 that have match in df2)
df1.join(df2, df1.id == df2.id, "semi")
```

### Window Functions
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead

# Define window
window_spec = Window.partitionBy("department").orderBy("salary")

# Apply window functions
df.withColumn("row_num", row_number().over(window_spec))
df.withColumn("rank", rank().over(window_spec))
df.withColumn("prev_salary", lag("salary", 1).over(window_spec))

# Running total
window_spec_running = Window.partitionBy("department").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("running_total", sum("amount").over(window_spec_running))
```

### UDFs (User-Defined Functions)
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

# Define UDF
@udf(returnType=StringType())
def categorize_age(age):
    if age < 18:
        return "minor"
    elif age < 65:
        return "adult"
    else:
        return "senior"

# Use UDF
df.withColumn("age_category", categorize_age(col("age")))

# Alternative syntax
def categorize_age_func(age):
    if age < 18:
        return "minor"
    elif age < 65:
        return "adult"
    else:
        return "senior"

categorize_age_udf = udf(categorize_age_func, StringType())
df.withColumn("age_category", categorize_age_udf(col("age")))
```

---

## File Operations

### Read Data
```python
# CSV
df = spark.read.csv("/path/to/file.csv", header=True, inferSchema=True)
df = spark.read.format("csv").option("header", "true").load("/path/")

# JSON
df = spark.read.json("/path/to/file.json")
df = spark.read.format("json").load("/path/")

# Parquet
df = spark.read.parquet("/path/to/file.parquet")
df = spark.read.format("parquet").load("/path/")

# Delta
df = spark.read.format("delta").load("/path/to/delta/table")
df = spark.read.table("table_name")

# With options
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ",") \
    .load("/path/")
```

### Write Data
```python
# CSV
df.write.csv("/path/to/output/", header=True, mode="overwrite")

# JSON
df.write.json("/path/to/output/", mode="overwrite")

# Parquet
df.write.parquet("/path/to/output/", mode="overwrite")

# Delta
df.write.format("delta").mode("overwrite").save("/path/to/delta/table")
df.write.format("delta").mode("overwrite").saveAsTable("table_name")

# Write modes
.mode("overwrite")  # Replace existing data
.mode("append")     # Add to existing data
.mode("ignore")     # Skip if exists
.mode("error")      # Fail if exists (default)

# Partitioning
df.write.partitionBy("year", "month").parquet("/path/")
```

### DBFS Operations
```python
# List files
dbutils.fs.ls("/path/")
display(dbutils.fs.ls("/databricks-datasets/"))

# Copy files
dbutils.fs.cp("/source/path", "/dest/path")
dbutils.fs.cp("/source/path", "/dest/path", recurse=True)

# Move files
dbutils.fs.mv("/source/path", "/dest/path")

# Remove files
dbutils.fs.rm("/path/to/file")
dbutils.fs.rm("/path/to/directory", recurse=True)

# Create directory
dbutils.fs.mkdirs("/path/to/directory")

# Read file
dbutils.fs.head("/path/to/file")

# File info
dbutils.fs.ls("/path/").map(lambda x: (x.name, x.size, x.modificationTime))
```

---

## Databricks Jobs

### Job Configuration
```python
# Create job via API
{
    "name": "My ETL Job",
    "tasks": [
        {
            "task_key": "extract",
            "notebook_task": {
                "notebook_path": "/Users/user@example.com/extract_notebook",
                "base_parameters": {"date": "2024-01-01"}
            },
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2
            }
        },
        {
            "task_key": "transform",
            "depends_on": [{"task_key": "extract"}],
            "notebook_task": {
                "notebook_path": "/Users/user@example.com/transform_notebook"
            },
            "existing_cluster_id": "cluster-id"
        }
    ],
    "schedule": {
        "quartz_cron_expression": "0 0 * * * ?",
        "timezone_id": "America/Los_Angeles"
    }
}
```

### Task Dependencies
- Linear: Task B depends on Task A
- Fan-out: Tasks B and C depend on Task A
- Fan-in: Task D depends on Tasks B and C
- Complex: Mix of dependencies

### Cluster Types for Jobs
- **New cluster**: Created for job, terminated after
- **Existing cluster**: Use existing all-purpose cluster
- **Job cluster**: Shared across tasks in same job

---

## Unity Catalog

### Hierarchy
```
Metastore
└── Catalog
    └── Schema (Database)
        └── Table/View
```

### Basic Operations
```sql
-- Create catalog
CREATE CATALOG IF NOT EXISTS my_catalog;

-- Use catalog
USE CATALOG my_catalog;

-- Create schema
CREATE SCHEMA IF NOT EXISTS my_catalog.my_schema;

-- Use schema
USE my_catalog.my_schema;

-- Create table
CREATE TABLE my_catalog.my_schema.my_table (
    id INT,
    name STRING
) USING DELTA;

-- Query with full path
SELECT * FROM my_catalog.my_schema.my_table;

-- Show catalogs
SHOW CATALOGS;

-- Show schemas
SHOW SCHEMAS IN my_catalog;

-- Show tables
SHOW TABLES IN my_catalog.my_schema;
```

### Permissions
```sql
-- Grant permissions
GRANT SELECT ON TABLE my_catalog.my_schema.my_table TO `user@example.com`;
GRANT ALL PRIVILEGES ON SCHEMA my_catalog.my_schema TO `data_engineers`;
GRANT USAGE ON CATALOG my_catalog TO `analysts`;

-- Revoke permissions
REVOKE SELECT ON TABLE my_catalog.my_schema.my_table FROM `user@example.com`;

-- Show grants
SHOW GRANTS ON TABLE my_catalog.my_schema.my_table;
```

---

## Cluster Configuration

### Cluster Modes
- **Standard**: Single user, full Spark API
- **High Concurrency**: Multiple users, process isolation
- **Single Node**: No workers, driver only

### Autoscaling
```python
{
    "autoscale": {
        "min_workers": 1,
        "max_workers": 5
    }
}
```

### Auto-termination
```python
{
    "autotermination_minutes": 30
}
```

---

## Common Patterns

### Medallion Architecture
```
Bronze (Raw) → Silver (Cleaned) → Gold (Aggregated)
```

**Bronze**: Raw data, minimal processing
**Silver**: Cleaned, validated, deduplicated
**Gold**: Business-level aggregates

### SCD Type 2 (Slowly Changing Dimensions)
```sql
MERGE INTO dim_customer t
USING (
    SELECT *, current_timestamp() as effective_date
    FROM source_customer
) s
ON t.customer_id = s.customer_id AND t.is_current = true
WHEN MATCHED AND (t.name != s.name OR t.email != s.email) THEN
    UPDATE SET is_current = false, end_date = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, effective_date, is_current)
    VALUES (s.customer_id, s.name, s.email, s.effective_date, true);
```

### Incremental Processing
```python
# Read new data only
df_new = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", last_processed_version) \
    .table("source_table")

# Process and write
df_processed = process(df_new)
df_processed.write.format("delta").mode("append").save("/path/")
```

---

## Exam Tips

### Key Facts to Remember
- **VACUUM default retention**: 7 days (168 hours)
- **Z-order max columns**: 4 recommended
- **Exam format**: 45 questions, 90 minutes, 70% passing (32/45)
- **Output modes**: append (most common), complete, update
- **Checkpoint**: Required for streaming fault tolerance

### Common Mistakes
- Forgetting checkpoint location in streaming
- Using wrong output mode for aggregations
- Not setting watermark for late data
- Vacuuming too aggressively (loses time travel)
- Using UPDATE instead of MERGE for upserts

### Time Management
- 2 minutes per question
- Flag difficult questions
- Review flagged questions at end
- Don't overthink

---

**Good luck on your exam!** 🚀
