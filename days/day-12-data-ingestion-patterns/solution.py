# Day 12: Data Ingestion Patterns - Solutions
# =========================================================

from pyspark.sql.functions import col, current_timestamp, input_file_name, lit, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# =========================================================
# PART 1: COPY INTO - BASIC USAGE
# =========================================================

# Exercise 1: Simple COPY INTO from CSV
spark.sql("""
COPY INTO day12_db.customers_copy
FROM '/FileStore/day12-ingestion/csv/batch1.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
""")

# Exercise 2: COPY INTO with Format Options
spark.sql("""
COPY INTO day12_db.customers_copy
FROM '/FileStore/day12-ingestion/csv/batch2.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true',
    'inferSchema' = 'true',
    'delimiter' = ',',
    'quote' = '"'
)
""")

# Exercise 3: COPY INTO Multiple Files
spark.sql("""
COPY INTO day12_db.customers_copy
FROM '/FileStore/day12-ingestion/csv/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
""")

# Exercise 4: COPY INTO with Pattern Matching
spark.sql("""
COPY INTO day12_db.customers_copy
FROM '/FileStore/day12-ingestion/csv/'
FILEFORMAT = CSV
PATTERN = 'batch*.csv'
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
""")

# Exercise 5: View Loaded Files
spark.sql("DESCRIBE HISTORY day12_db.customers_copy").show()

# =========================================================
# PART 2: COPY INTO - FILE FORMATS
# =========================================================

# Exercise 6: COPY INTO from JSON
spark.sql("""
COPY INTO day12_db.orders
FROM '/FileStore/day12-ingestion/json/orders1.json'
FILEFORMAT = JSON
""")

# Exercise 7: COPY INTO from Parquet
spark.sql("""
CREATE TABLE IF NOT EXISTS day12_db.products (
    product_id INT,
    product_name STRING,
    category STRING,
    price DOUBLE,
    stock INT
) USING DELTA
""")

spark.sql("""
COPY INTO day12_db.products
FROM '/FileStore/day12-ingestion/parquet/products1.parquet'
FILEFORMAT = PARQUET
""")

# Exercise 8: COPY INTO with Custom Delimiter
# Create pipe-delimited file first
pipe_data = "id|name|amount\n1|Alice|1000\n2|Bob|2000"
with open("/dbfs/FileStore/day12-ingestion/csv/pipe_delimited.txt", "w") as f:
    f.write(pipe_data)

spark.sql("""
COPY INTO day12_db.customers_copy
FROM '/FileStore/day12-ingestion/csv/pipe_delimited.txt'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'delimiter' = '|')
""")

# Exercise 9: COPY INTO with Null Values
null_data = "id,name,amount\n1,Alice,1000\n2,NULL,2000\n3,Charlie,NULL"
with open("/dbfs/FileStore/day12-ingestion/csv/with_nulls.csv", "w") as f:
    f.write(null_data)

spark.sql("""
COPY INTO day12_db.customers_copy
FROM '/FileStore/day12-ingestion/csv/with_nulls.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'nullValue' = 'NULL')
""")

# Exercise 10: COPY INTO with Date Format
date_data = "id,name,date\n1,Alice,01/15/2024\n2,Bob,01/16/2024"
with open("/dbfs/FileStore/day12-ingestion/csv/custom_dates.csv", "w") as f:
    f.write(date_data)

spark.sql("""
COPY INTO day12_db.customers_copy
FROM '/FileStore/day12-ingestion/csv/custom_dates.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'dateFormat' = 'MM/dd/yyyy')
""")

# =========================================================
# PART 3: COPY INTO - ADVANCED OPTIONS
# =========================================================

# Exercise 11: COPY INTO with mergeSchema
spark.sql("""
COPY INTO day12_db.customers_copy
FROM '/FileStore/day12-ingestion/csv/batch3.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')
""")

# Exercise 12: COPY INTO with force Option
spark.sql("""
COPY INTO day12_db.customers_copy
FROM '/FileStore/day12-ingestion/csv/batch1.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
COPY_OPTIONS ('force' = 'true')
""")

# Exercise 13: COPY INTO Idempotency Test
# Run twice and verify count
count_before = spark.table("day12_db.customers_copy").count()
spark.sql("""
COPY INTO day12_db.customers_copy
FROM '/FileStore/day12-ingestion/csv/batch1.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
""")
count_after = spark.table("day12_db.customers_copy").count()
print(f"Count before: {count_before}, Count after: {count_after}")
print(f"Idempotent: {count_before == count_after}")

# Exercise 14: COPY INTO with Validation
result = spark.sql("""
COPY INTO day12_db.customers_copy
FROM '/FileStore/day12-ingestion/csv/batch2.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
""")
display(result)

# Exercise 15: COPY INTO Error Handling
try:
    spark.sql("""
    COPY INTO day12_db.customers_copy
    FROM '/FileStore/day12-ingestion/csv/nonexistent.csv'
    FILEFORMAT = CSV
    """)
except Exception as e:
    print(f"Error handled: {str(e)}")

# =========================================================
# PART 4: AUTO LOADER - BASIC USAGE
# =========================================================

# Exercise 16: Simple Auto Loader
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("header", "true") \
    .load("/FileStore/day12-ingestion/incremental/")

display(df)

# Exercise 17: Auto Loader with Schema Location
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/FileStore/day12-ingestion/schemas/incremental/") \
    .option("header", "true") \
    .load("/FileStore/day12-ingestion/incremental/")

# Exercise 18: Auto Loader Write to Table
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/FileStore/day12-ingestion/schemas/autoloader1/") \
    .option("header", "true") \
    .load("/FileStore/day12-ingestion/incremental/")

query = df.writeStream \
    .option("checkpointLocation", "/FileStore/day12-ingestion/checkpoints/autoloader1/") \
    .trigger(availableNow=True) \
    .table("day12_db.customers_autoloader")

query.awaitTermination()

# Exercise 19: Auto Loader with Trigger Once
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/FileStore/day12-ingestion/schemas/once/") \
    .option("header", "true") \
    .load("/FileStore/day12-ingestion/incremental/")

query = df.writeStream \
    .option("checkpointLocation", "/FileStore/day12-ingestion/checkpoints/once/") \
    .trigger(once=True) \
    .table("day12_db.customers_autoloader")

query.awaitTermination()

# Exercise 20: Auto Loader with availableNow
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/FileStore/day12-ingestion/schemas/available/") \
    .option("header", "true") \
    .load("/FileStore/day12-ingestion/incremental/")

query = df.writeStream \
    .option("checkpointLocation", "/FileStore/day12-ingestion/checkpoints/available/") \
    .trigger(availableNow=True) \
    .table("day12_db.customers_autoloader")

query.awaitTermination()

# =========================================================
# PART 5: AUTO LOADER - SCHEMA MANAGEMENT
# =========================================================

# Exercise 21: Auto Loader with Schema Inference
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/FileStore/day12-ingestion/schemas/infer/") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("header", "true") \
    .load("/FileStore/day12-ingestion/csv/")

# Exercise 22: Auto Loader with Explicit Schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

df = spark.readStream \
    .format("cloudFiles") \
    .schema(schema) \
    .option("cloudFiles.format", "csv") \
    .option("header", "true") \
    .load("/FileStore/day12-ingestion/incremental/")

# Exercise 23: Auto Loader with Schema Hints
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/FileStore/day12-ingestion/schemas/hints/") \
    .option("cloudFiles.schemaHints", "id INT, amount DOUBLE") \
    .option("header", "true") \
    .load("/FileStore/day12-ingestion/incremental/")

# Exercise 24: Auto Loader Schema Evolution
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/FileStore/day12-ingestion/schemas/evolution/") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .option("header", "true") \
    .load("/FileStore/day12-ingestion/csv/")

# Exercise 25: Auto Loader Rescue Data
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/FileStore/day12-ingestion/schemas/rescue/") \
    .option("cloudFiles.schemaEvolutionMode", "rescue") \
    .option("header", "true") \
    .load("/FileStore/day12-ingestion/csv/")

# =========================================================
# PART 6-12: ADDITIONAL SOLUTIONS (Abbreviated for space)
# =========================================================

# Exercise 26-30: File Formats
# JSON, Parquet, Multiline JSON, Nested JSON, Format Options
df_json = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/FileStore/day12-ingestion/schemas/json/") \
    .load("/FileStore/day12-ingestion/json/")

# Exercise 31-35: Performance
# maxFilesPerTrigger, File Notifications, Checkpoints, Parallel Processing
df_perf = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/FileStore/day12-ingestion/schemas/perf/") \
    .option("cloudFiles.maxFilesPerTrigger", "10") \
    .option("cloudFiles.useNotifications", "false") \
    .option("header", "true") \
    .load("/FileStore/day12-ingestion/incremental/")

# Exercise 36-40: Data Quality
df_quality = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/FileStore/day12-ingestion/schemas/quality/") \
    .option("header", "true") \
    .load("/FileStore/day12-ingestion/csv/")

df_validated = df_quality \
    .filter(col("amount") > 0) \
    .filter(col("email").isNotNull()) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", input_file_name())

# Exercise 41-45: Incremental Patterns
# Incremental with COPY INTO, Auto Loader, Watermarks, Deduplication, Upsert

# Exercise 46-50: Schema Evolution
# Handle new columns, type changes, validation, backward compatibility

# Exercise 51-55: Error Handling
# Missing files, corrupt records, schema mismatch, retry logic, error logging

# Exercise 56-60: Real-World Scenarios
# Daily batch, continuous streaming, multi-source, partitioned, complete pipeline

# =========================================================
# REFLECTION QUESTIONS - ANSWERS
# =========================================================

"""
1. When should you use COPY INTO vs Auto Loader?
   - COPY INTO: Scheduled batch jobs, smaller datasets, SQL workflows, simple requirements
   - Auto Loader: Continuous arrival, large-scale, schema evolution, complex transformations
   - COPY INTO is simpler for batch, Auto Loader scales better for streaming

2. What is the purpose of checkpoint location in Auto Loader?
   - Tracks processed files for exactly-once semantics
   - Stores stream state for fault tolerance
   - Enables recovery after failures
   - Required for all streaming queries
   - Must be unique per stream

3. How does COPY INTO ensure idempotency?
   - Automatically tracks loaded files in transaction log
   - Skips already-loaded files by default
   - Uses file path and modification time as key
   - force=true option overrides this behavior
   - Safe to re-run without duplicates

4. What are the schema evolution modes in Auto Loader?
   - addNewColumns: Add new columns automatically (default)
   - failOnNewColumns: Fail if schema changes detected
   - rescue: Save unparseable data to _rescued_data column
   - none: No schema evolution

5. How do you handle schema changes in COPY INTO?
   - Use mergeSchema=true in COPY_OPTIONS
   - Manually alter table before loading
   - Use explicit schema in read operation
   - COPY INTO doesn't auto-evolve schema like Auto Loader

6. What is the difference between trigger(once=True) and trigger(availableNow=True)?
   - once=True: Process available data once and stop (legacy)
   - availableNow=True: Process all available data in multiple batches, then stop (recommended)
   - availableNow is more efficient for large backlogs
   - Both are for batch-like processing of streaming data

7. How do you monitor Auto Loader progress?
   - Check streaming query status
   - Query checkpoint location
   - Use Spark UI streaming tab
   - Monitor target table row counts
   - Check schema location for schema changes

8. What happens if you delete checkpoint location?
   - Stream loses state
   - May reprocess files (duplicates)
   - Schema inference restarts
   - Should avoid deleting in production
   - Use for testing/reset only

9. How do you handle corrupt or invalid records?
   - Use rescue mode to capture unparseable data
   - Add validation filters
   - Write to quarantine table
   - Log errors to separate table
   - Use try-catch for error handling

10. What are best practices for data quality during ingestion?
    - Validate data types and formats
    - Check for nulls in required fields
    - Implement range checks
    - Use quarantine pattern for invalid data
    - Add ingestion metadata (timestamp, source file)
    - Monitor data quality metrics
    - Test with sample data first
"""

# =========================================================
# BONUS CHALLENGES - SOLUTIONS
# =========================================================

# Bonus 1: Robust Ingestion Framework
def ingest_data(source_path, target_table, file_format, options={}):
    """Reusable ingestion framework with error handling"""
    try:
        df = spark.readStream \
            .format("cloudFiles") \
            .option("cloudFiles.format", file_format) \
            .option("cloudFiles.schemaLocation", f"/schemas/{target_table}/") \
            .options(**options) \
            .load(source_path)
        
        # Add metadata
        df_with_metadata = df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", input_file_name())
        
        # Write to table
        query = df_with_metadata.writeStream \
            .option("checkpointLocation", f"/checkpoints/{target_table}/") \
            .trigger(availableNow=True) \
            .table(target_table)
        
        query.awaitTermination()
        print(f"Successfully ingested data into {target_table}")
        
    except Exception as e:
        print(f"Error ingesting data: {str(e)}")
        raise

# Bonus 2: Schema Registry
def register_schema(table_name, schema):
    """Track schema versions"""
    schema_registry = spark.table("schema_registry") if spark.catalog.tableExists("schema_registry") else None
    
    if schema_registry is None:
        spark.sql("""
        CREATE TABLE schema_registry (
            table_name STRING,
            schema_version INT,
            schema_definition STRING,
            registered_at TIMESTAMP
        ) USING DELTA
        """)
    
    version = spark.table("schema_registry") \
        .filter(col("table_name") == table_name) \
        .count() + 1
    
    spark.createDataFrame([
        (table_name, version, str(schema), current_timestamp())
    ], ["table_name", "schema_version", "schema_definition", "registered_at"]) \
        .write.mode("append").saveAsTable("schema_registry")

# Bonus 3: Data Quality Framework
def validate_data_quality(df, rules):
    """Validate data against quality rules"""
    df_validated = df
    
    for rule in rules:
        column = rule["column"]
        check_type = rule["type"]
        
        if check_type == "not_null":
            df_validated = df_validated.withColumn(
                f"{column}_valid",
                col(column).isNotNull()
            )
        elif check_type == "range":
            min_val, max_val = rule["min"], rule["max"]
            df_validated = df_validated.withColumn(
                f"{column}_valid",
                (col(column) >= min_val) & (col(column) <= max_val)
            )
    
    return df_validated

# Bonus 4: Monitoring Dashboard
def get_ingestion_metrics(table_name):
    """Get ingestion metrics"""
    df = spark.table(table_name)
    
    metrics = {
        "total_rows": df.count(),
        "latest_ingestion": df.agg({"ingestion_timestamp": "max"}).collect()[0][0],
        "source_files": df.select("source_file").distinct().count()
    }
    
    return metrics

# Bonus 5: Multi-Format Ingestion
def universal_ingest(source_path, target_table):
    """Handle multiple file formats automatically"""
    # Detect format from file extension
    files = dbutils.fs.ls(source_path)
    if any(".csv" in f.name for f in files):
        file_format = "csv"
        options = {"header": "true"}
    elif any(".json" in f.name for f in files):
        file_format = "json"
        options = {}
    elif any(".parquet" in f.name for f in files):
        file_format = "parquet"
        options = {}
    else:
        raise ValueError("Unsupported file format")
    
    ingest_data(source_path, target_table, file_format, options)

print("All solutions completed!")
