# Day 12: Data Ingestion Patterns - Exercises
# =========================================================
# Complete these exercises to master COPY INTO and Auto Loader

# =========================================================
# PART 1: COPY INTO - BASIC USAGE
# =========================================================

# Exercise 1: Simple COPY INTO from CSV
# TODO: Use COPY INTO to load batch1.csv into customers_copy table

# YOUR CODE HERE


# Exercise 2: COPY INTO with Format Options
# TODO: Load batch2.csv with explicit format options (header=true, inferSchema=true)

# YOUR CODE HERE


# Exercise 3: COPY INTO Multiple Files
# TODO: Load all CSV files from csv/ directory at once

# YOUR CODE HERE


# Exercise 4: COPY INTO with Pattern Matching
# TODO: Load only files matching pattern 'batch*.csv'

# YOUR CODE HERE


# Exercise 5: View Loaded Files
# TODO: Query to see which files have been loaded into customers_copy

# YOUR CODE HERE


# =========================================================
# PART 2: COPY INTO - FILE FORMATS
# =========================================================

# Exercise 6: COPY INTO from JSON
# TODO: Load orders1.json into orders table

# YOUR CODE HERE


# Exercise 7: COPY INTO from Parquet
# TODO: Load products1.parquet into a new products table

# YOUR CODE HERE


# Exercise 8: COPY INTO with Custom Delimiter
# TODO: Create and load a pipe-delimited file

# YOUR CODE HERE


# Exercise 9: COPY INTO with Null Values
# TODO: Load CSV with custom null value handling

# YOUR CODE HERE


# Exercise 10: COPY INTO with Date Format
# TODO: Load CSV with custom date format specification

# YOUR CODE HERE


# =========================================================
# PART 3: COPY INTO - ADVANCED OPTIONS
# =========================================================

# Exercise 11: COPY INTO with mergeSchema
# TODO: Load batch3.csv (has extra column) with mergeSchema=true

# YOUR CODE HERE


# Exercise 12: COPY INTO with force Option
# TODO: Reload batch1.csv using force=true

# YOUR CODE HERE


# Exercise 13: COPY INTO Idempotency Test
# TODO: Run same COPY INTO twice and verify no duplicates

# YOUR CODE HERE


# Exercise 14: COPY INTO with Validation
# TODO: Load data and validate row count matches source

# YOUR CODE HERE


# Exercise 15: COPY INTO Error Handling
# TODO: Attempt to load invalid file and handle error

# YOUR CODE HERE


# =========================================================
# PART 4: AUTO LOADER - BASIC USAGE
# =========================================================

# Exercise 16: Simple Auto Loader
# TODO: Use Auto Loader to read CSV files from incremental/

# YOUR CODE HERE


# Exercise 17: Auto Loader with Schema Location
# TODO: Set up Auto Loader with schema location

# YOUR CODE HERE


# Exercise 18: Auto Loader Write to Table
# TODO: Write Auto Loader stream to customers_autoloader table

# YOUR CODE HERE


# Exercise 19: Auto Loader with Trigger Once
# TODO: Use trigger(once=True) to process files once

# YOUR CODE HERE


# Exercise 20: Auto Loader with availableNow
# TODO: Use trigger(availableNow=True) for batch-like processing

# YOUR CODE HERE


# =========================================================
# PART 5: AUTO LOADER - SCHEMA MANAGEMENT
# =========================================================

# Exercise 21: Auto Loader with Schema Inference
# TODO: Let Auto Loader infer schema automatically

# YOUR CODE HERE


# Exercise 22: Auto Loader with Explicit Schema
# TODO: Provide explicit schema to Auto Loader

# YOUR CODE HERE


# Exercise 23: Auto Loader with Schema Hints
# TODO: Use schemaHints to specify data types

# YOUR CODE HERE


# Exercise 24: Auto Loader Schema Evolution
# TODO: Enable schema evolution with addNewColumns mode

# YOUR CODE HERE


# Exercise 25: Auto Loader Rescue Data
# TODO: Use rescue mode to capture unparseable data

# YOUR CODE HERE


# =========================================================
# PART 6: AUTO LOADER - FILE FORMATS
# =========================================================

# Exercise 26: Auto Loader with JSON
# TODO: Use Auto Loader to read JSON files

# YOUR CODE HERE


# Exercise 27: Auto Loader with Parquet
# TODO: Use Auto Loader to read Parquet files

# YOUR CODE HERE


# Exercise 28: Auto Loader with Multiline JSON
# TODO: Read multiline JSON with Auto Loader

# YOUR CODE HERE


# Exercise 29: Auto Loader with Nested JSON
# TODO: Read and flatten nested JSON structure

# YOUR CODE HERE


# Exercise 30: Auto Loader File Format Options
# TODO: Specify custom format options for CSV

# YOUR CODE HERE


# =========================================================
# PART 7: AUTO LOADER - PERFORMANCE
# =========================================================

# Exercise 31: Auto Loader with maxFilesPerTrigger
# TODO: Limit files processed per trigger

# YOUR CODE HERE


# Exercise 32: Auto Loader with File Notifications
# TODO: Enable file notification mode

# YOUR CODE HERE


# Exercise 33: Auto Loader Checkpoint Management
# TODO: Set up proper checkpoint location

# YOUR CODE HERE


# Exercise 34: Auto Loader Parallel Processing
# TODO: Configure for parallel file processing

# YOUR CODE HERE


# Exercise 35: Auto Loader Monitor Progress
# TODO: Query streaming metrics

# YOUR CODE HERE


# =========================================================
# PART 8: DATA QUALITY AND VALIDATION
# =========================================================

# Exercise 36: Add Data Quality Checks
# TODO: Filter out records with null values during ingestion

# YOUR CODE HERE


# Exercise 37: Validate Email Format
# TODO: Add validation for email format

# YOUR CODE HERE


# Exercise 38: Validate Amount Range
# TODO: Ensure amount is positive

# YOUR CODE HERE


# Exercise 39: Quarantine Invalid Records
# TODO: Separate valid and invalid records

# YOUR CODE HERE


# Exercise 40: Add Ingestion Metadata
# TODO: Add columns for ingestion timestamp and source file

# YOUR CODE HERE


# =========================================================
# PART 9: INCREMENTAL PATTERNS
# =========================================================

# Exercise 41: Incremental Load with COPY INTO
# TODO: Load only new files since last run

# YOUR CODE HERE


# Exercise 42: Incremental Load with Auto Loader
# TODO: Set up continuous incremental ingestion

# YOUR CODE HERE


# Exercise 43: Watermark-based Processing
# TODO: Add watermark for late data handling

# YOUR CODE HERE


# Exercise 44: Deduplication During Ingestion
# TODO: Remove duplicates during ingestion

# YOUR CODE HERE


# Exercise 45: Upsert Pattern
# TODO: Implement upsert logic during ingestion

# YOUR CODE HERE


# =========================================================
# PART 10: SCHEMA EVOLUTION
# =========================================================

# Exercise 46: Handle New Columns
# TODO: Ingest file with additional columns

# YOUR CODE HERE


# Exercise 47: Handle Column Type Changes
# TODO: Handle schema changes gracefully

# YOUR CODE HERE


# Exercise 48: Schema Evolution with Validation
# TODO: Validate schema changes before applying

# YOUR CODE HERE


# Exercise 49: Backward Compatible Schema
# TODO: Ensure new schema is backward compatible

# YOUR CODE HERE


# Exercise 50: Schema Version Tracking
# TODO: Track schema versions over time

# YOUR CODE HERE


# =========================================================
# PART 11: ERROR HANDLING
# =========================================================

# Exercise 51: Handle Missing Files
# TODO: Gracefully handle missing source files

# YOUR CODE HERE


# Exercise 52: Handle Corrupt Records
# TODO: Skip or quarantine corrupt records

# YOUR CODE HERE


# Exercise 53: Handle Schema Mismatch
# TODO: Handle files with unexpected schema

# YOUR CODE HERE


# Exercise 54: Retry Logic
# TODO: Implement retry logic for failed ingestion

# YOUR CODE HERE


# Exercise 55: Error Logging
# TODO: Log ingestion errors to separate table

# YOUR CODE HERE


# =========================================================
# PART 12: REAL-WORLD SCENARIOS
# =========================================================

# Exercise 56: Daily Batch Ingestion
# TODO: Set up daily batch ingestion with COPY INTO

# YOUR CODE HERE


# Exercise 57: Continuous Streaming Ingestion
# TODO: Set up continuous ingestion with Auto Loader

# YOUR CODE HERE


# Exercise 58: Multi-Source Ingestion
# TODO: Ingest from multiple source directories

# YOUR CODE HERE


# Exercise 59: Partitioned Ingestion
# TODO: Ingest data and partition by date

# YOUR CODE HERE


# Exercise 60: Complete Ingestion Pipeline
# TODO: Build end-to-end ingestion pipeline with validation and monitoring

# YOUR CODE HERE


# =========================================================
# REFLECTION QUESTIONS
# =========================================================
# Answer these in comments or markdown cell:
#
# 1. When should you use COPY INTO vs Auto Loader?
# 2. What is the purpose of checkpoint location in Auto Loader?
# 3. How does COPY INTO ensure idempotency?
# 4. What are the schema evolution modes in Auto Loader?
# 5. How do you handle schema changes in COPY INTO?
# 6. What is the difference between trigger(once=True) and trigger(availableNow=True)?
# 7. How do you monitor Auto Loader progress?
# 8. What happens if you delete checkpoint location?
# 9. How do you handle corrupt or invalid records?
# 10. What are best practices for data quality during ingestion?


# =========================================================
# BONUS CHALLENGES
# =========================================================

# Bonus 1: Build Robust Ingestion Framework
# TODO: Create reusable ingestion framework with error handling

def ingest_data(source_path, target_table, file_format, options={}):
    # YOUR CODE HERE
    pass


# Bonus 2: Schema Registry
# TODO: Implement schema registry for tracking schema versions

def register_schema(table_name, schema):
    # YOUR CODE HERE
    pass


# Bonus 3: Data Quality Framework
# TODO: Build data quality validation framework

def validate_data_quality(df, rules):
    # YOUR CODE HERE
    pass


# Bonus 4: Monitoring Dashboard
# TODO: Create monitoring queries for ingestion metrics

def get_ingestion_metrics(table_name):
    # YOUR CODE HERE
    pass


# Bonus 5: Multi-Format Ingestion
# TODO: Build unified ingestion that handles multiple formats

def universal_ingest(source_path, target_table):
    # YOUR CODE HERE
    pass


# =========================================================
# CLEANUP (Optional)
# =========================================================

# Uncomment to clean up after exercises
# dbutils.fs.rm("/FileStore/day12-ingestion/", recurse=True)
# spark.sql("DROP DATABASE IF EXISTS day12_db CASCADE")


# =========================================================
# KEY LEARNINGS CHECKLIST
# =========================================================
# Mark these as you complete them:
# [ ] Used COPY INTO for batch ingestion
# [ ] Implemented Auto Loader for streaming ingestion
# [ ] Handled schema inference and evolution
# [ ] Managed checkpoints properly
# [ ] Validated data quality during ingestion
# [ ] Handled various file formats (CSV, JSON, Parquet)
# [ ] Implemented incremental ingestion patterns
# [ ] Added error handling and monitoring
# [ ] Understood idempotency guarantees
# [ ] Built production-ready ingestion pipelines
