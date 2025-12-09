# Day 14: ELT Best Practices - Exercises

"""
Complete these exercises to practice building production-ready ELT pipelines
using the Medallion Architecture pattern.

Setup: Run setup.md first to create sample data and helper functions
"""

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# ============================================================================
# PART 1: BRONZE LAYER - RAW DATA INGESTION (Exercises 1-10)
# ============================================================================

# Exercise 1: Create Bronze table from raw JSON files
# Read JSON files from /tmp/raw_data/orders/ and add ingestion metadata
# TODO: Your code here


# Exercise 2: Add source file tracking
# Add a column that captures the source file name for each record
# TODO: Your code here


# Exercise 3: Add ingestion timestamp
# Add a column with the current timestamp when data was ingested
# TODO: Your code here


# Exercise 4: Write to Bronze table
# Save the DataFrame to bronze.orders table using Delta format
# TODO: Your code here


# Exercise 5: Verify Bronze table
# Count records in bronze.orders and display sample
# TODO: Your code here


# Exercise 6: Check Bronze table properties
# Display the table properties and schema
# TODO: Your code here


# Exercise 7: Implement incremental Bronze ingestion
# Use Auto Loader (cloudFiles) to incrementally ingest new files
# TODO: Your code here


# Exercise 8: Add schema evolution to Bronze
# Enable schema evolution for the Bronze table
# TODO: Your code here


# Exercise 9: Create Bronze ingestion function
# Create a reusable function for Bronze layer ingestion
# def ingest_to_bronze(source_path, table_name, checkpoint_path):
#     """Ingest raw data to Bronze layer"""
#     pass
# TODO: Your code here


# Exercise 10: Test Bronze ingestion with error handling
# Wrap Bronze ingestion in try-except block with logging
# TODO: Your code here


# ============================================================================
# PART 2: SILVER LAYER - DATA CLEANING (Exercises 11-25)
# ============================================================================

# Exercise 11: Read from Bronze table
# Read all data from bronze.orders
# TODO: Your code here


# Exercise 12: Remove null order_ids
# Filter out records where order_id is null
# TODO: Your code here


# Exercise 13: Remove null customer_ids
# Filter out records where customer_id is null
# TODO: Your code here


# Exercise 14: Remove duplicate orders
# Remove duplicate records based on order_id
# TODO: Your code here


# Exercise 15: Standardize date format
# Parse order_date column handling multiple date formats
# Hint: Use transforms.parse_flexible_date()
# TODO: Your code here


# Exercise 16: Validate and fix amounts
# Filter out records with amount <= 0
# TODO: Your code here


# Exercise 17: Standardize email addresses
# Convert emails to lowercase and trim whitespace
# Hint: Use transforms.standardize_email()
# TODO: Your code here


# Exercise 18: Validate email format
# Filter out records with invalid email format
# Hint: Use transforms.validate_email()
# TODO: Your code here


# Exercise 19: Standardize status values
# Convert status to lowercase
# Hint: Use transforms.standardize_status()
# TODO: Your code here


# Exercise 20: Add audit columns
# Add _created_at and _created_by columns
# Hint: Use transforms.add_audit_columns()
# TODO: Your code here


# Exercise 21: Calculate derived columns
# Add total_amount column (quantity * amount)
# TODO: Your code here


# Exercise 22: Write to Silver table
# Save cleaned data to silver.orders
# TODO: Your code here


# Exercise 23: Implement Silver transformation function
# Create a reusable function for Silver transformations
# def transform_to_silver(bronze_table, silver_table):
#     """Transform Bronze data to Silver layer"""
#     pass
# TODO: Your code here


# Exercise 24: Add data quality checks to Silver
# Check for nulls, duplicates, and invalid values
# Log results to monitoring.data_quality_metrics
# TODO: Your code here


# Exercise 25: Implement MERGE for Silver updates
# Use MERGE to upsert new records into silver.orders
# TODO: Your code here


# ============================================================================
# PART 3: GOLD LAYER - BUSINESS AGGREGATIONS (Exercises 26-35)
# ============================================================================

# Exercise 26: Create daily order summary
# Aggregate orders by date: count, sum, avg
# TODO: Your code here


# Exercise 27: Create customer summary
# Aggregate by customer_id: total orders, total revenue, avg order value
# TODO: Your code here


# Exercise 28: Create product summary
# Aggregate by product_id: total quantity sold, total revenue
# TODO: Your code here


# Exercise 29: Create daily customer summary
# Aggregate by date and customer_id
# TODO: Your code here


# Exercise 30: Join with customer dimension
# Enrich Gold data with customer segment information
# TODO: Your code here


# Exercise 31: Join with product dimension
# Enrich Gold data with product category information
# TODO: Your code here


# Exercise 32: Create customer 360 view
# Combine customer info with order history and metrics
# TODO: Your code here


# Exercise 33: Write to Gold table
# Save aggregated data to gold.daily_customer_summary
# TODO: Your code here


# Exercise 34: Create Gold aggregation function
# Create a reusable function for Gold layer aggregations
# def aggregate_to_gold(silver_table, gold_table, group_cols, agg_exprs):
#     """Aggregate Silver data to Gold layer"""
#     pass
# TODO: Your code here


# Exercise 35: Optimize Gold table
# Apply OPTIMIZE and ZORDER to gold.daily_customer_summary
# TODO: Your code here


# ============================================================================
# PART 4: DATA QUALITY AND VALIDATION (Exercises 36-45)
# ============================================================================

# Exercise 36: Create schema validation function
# Validate DataFrame schema matches expected schema
# TODO: Your code here


# Exercise 37: Implement null check
# Count null values in each column
# TODO: Your code here


# Exercise 38: Implement duplicate check
# Count duplicate records based on key columns
# TODO: Your code here


# Exercise 39: Implement value range check
# Check if numeric values are within expected ranges
# TODO: Your code here


# Exercise 40: Implement referential integrity check
# Verify all customer_ids exist in customer dimension
# TODO: Your code here


# Exercise 41: Calculate data quality score
# Create function to calculate overall quality score (0-1)
# TODO: Your code here


# Exercise 42: Log quality metrics
# Write quality check results to monitoring.data_quality_metrics
# TODO: Your code here


# Exercise 43: Implement quality check function
# Create comprehensive quality check function
# def run_quality_checks(df, table_name, checks):
#     """Run data quality checks and log results"""
#     pass
# TODO: Your code here


# Exercise 44: Add table constraints
# Add CHECK constraints to silver.orders table
# TODO: Your code here


# Exercise 45: Verify constraints
# Query table properties to verify constraints are applied
# TODO: Your code here


# ============================================================================
# PART 5: PIPELINE ORCHESTRATION (Exercises 46-55)
# ============================================================================

# Exercise 46: Create pipeline execution logger
# Log pipeline start, end, duration, and status
# TODO: Your code here


# Exercise 47: Implement error handling wrapper
# Create function to wrap pipeline execution with error handling
# def safe_pipeline_execution(pipeline_func, *args, **kwargs):
#     """Execute pipeline with error handling"""
#     pass
# TODO: Your code here


# Exercise 48: Create dead letter queue handler
# Write failed records to monitoring.dead_letter_queue
# TODO: Your code here


# Exercise 49: Implement retry logic
# Add retry logic for transient failures
# TODO: Your code here


# Exercise 50: Create complete pipeline class
# Build MedallionPipeline class with all layers
# class MedallionPipeline:
#     def __init__(self, spark, source_path, checkpoint_base):
#         pass
#     
#     def run_bronze_ingestion(self):
#         pass
#     
#     def run_silver_transformation(self):
#         pass
#     
#     def run_gold_aggregation(self):
#         pass
#     
#     def run_full_pipeline(self):
#         pass
# TODO: Your code here


# Exercise 51: Add pipeline monitoring
# Track pipeline execution metrics in monitoring.pipeline_metrics
# TODO: Your code here


# Exercise 52: Implement idempotent daily batch
# Create function to process specific date partition idempotently
# TODO: Your code here


# Exercise 53: Add checkpoint management
# Create functions to list and clear checkpoints
# TODO: Your code here


# Exercise 54: Implement pipeline validation
# Validate pipeline configuration before execution
# TODO: Your code here


# Exercise 55: Create pipeline status dashboard
# Query monitoring tables to show pipeline health
# TODO: Your code here


# ============================================================================
# PART 6: PERFORMANCE OPTIMIZATION (Exercises 56-65)
# ============================================================================

# Exercise 56: Partition Silver table by date
# Repartition silver.orders by order_date
# TODO: Your code here


# Exercise 57: Apply Z-ordering
# Z-order silver.orders by customer_id and order_date
# TODO: Your code here


# Exercise 58: Optimize file sizes
# Run OPTIMIZE on silver.orders
# TODO: Your code here


# Exercise 59: Implement broadcast join
# Join orders with products using broadcast
# TODO: Your code here


# Exercise 60: Cache frequently used data
# Cache customer dimension for multiple operations
# TODO: Your code here


# Exercise 61: Analyze table statistics
# Run ANALYZE TABLE to update statistics
# TODO: Your code here


# Exercise 62: Check file sizes
# Query table details to see file size distribution
# TODO: Your code here


# Exercise 63: Implement incremental aggregation
# Update Gold table incrementally instead of full refresh
# TODO: Your code here


# Exercise 64: Add partition pruning
# Query with partition filter to demonstrate pruning
# TODO: Your code here


# Exercise 65: Measure query performance
# Compare query performance before and after optimization
# TODO: Your code here


# ============================================================================
# PART 7: NAMING AND DOCUMENTATION (Exercises 66-70)
# ============================================================================

# Exercise 66: Standardize column names
# Convert all column names to snake_case
# TODO: Your code here


# Exercise 67: Add table comments
# Add descriptive comments to all tables
# TODO: Your code here


# Exercise 68: Add column comments
# Add comments to key columns in silver.orders
# TODO: Your code here


# Exercise 69: Create data dictionary
# Generate data dictionary from table metadata
# TODO: Your code here


# Exercise 70: Document pipeline lineage
# Create documentation showing data flow between layers
# TODO: Your code here


# ============================================================================
# BONUS CHALLENGES (Exercises 71-75)
# ============================================================================

# Bonus 1: Implement SCD Type 2
# Track historical changes in customer dimension
# TODO: Your code here


# Bonus 2: Create data quality dashboard
# Build comprehensive quality metrics dashboard
# TODO: Your code here


# Bonus 3: Implement data sampling
# Create stratified sample for testing
# TODO: Your code here


# Bonus 4: Add data masking
# Mask sensitive data (email, phone) in lower environments
# TODO: Your code here


# Bonus 5: Create pipeline orchestration DAG
# Define dependencies between pipeline tasks
# TODO: Your code here


# ============================================================================
# VERIFICATION QUERIES
# ============================================================================

# Verify Bronze layer
print("=== Bronze Layer ===")
spark.sql("SELECT COUNT(*) as bronze_count FROM bronze.orders").show()

# Verify Silver layer
print("\n=== Silver Layer ===")
spark.sql("SELECT COUNT(*) as silver_count FROM silver.orders").show()

# Verify Gold layer
print("\n=== Gold Layer ===")
spark.sql("SELECT COUNT(*) as gold_count FROM gold.daily_customer_summary").show()

# Check data quality metrics
print("\n=== Data Quality Metrics ===")
spark.sql("""
    SELECT table_name, check_name, check_result, 
           ROUND(metric_value, 4) as pass_rate
    FROM monitoring.data_quality_metrics
    ORDER BY check_timestamp DESC
    LIMIT 10
""").show()

# Check pipeline metrics
print("\n=== Pipeline Metrics ===")
spark.sql("""
    SELECT pipeline_name, status, 
           ROUND(duration_seconds, 2) as duration_sec,
           execution_timestamp
    FROM monitoring.pipeline_metrics
    ORDER BY execution_timestamp DESC
    LIMIT 10
""").show()

print("\n✅ Exercises complete! Check solution.py for answers.")
