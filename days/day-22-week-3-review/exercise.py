# Databricks notebook source
# MAGIC %md
# MAGIC # Day 22: Week 3 Review - Exercises
# MAGIC 
# MAGIC ## Overview
# MAGIC These exercises review all concepts from Week 3 (Days 16-21):
# MAGIC - Structured Streaming Basics
# MAGIC - Streaming Transformations
# MAGIC - Delta Lake MERGE
# MAGIC - Change Data Capture (CDC)
# MAGIC - Multi-Hop Architecture
# MAGIC - Optimization Techniques
# MAGIC 
# MAGIC Complete all exercises to solidify your understanding of incremental data processing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the setup from `setup.md` first, then verify your environment:

# COMMAND ----------

# Verify setup
USE week3_review;

# Check tables
SHOW TABLES;

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Verify helper functions are available
print("Helper functions loaded:")
print("  - show_table_stats()")
print("  - check_streaming_queries()")
print("  - simulate_new_orders()")
print("  - simulate_cdc_events()")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Structured Streaming Basics (10 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.1: Basic Streaming Read
# MAGIC Create a streaming DataFrame that reads from the streaming_orders directory

# COMMAND ----------

# TODO: Create streaming read from /tmp/week3_review/streaming_orders/
streaming_orders = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.2: Streaming Write with Checkpoint
# MAGIC Write the streaming orders to bronze_orders table with proper checkpointing

# COMMAND ----------

# TODO: Write stream to bronze_orders with checkpoint at /tmp/week3_review/checkpoints/bronze_orders
query_bronze_orders = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.3: Check Streaming Query Status
# MAGIC Display the status of your streaming query

# COMMAND ----------

# TODO: Check streaming query status
check_streaming_queries()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.4: Trigger Once Processing
# MAGIC Create a streaming query that processes available data once and stops

# COMMAND ----------

# TODO: Create a trigger once query to process orders
query_once = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.5: Output Mode Selection
# MAGIC Create a streaming aggregation with the appropriate output mode

# COMMAND ----------

# TODO: Count orders by customer_id using complete output mode
query_customer_counts = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.6: Streaming with Filter
# MAGIC Create a streaming query that only processes orders above $100

# COMMAND ----------

# TODO: Filter orders where amount > 100 and write to a filtered table
query_high_value = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.7: Multiple Streaming Queries
# MAGIC Create two streaming queries running simultaneously

# COMMAND ----------

# TODO: Create two queries - one for all orders, one for high-value orders
query1 = None
query2 = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.8: Stop Streaming Query
# MAGIC Stop a specific streaming query by name

# COMMAND ----------

# TODO: Stop the query_bronze_orders query
# query_bronze_orders.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.9: Await Termination with Timeout
# MAGIC Start a query and wait for it with a timeout

# COMMAND ----------

# TODO: Start a query and await termination with 30 second timeout
query_timeout = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.10: Streaming Query Metrics
# MAGIC Extract and display metrics from a streaming query

# COMMAND ----------

# TODO: Get the last progress from an active query and display key metrics
# Hint: query.lastProgress

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Streaming Transformations (10 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.1: Add Watermark
# MAGIC Add a 10-minute watermark on order_timestamp

# COMMAND ----------

# TODO: Read streaming orders and add watermark
watermarked_orders = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.2: Tumbling Window Aggregation
# MAGIC Count orders in 5-minute tumbling windows

# COMMAND ----------

# TODO: Create 5-minute tumbling window aggregation
windowed_counts = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.3: Sliding Window Aggregation
# MAGIC Calculate total revenue in 10-minute windows, sliding every 5 minutes

# COMMAND ----------

# TODO: Create sliding window aggregation (10 min window, 5 min slide)
sliding_revenue = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.4: Streaming Deduplication
# MAGIC Remove duplicate orders based on order_id with watermarking

# COMMAND ----------

# TODO: Deduplicate streaming orders using order_id
deduplicated_orders = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.5: Streaming Join with Static Data
# MAGIC Join streaming orders with the products dimension table

# COMMAND ----------

# TODO: Join streaming orders with silver_products
enriched_orders = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.6: Multiple Aggregations in Window
# MAGIC Calculate count, sum, and average in 5-minute windows

# COMMAND ----------

# TODO: Create multiple aggregations in windows
multi_agg = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.7: Watermark with Late Data
# MAGIC Process data with watermark and observe late data handling

# COMMAND ----------

# TODO: Create query with watermark and test with late data
late_data_query = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.8: Session Window
# MAGIC Create session windows with 30-minute gaps

# COMMAND ----------

# TODO: Create session window aggregation
session_windows = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.9: Streaming with Multiple Filters
# MAGIC Apply multiple filters in a streaming pipeline

# COMMAND ----------

# TODO: Filter by amount > 50 AND quantity > 1
multi_filter = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.10: Write Windowed Results
# MAGIC Write windowed aggregation results to gold_hourly_metrics

# COMMAND ----------

# TODO: Write hourly metrics to gold table
query_hourly = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Delta Lake MERGE (10 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.1: Basic MERGE (UPSERT)
# MAGIC Perform a basic UPSERT on silver_products

# COMMAND ----------

# TODO: Create updates for products and MERGE into silver_products
updates_df = spark.createDataFrame([
    ("P001", "Gaming Laptop", "Electronics", 1299.99),
    ("P011", "Tablet", "Electronics", 499.99)
], ["product_id", "product_name", "category", "price"])

# TODO: Write MERGE statement

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.2: MERGE with Deduplication
# MAGIC Deduplicate source data before MERGE

# COMMAND ----------

# TODO: Create source with duplicates, deduplicate, then MERGE
source_with_dupes = spark.createDataFrame([
    ("P001", "Laptop Pro", 1399.99, "2024-01-01"),
    ("P001", "Laptop Pro Max", 1499.99, "2024-01-02"),  # Latest
    ("P002", "Wireless Mouse", 39.99, "2024-01-01")
], ["product_id", "product_name", "price", "update_date"])

# TODO: Deduplicate and MERGE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.3: Conditional UPDATE
# MAGIC Only update if the source price is different

# COMMAND ----------

# TODO: MERGE with condition: only update if price changed
# WHEN MATCHED AND t.price != s.price THEN UPDATE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.4: MERGE with DELETE
# MAGIC Delete records where a flag is set

# COMMAND ----------

# TODO: MERGE with DELETE clause for discontinued products
updates_with_deletes = spark.createDataFrame([
    ("P001", "Laptop", "Electronics", 999.99, False),
    ("P007", "Notebook", "Office Supplies", 4.99, True)  # Discontinued
], ["product_id", "product_name", "category", "price", "is_discontinued"])

# TODO: MERGE with DELETE when is_discontinued = true

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.5: MERGE with Multiple Conditions
# MAGIC Use multiple WHEN MATCHED clauses with different conditions

# COMMAND ----------

# TODO: MERGE with:
# - DELETE if is_discontinued
# - UPDATE if price changed
# - Do nothing if no changes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.6: MERGE Using Subquery
# MAGIC Use a subquery in the USING clause

# COMMAND ----------

# TODO: MERGE using a subquery that aggregates data
# Example: Latest price per product from a history table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.7: MERGE with Schema Evolution
# MAGIC Add a new column during MERGE

# COMMAND ----------

# TODO: Enable schema evolution and MERGE with new column
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# TODO: MERGE data with additional column

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.8: Streaming MERGE with foreachBatch
# MAGIC Use foreachBatch to perform MERGE in streaming

# COMMAND ----------

# TODO: Create foreachBatch function that performs MERGE
def upsert_products(batch_df, batch_id):
    # TODO: Implement MERGE logic
    pass

# TODO: Apply to streaming query

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.9: MERGE Performance Analysis
# MAGIC Analyze MERGE operation metrics

# COMMAND ----------

# TODO: Perform MERGE and analyze metrics from DESCRIBE HISTORY
# Look at numTargetRowsUpdated, numTargetRowsInserted, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.10: Complex MERGE with Calculations
# MAGIC Perform calculations in the UPDATE clause

# COMMAND ----------

# TODO: MERGE that calculates new values
# Example: Update price with percentage increase

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Change Data Capture (10 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.1: Process INSERT Operations
# MAGIC Process only INSERT CDC events

# COMMAND ----------

# TODO: Read CDC events and filter for INSERT operations
cdc_inserts = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.2: Process UPDATE Operations
# MAGIC Process only UPDATE CDC events

# COMMAND ----------

# TODO: Read CDC events and filter for UPDATE operations
cdc_updates = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.3: Process DELETE Operations
# MAGIC Process only DELETE CDC events

# COMMAND ----------

# TODO: Read CDC events and filter for DELETE operations
cdc_deletes = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.4: CDC with MERGE
# MAGIC Process all CDC operations using MERGE

# COMMAND ----------

# TODO: Read CDC batch and apply using MERGE
# Handle INSERT, UPDATE, DELETE in one MERGE statement

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.5: Handle Out-of-Order CDC Events
# MAGIC Process CDC events that arrive out of order

# COMMAND ----------

# TODO: Create CDC events with different timestamps
# Deduplicate to get latest operation per key

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.6: SCD Type 1 Implementation
# MAGIC Implement SCD Type 1 (overwrite) for customer updates

# COMMAND ----------

# TODO: Simple MERGE that overwrites existing records
# No history tracking

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.7: SCD Type 2 - Close Old Records
# MAGIC Close old records when updates arrive

# COMMAND ----------

# TODO: Update existing records to set is_current = false and effective_end_date

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.8: SCD Type 2 - Insert New Records
# MAGIC Insert new versions of records

# COMMAND ----------

# TODO: Insert new records with is_current = true and new effective_start_date

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.9: Complete SCD Type 2 Function
# MAGIC Create a reusable function for SCD Type 2 processing

# COMMAND ----------

# TODO: Create function that handles complete SCD Type 2 logic
def apply_scd_type2(target_table, updates_df, key_col, timestamp_col):
    """
    Apply SCD Type 2 logic to target table
    
    Args:
        target_table: Name of target table
        updates_df: DataFrame with updates
        key_col: Primary key column
        timestamp_col: Timestamp column for effective dates
    """
    # TODO: Implement complete SCD Type 2 logic
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.10: Streaming CDC Processing
# MAGIC Process CDC events in streaming mode

# COMMAND ----------

# TODO: Create streaming CDC processor
def process_cdc_batch(batch_df, batch_id):
    # TODO: Implement CDC processing logic
    pass

# TODO: Apply to streaming query

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Multi-Hop Architecture (10 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.1: Bronze Layer Ingestion
# MAGIC Ingest raw data into Bronze layer

# COMMAND ----------

# TODO: Create streaming ingestion to bronze_orders
bronze_ingestion = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.2: Silver Layer Cleaning
# MAGIC Clean and validate data in Silver layer

# COMMAND ----------

# TODO: Read from bronze, clean data, write to silver
# Remove nulls, validate data types, add processed_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.3: Silver Layer Deduplication
# MAGIC Deduplicate data in Silver layer

# COMMAND ----------

# TODO: Deduplicate orders by order_id before writing to silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.4: Gold Layer Aggregation
# MAGIC Create daily aggregates in Gold layer

# COMMAND ----------

# TODO: Aggregate silver_orders to create gold_daily_sales

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.5: End-to-End Pipeline
# MAGIC Create complete Bronze → Silver → Gold pipeline

# COMMAND ----------

# TODO: Create three streaming queries for complete pipeline
bronze_query = None
silver_query = None
gold_query = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.6: Incremental Processing
# MAGIC Implement incremental processing for batch updates

# COMMAND ----------

# TODO: Use trigger(once=True) for incremental batch processing

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.7: Data Quality Checks
# MAGIC Add data quality checks at each layer

# COMMAND ----------

# TODO: Add quality checks:
# - Bronze: Check for required fields
# - Silver: Validate data types and ranges
# - Gold: Verify aggregation logic

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.8: Silver Layer Enrichment
# MAGIC Enrich Silver data with dimension tables

# COMMAND ----------

# TODO: Join silver_orders with silver_products and silver_customers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.9: Gold Layer Multiple Aggregations
# MAGIC Create multiple Gold tables from Silver

# COMMAND ----------

# TODO: Create gold_customer_ltv and gold_product_performance from silver_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.10: Monitor Pipeline Health
# MAGIC Create monitoring for the multi-hop pipeline

# COMMAND ----------

# TODO: Check all streaming queries and display metrics
check_streaming_queries()

# Show row counts at each layer
for table in ["bronze_orders", "silver_orders", "gold_daily_sales"]:
    count = spark.table(table).count()
    print(f"{table}: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Optimization Techniques (10 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.1: Analyze Table Details
# MAGIC Check current state of silver_orders table

# COMMAND ----------

# TODO: Use DESCRIBE DETAIL to analyze table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.2: Check File Count
# MAGIC Identify small files problem

# COMMAND ----------

# TODO: Count files and calculate average file size

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.3: OPTIMIZE Table
# MAGIC Compact small files using OPTIMIZE

# COMMAND ----------

# TODO: Run OPTIMIZE on silver_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.4: Z-ORDER Optimization
# MAGIC Apply Z-ordering on frequently filtered columns

# COMMAND ----------

# TODO: OPTIMIZE with ZORDER BY customer_id, product_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.5: Partition Table
# MAGIC Create a partitioned version of orders table

# COMMAND ----------

# TODO: Create partitioned table by order date

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.6: VACUUM Old Files
# MAGIC Clean up old files with VACUUM

# COMMAND ----------

# TODO: VACUUM silver_orders with 7 day retention

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.7: Analyze Optimization Impact
# MAGIC Compare table stats before and after optimization

# COMMAND ----------

# TODO: Compare DESCRIBE DETAIL results before/after OPTIMIZE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.8: Data Skipping Statistics
# MAGIC Check data skipping statistics

# COMMAND ----------

# TODO: Query data skipping stats from table metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.9: Optimize All Tables
# MAGIC Create a function to optimize all tables in the database

# COMMAND ----------

# TODO: Create function to optimize all tables
def optimize_all_tables(database_name):
    """Optimize all tables in a database"""
    # TODO: Implement
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.10: Optimization Strategy
# MAGIC Design complete optimization strategy for the pipeline

# COMMAND ----------

# TODO: Create optimization plan:
# 1. Which tables to partition?
# 2. Which columns to Z-order?
# 3. OPTIMIZE schedule?
# 4. VACUUM schedule?

# Document your strategy here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Integration Exercises (10 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 7.1: Streaming + MERGE Pipeline
# MAGIC Combine streaming with MERGE for real-time UPSERT

# COMMAND ----------

# TODO: Create streaming query that uses foreachBatch with MERGE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 7.2: CDC + Multi-Hop
# MAGIC Process CDC through medallion architecture

# COMMAND ----------

# TODO: Bronze CDC → Silver SCD Type 2 → Gold aggregates

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 7.3: Streaming + Watermark + MERGE
# MAGIC Combine watermarking with MERGE operations

# COMMAND ----------

# TODO: Add watermark to streaming CDC processing

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 7.4: Optimized Multi-Hop
# MAGIC Apply optimization to each layer of medallion architecture

# COMMAND ----------

# TODO: Optimize bronze, silver, and gold tables appropriately

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 7.5: Complete Real-Time Analytics Pipeline
# MAGIC Build end-to-end real-time analytics system

# COMMAND ----------

# TODO: Create complete pipeline:
# 1. Stream ingestion (Bronze)
# 2. Real-time cleaning (Silver)
# 3. Live aggregations (Gold)
# 4. Optimization at each layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 7.6: Handle Late Data in Multi-Hop
# MAGIC Implement late data handling across all layers

# COMMAND ----------

# TODO: Add watermarking at each layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 7.7: SCD Type 2 with Streaming
# MAGIC Implement streaming SCD Type 2 processing

# COMMAND ----------

# TODO: Stream CDC events and apply SCD Type 2 in foreachBatch

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 7.8: Performance Monitoring
# MAGIC Monitor performance of the complete pipeline

# COMMAND ----------

# TODO: Create dashboard showing:
# - Streaming query metrics
# - Table sizes and file counts
# - Processing latency
# - Data quality metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 7.9: Error Handling
# MAGIC Add error handling to the pipeline

# COMMAND ----------

# TODO: Add try-catch blocks and error logging

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 7.10: Production-Ready Pipeline
# MAGIC Package everything into a production-ready solution

# COMMAND ----------

# TODO: Create production pipeline with:
# - Proper error handling
# - Monitoring and alerting
# - Optimization schedule
# - Documentation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Challenges (5 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 1: Stream-Stream Join
# MAGIC Join two streaming DataFrames

# COMMAND ----------

# TODO: Join streaming orders with streaming customer events

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 2: Complex Window Aggregations
# MAGIC Create multiple window types in one query

# COMMAND ----------

# TODO: Combine tumbling, sliding, and session windows

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 3: Custom CDC Logic
# MAGIC Implement custom CDC processing for complex scenarios

# COMMAND ----------

# TODO: Handle CDC with multiple operations per key in same batch

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 4: Dynamic Optimization
# MAGIC Create auto-optimization based on table statistics

# COMMAND ----------

# TODO: Analyze table and automatically apply appropriate optimizations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 5: Complete E-Commerce Analytics
# MAGIC Build comprehensive e-commerce analytics platform

# COMMAND ----------

# TODO: Create complete solution with:
# - Real-time order processing
# - Customer behavior tracking
# - Product performance analytics
# - Inventory management
# - All optimizations applied

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC You've completed 75 exercises covering:
# MAGIC - ✅ Structured Streaming (20 exercises)
# MAGIC - ✅ Delta Lake MERGE (10 exercises)
# MAGIC - ✅ Change Data Capture (10 exercises)
# MAGIC - ✅ Multi-Hop Architecture (10 exercises)
# MAGIC - ✅ Optimization Techniques (10 exercises)
# MAGIC - ✅ Integration Patterns (10 exercises)
# MAGIC - ✅ Bonus Challenges (5 exercises)
# MAGIC 
# MAGIC Next steps:
# MAGIC 1. Review your solutions
# MAGIC 2. Take the 50-question quiz
# MAGIC 3. Identify areas for additional practice
# MAGIC 4. Move on to Week 4!
