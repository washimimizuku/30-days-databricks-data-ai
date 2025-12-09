# Day 15: Week 2 Review & ELT Project - Exercises

"""
Week 2 Integration Project: E-Commerce ELT Pipeline

Build a complete end-to-end ELT pipeline using the Medallion Architecture.
This project integrates concepts from Days 8-14.

Setup: Run setup.md first to create sample data
"""

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ============================================================================
# PART 1: BRONZE LAYER - RAW DATA INGESTION (Exercises 1-10)
# ============================================================================

# Exercise 1: Ingest orders from JSON files to Bronze
# Read JSON files from /tmp/week2_project/raw/orders/
# Add _ingestion_timestamp and _source_file columns
# Write to week2_bronze.orders
# TODO: Your code here


# Exercise 2: Ingest customers from CSV to Bronze
# Read CSV files with header from /tmp/week2_project/raw/customers/
# Add ingestion metadata
# Write to week2_bronze.customers
# TODO: Your code here


# Exercise 3: Ingest products from Parquet to Bronze
# Read Parquet files from /tmp/week2_project/raw/products/
# Add ingestion metadata
# Write to week2_bronze.products
# TODO: Your code here


# Exercise 4: Ingest nested orders using Auto Loader
# Use cloudFiles format to read /tmp/week2_project/raw/nested_orders/
# Enable schema evolution
# Write to week2_bronze.nested_orders
# TODO: Your code here


# Exercise 5: Verify Bronze layer record counts
# Count records in all Bronze tables
# TODO: Your code here


# Exercise 6: Check Bronze data quality issues
# Count nulls, invalid values in bronze.orders
# TODO: Your code here


# Exercise 7: Create Bronze ingestion function
# Create reusable function: ingest_to_bronze(source_path, format, table_name)
# TODO: Your code here


# Exercise 8: Log Bronze ingestion metrics
# Use helpers.log_pipeline_execution() to log metrics
# TODO: Your code here


# Exercise 9: Handle Bronze ingestion errors
# Wrap ingestion in try-except with error logging
# TODO: Your code here


# Exercise 10: Verify Bronze table properties
# Check table format, location, and properties
# TODO: Your code here


# ============================================================================
# PART 2: SILVER LAYER - DATA CLEANING (Exercises 11-25)
# ============================================================================

# Exercise 11: Clean orders - remove nulls
# Read bronze.orders and filter out null order_ids and customer_ids
# TODO: Your code here


# Exercise 12: Clean orders - remove duplicates
# Remove duplicate orders based on order_id
# TODO: Your code here


# Exercise 13: Clean orders - standardize dates
# Parse order_date handling multiple formats (yyyy-MM-dd HH:mm:ss, MM/dd/yyyy, yyyy-MM-dd)
# TODO: Your code here


# Exercise 14: Clean orders - validate amounts
# Filter out orders with amount <= 0
# TODO: Your code here


# Exercise 15: Clean orders - standardize status
# Convert status to lowercase
# TODO: Your code here


# Exercise 16: Clean orders - add derived columns
# Add total_amount = amount + shipping_cost
# Add discount_amount = amount * (discount_percent / 100)
# Add final_amount = total_amount - discount_amount
# TODO: Your code here


# Exercise 17: Clean customers - validate emails
# Filter customers with valid email format (contains @ and .)
# TODO: Your code here


# Exercise 18: Clean customers - standardize phone numbers
# Format phone numbers consistently
# TODO: Your code here


# Exercise 19: Enrich orders with customer data
# Join cleaned orders with customers on customer_id
# TODO: Your code here


# Exercise 20: Enrich orders with product data
# Join enriched orders with products on product_id
# TODO: Your code here


# Exercise 21: Add audit columns to Silver orders
# Add _created_at, _created_by, _updated_at columns
# TODO: Your code here


# Exercise 22: Write to Silver orders table
# Save cleaned and enriched orders to week2_silver.orders
# TODO: Your code here


# Exercise 23: Implement data quality checks for Silver
# Check for nulls, duplicates, invalid values
# Log results using helpers.log_quality_check()
# TODO: Your code here


# Exercise 24: Write failed records to DLQ
# Identify records that failed validation
# Write to dead letter queue using helpers.write_to_dlq()
# TODO: Your code here


# Exercise 25: Create Silver transformation function
# Create reusable function: transform_to_silver(bronze_table, silver_table)
# TODO: Your code here


# ============================================================================
# PART 3: WINDOW FUNCTIONS (Exercises 26-35)
# ============================================================================

# Exercise 26: Rank customers by total spending
# Use ROW_NUMBER() to rank customers by total order amount
# TODO: Your code here


# Exercise 27: Find top 3 products per category
# Use DENSE_RANK() to find top 3 products by sales in each category
# TODO: Your code here


# Exercise 28: Calculate running total of sales
# Use window function to calculate cumulative sales by date
# TODO: Your code here


# Exercise 29: Calculate 7-day moving average
# Calculate 7-day moving average of daily sales
# TODO: Your code here


# Exercise 30: Find previous order for each customer
# Use LAG() to find previous order date for each customer
# TODO: Your code here


# Exercise 31: Calculate days between orders
# Use LAG() and datediff() to calculate days between consecutive orders
# TODO: Your code here


# Exercise 32: Identify first-time vs repeat customers
# Use ROW_NUMBER() to identify first order for each customer
# TODO: Your code here


# Exercise 33: Calculate customer lifetime value
# Use window function to calculate cumulative spending per customer
# TODO: Your code here


# Exercise 34: Find percentile ranks
# Use NTILE() to divide customers into quartiles by spending
# TODO: Your code here


# Exercise 35: Calculate month-over-month growth
# Use LAG() to calculate MoM sales growth percentage
# TODO: Your code here


# ============================================================================
# PART 4: HIGHER-ORDER FUNCTIONS (Exercises 36-40)
# ============================================================================

# Exercise 36: Process nested order items
# Read bronze.nested_orders and explode items array
# TODO: Your code here


# Exercise 37: Transform prices with discount
# Use TRANSFORM to apply 10% discount to all item prices
# TODO: Your code here


# Exercise 38: Filter high-value items
# Use FILTER to keep only items with unit_price > 100
# TODO: Your code here


# Exercise 39: Calculate total per order
# Use AGGREGATE to sum item totals (quantity * unit_price)
# TODO: Your code here


# Exercise 40: Check for specific products
# Use EXISTS to check if order contains specific product_id
# TODO: Your code here


# ============================================================================
# PART 5: GOLD LAYER - BUSINESS METRICS (Exercises 41-55)
# ============================================================================

# Exercise 41: Create daily sales summary
# Aggregate by date: total_orders, total_revenue, avg_order_value
# TODO: Your code here


# Exercise 42: Create customer summary
# Aggregate by customer: total_orders, total_spent, avg_order_value, last_order_date
# TODO: Your code here


# Exercise 43: Create product performance summary
# Aggregate by product: total_quantity_sold, total_revenue, avg_price
# TODO: Your code here


# Exercise 44: Create category performance summary
# Aggregate by category: total_sales, total_orders, avg_order_value
# TODO: Your code here


# Exercise 45: Create customer segment analysis
# Aggregate by segment: customer_count, total_revenue, avg_lifetime_value
# TODO: Your code here


# Exercise 46: Create monthly revenue trend
# Aggregate by month: total_revenue, order_count, unique_customers
# TODO: Your code here


# Exercise 47: Create top customers report
# Find top 100 customers by total spending
# TODO: Your code here


# Exercise 48: Create product recommendations
# Find frequently bought together products
# TODO: Your code here


# Exercise 49: Create customer 360 view
# Combine customer info with order history and metrics
# TODO: Your code here


# Exercise 50: Write Gold tables
# Save all Gold aggregations to week2_gold database
# TODO: Your code here


# Exercise 51: Create Gold daily customer summary
# Aggregate by date and customer_id
# TODO: Your code here


# Exercise 52: Create Gold cohort analysis
# Group customers by registration month, analyze retention
# TODO: Your code here


# Exercise 53: Create Gold RFM analysis
# Calculate Recency, Frequency, Monetary scores
# TODO: Your code here


# Exercise 54: Create Gold product affinity matrix
# Calculate which products are bought together
# TODO: Your code here


# Exercise 55: Optimize Gold tables
# Apply OPTIMIZE and ZORDER to all Gold tables
# TODO: Your code here


# ============================================================================
# PART 6: DATA QUALITY & MONITORING (Exercises 56-65)
# ============================================================================

# Exercise 56: Implement comprehensive quality checks
# Check nulls, duplicates, referential integrity, value ranges
# TODO: Your code here


# Exercise 57: Create quality dashboard query
# Query monitoring.quality_metrics for latest results
# TODO: Your code here


# Exercise 58: Create pipeline metrics dashboard
# Query monitoring.pipeline_metrics for execution history
# TODO: Your code here


# Exercise 59: Analyze dead letter queue
# Query and analyze failed records
# TODO: Your code here


# Exercise 60: Calculate data quality score
# Calculate overall quality score (0-100) for each table
# TODO: Your code here


# Exercise 61: Implement data profiling
# Profile Silver tables: row counts, null rates, distinct values
# TODO: Your code here


# Exercise 62: Check referential integrity
# Verify all foreign keys exist in dimension tables
# TODO: Your code here


# Exercise 63: Validate business rules
# Check: amount > 0, quantity > 0, dates are valid
# TODO: Your code here


# Exercise 64: Monitor data freshness
# Check when each table was last updated
# TODO: Your code here


# Exercise 65: Create alerting logic
# Identify tables with quality score < 95%
# TODO: Your code here


# ============================================================================
# PART 7: PERFORMANCE OPTIMIZATION (Exercises 66-75)
# ============================================================================

# Exercise 66: Partition Silver orders by date
# Repartition silver.orders by order_date
# TODO: Your code here


# Exercise 67: Z-order Silver orders
# Apply Z-ordering on customer_id and product_id
# TODO: Your code here


# Exercise 68: Optimize file sizes
# Run OPTIMIZE on all Silver and Gold tables
# TODO: Your code here


# Exercise 69: Implement broadcast join
# Join orders with products using broadcast
# TODO: Your code here


# Exercise 70: Cache dimension tables
# Cache customers and products for multiple operations
# TODO: Your code here


# Exercise 71: Analyze table statistics
# Run ANALYZE TABLE on all tables
# TODO: Your code here


# Exercise 72: Check file size distribution
# Query DESCRIBE DETAIL to see file sizes
# TODO: Your code here


# Exercise 73: Implement incremental Gold updates
# Update Gold tables incrementally instead of full refresh
# TODO: Your code here


# Exercise 74: Add table constraints
# Add CHECK constraints to Silver tables
# TODO: Your code here


# Exercise 75: Measure query performance
# Compare query performance before and after optimization
# TODO: Your code here


# ============================================================================
# BONUS: ADVANCED CHALLENGES (Exercises 76-80)
# ============================================================================

# Bonus 1: Implement complete Medallion pipeline class
# Create class with methods for Bronze, Silver, Gold processing
# TODO: Your code here


# Bonus 2: Add SCD Type 2 for customer dimension
# Track historical changes in customer data
# TODO: Your code here


# Bonus 3: Create real-time dashboard queries
# Build queries for real-time business metrics
# TODO: Your code here


# Bonus 4: Implement data lineage tracking
# Track data flow from Bronze → Silver → Gold
# TODO: Your code here


# Bonus 5: Create automated testing framework
# Build tests to validate pipeline correctness
# TODO: Your code here


# ============================================================================
# VERIFICATION QUERIES
# ============================================================================

print("="*80)
print("WEEK 2 PROJECT VERIFICATION")
print("="*80)

# Bronze layer
print("\n=== Bronze Layer ===")
spark.sql("""
    SELECT 
        'orders' as table_name,
        COUNT(*) as record_count
    FROM week2_bronze.orders
    UNION ALL
    SELECT 
        'customers' as table_name,
        COUNT(*) as record_count
    FROM week2_bronze.customers
    UNION ALL
    SELECT 
        'products' as table_name,
        COUNT(*) as record_count
    FROM week2_bronze.products
""").show()

# Silver layer
print("\n=== Silver Layer ===")
spark.sql("""
    SELECT 
        'orders' as table_name,
        COUNT(*) as record_count
    FROM week2_silver.orders
""").show()

# Gold layer
print("\n=== Gold Layer ===")
spark.sql("SHOW TABLES IN week2_gold").show()

# Data quality
print("\n=== Data Quality Summary ===")
spark.sql("""
    SELECT 
        table_name,
        check_name,
        check_result,
        ROUND(pass_rate * 100, 2) as pass_rate_pct,
        check_timestamp
    FROM week2_monitoring.quality_metrics
    ORDER BY check_timestamp DESC
    LIMIT 10
""").show(truncate=False)

# Pipeline metrics
print("\n=== Pipeline Execution Summary ===")
spark.sql("""
    SELECT 
        pipeline_name,
        layer,
        status,
        records_processed,
        ROUND(duration_seconds, 2) as duration_sec,
        execution_timestamp
    FROM week2_monitoring.pipeline_metrics
    ORDER BY execution_timestamp DESC
    LIMIT 10
""").show(truncate=False)

# Dead letter queue
print("\n=== Dead Letter Queue ===")
dlq_count = spark.sql("SELECT COUNT(*) as failed_records FROM week2_monitoring.dead_letter_queue").collect()[0][0]
print(f"Failed records in DLQ: {dlq_count}")

print("\n" + "="*80)
print("✅ Week 2 Project Complete!")
print("="*80)
print("\nNext Steps:")
print("1. Review your implementation")
print("2. Check solution.py for reference implementations")
print("3. Take the 50-question quiz to test your knowledge")
print("4. Prepare for Week 3: Incremental Data Processing")
