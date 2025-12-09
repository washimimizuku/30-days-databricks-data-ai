# Day 16: Structured Streaming Basics - Exercises

"""
Complete these exercises to practice Structured Streaming concepts.

Setup: Run setup.md first to create streaming data sources
"""

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Define schema for exercises
orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("amount", DoubleType(), False),
    StructField("status", StringType(), False),
    StructField("payment_method", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("order_date", StringType(), False)
])

# ============================================================================
# PART 1: BASIC STREAMING QUERIES (Exercises 1-10)
# ============================================================================

# Exercise 1: Create a basic streaming reader
# Read JSON files from /tmp/streaming/input/orders/ with schema
# TODO: Your code here


# Exercise 2: Display streaming data to console
# Use format("console") to display first 10 rows
# TODO: Your code here


# Exercise 3: Write streaming data to Delta table
# Write to streaming_bronze.orders with checkpoint
# Use outputMode("append")
# TODO: Your code here


# Exercise 4: Read from Delta table as stream
# Read streaming_bronze.orders as a stream
# TODO: Your code here


# Exercise 5: Apply filter transformation
# Filter orders where amount > 100
# TODO: Your code here


# Exercise 6: Add derived column
# Add processed_time column with current_timestamp()
# TODO: Your code here


# Exercise 7: Use maxFilesPerTrigger option
# Limit processing to 2 files per trigger
# TODO: Your code here


# Exercise 8: Create streaming query with name
# Use queryName() to name your query
# TODO: Your code here


# Exercise 9: Check query status
# Get and print the status of your streaming query
# TODO: Your code here


# Exercise 10: Stop streaming query
# Stop the query gracefully
# TODO: Your code here


# ============================================================================
# PART 2: OUTPUT MODES (Exercises 11-20)
# ============================================================================

# Exercise 11: Use append mode
# Create query with outputMode("append")
# TODO: Your code here


# Exercise 12: Use complete mode with aggregation
# Count orders by status using complete mode
# TODO: Your code here


# Exercise 13: Use update mode with aggregation
# Calculate sum of amounts by product_id using update mode
# TODO: Your code here


# Exercise 14: Write aggregation to memory sink
# Use format("memory") with queryName for testing
# TODO: Your code here


# Exercise 15: Query the memory sink
# Query the in-memory table created in Exercise 14
# TODO: Your code here


# Exercise 16: Understand append mode restrictions
# Try to use append mode with aggregation (will fail)
# Observe the error message
# TODO: Your code here


# Exercise 17: Use complete mode for small results
# Aggregate by payment_method (small cardinality)
# TODO: Your code here


# Exercise 18: Use update mode for large results
# Aggregate by customer_id (high cardinality)
# TODO: Your code here


# Exercise 19: Compare output modes
# Create same aggregation with complete and update modes
# Observe the differences
# TODO: Your code here


# Exercise 20: Choose appropriate output mode
# For a filter-only query, which mode should you use?
# TODO: Your code here


# ============================================================================
# PART 3: TRIGGERS (Exercises 21-30)
# ============================================================================

# Exercise 21: Use default trigger
# Create query without specifying trigger
# TODO: Your code here


# Exercise 22: Use processingTime trigger
# Set trigger to process every 10 seconds
# TODO: Your code here


# Exercise 23: Use availableNow trigger
# Process all available data and stop
# TODO: Your code here


# Exercise 24: Compare trigger behaviors
# Create queries with different triggers and observe
# TODO: Your code here


# Exercise 25: Use 1-minute trigger interval
# Set trigger(processingTime="1 minute")
# TODO: Your code here


# Exercise 26: Monitor trigger execution time
# Check lastProgress to see trigger execution duration
# TODO: Your code here


# Exercise 27: Adjust trigger based on data volume
# If processing is slow, increase trigger interval
# TODO: Your code here


# Exercise 28: Use availableNow for backfill
# Process historical data using availableNow
# TODO: Your code here


# Exercise 29: Understand trigger trade-offs
# Fast triggers (5s) vs slow triggers (5m) - when to use each?
# TODO: Your code here


# Exercise 30: Wait for termination
# Use awaitTermination() to keep query running
# TODO: Your code here


# ============================================================================
# PART 4: CHECKPOINTING (Exercises 31-40)
# ============================================================================

# Exercise 31: Create query with checkpoint
# Specify checkpointLocation option
# TODO: Your code here


# Exercise 32: Use unique checkpoint per query
# Create checkpoint path with query name
# TODO: Your code here


# Exercise 33: Verify checkpoint contents
# List files in checkpoint directory
# TODO: Your code here


# Exercise 34: Restart query from checkpoint
# Stop and restart query - it should resume
# TODO: Your code here


# Exercise 35: Clear checkpoint
# Remove checkpoint directory
# TODO: Your code here


# Exercise 36: Handle checkpoint errors
# Try to reuse checkpoint for different query (will fail)
# TODO: Your code here


# Exercise 37: Checkpoint best practices
# Use reliable storage location for checkpoints
# TODO: Your code here


# Exercise 38: Monitor checkpoint size
# Check checkpoint directory size over time
# TODO: Your code here


# Exercise 39: Checkpoint recovery
# Simulate failure and recovery using checkpoint
# TODO: Your code here


# Exercise 40: Clean up old checkpoints
# Remove checkpoints for retired queries
# TODO: Your code here


# ============================================================================
# PART 5: STREAMING TRANSFORMATIONS (Exercises 41-50)
# ============================================================================

# Exercise 41: Select specific columns
# Select only order_id, amount, timestamp
# TODO: Your code here


# Exercise 42: Filter with multiple conditions
# Filter where amount > 100 AND status = 'completed'
# TODO: Your code here


# Exercise 43: Add multiple derived columns
# Add order_year, order_month, order_day
# TODO: Your code here


# Exercise 44: Use when/otherwise
# Add price_category: 'high' if amount > 500, else 'low'
# TODO: Your code here


# Exercise 45: Cast column types
# Convert timestamp string to timestamp type
# TODO: Your code here


# Exercise 46: Rename columns
# Rename amount to order_amount
# TODO: Your code here


# Exercise 47: Drop columns
# Drop payment_method column
# TODO: Your code here


# Exercise 48: Use SQL expressions
# Use selectExpr to calculate amount * quantity
# TODO: Your code here


# Exercise 49: Chain multiple transformations
# Filter, select, withColumn in sequence
# TODO: Your code here


# Exercise 50: Validate transformations
# Check schema and sample data after transformations
# TODO: Your code here


# ============================================================================
# PART 6: STREAM-STATIC JOINS (Exercises 51-60)
# ============================================================================

# Exercise 51: Join streaming orders with static customers
# Enrich orders with customer information
# TODO: Your code here


# Exercise 52: Join streaming orders with static products
# Enrich orders with product information
# TODO: Your code here


# Exercise 53: Perform left join
# Keep all orders even if customer not found
# TODO: Your code here


# Exercise 54: Select columns after join
# Select specific columns from both tables
# TODO: Your code here


# Exercise 55: Join with multiple tables
# Join orders with both customers and products
# TODO: Your code here


# Exercise 56: Add join condition
# Join on customer_id = customer_id
# TODO: Your code here


# Exercise 57: Handle null values after join
# Fill nulls with default values
# TODO: Your code here


# Exercise 58: Broadcast small dimension table
# Use broadcast() for efficient join
# TODO: Your code here


# Exercise 59: Monitor join performance
# Check processing time for joined query
# TODO: Your code here


# Exercise 60: Write enriched data
# Write joined result to streaming_silver.enriched_orders
# TODO: Your code here


# ============================================================================
# PART 7: AGGREGATIONS (Exercises 61-70)
# ============================================================================

# Exercise 61: Count orders by status
# Group by status and count
# TODO: Your code here


# Exercise 62: Sum amounts by product_id
# Group by product_id and sum amounts
# TODO: Your code here


# Exercise 63: Calculate average order value
# Group by customer_id and calculate avg(amount)
# TODO: Your code here


# Exercise 64: Multiple aggregations
# Count, sum, avg in single query
# TODO: Your code here


# Exercise 65: Group by multiple columns
# Group by status and payment_method
# TODO: Your code here


# Exercise 66: Use window aggregation
# Group by 1-hour time window
# TODO: Your code here


# Exercise 67: Write aggregation with complete mode
# Use outputMode("complete") for aggregation
# TODO: Your code here


# Exercise 68: Write aggregation with update mode
# Use outputMode("update") for large result
# TODO: Your code here


# Exercise 69: Query aggregation results
# Read from aggregation output table
# TODO: Your code here


# Exercise 70: Monitor aggregation performance
# Check state size and processing time
# TODO: Your code here


# ============================================================================
# PART 8: MONITORING (Exercises 71-80)
# ============================================================================

# Exercise 71: Get query status
# Print query.status
# TODO: Your code here


# Exercise 72: Get last progress
# Print query.lastProgress
# TODO: Your code here


# Exercise 73: Get recent progress
# Print query.recentProgress
# TODO: Your code here


# Exercise 74: Check if query is active
# Use query.isActive
# TODO: Your code here


# Exercise 75: Get query ID
# Print query.id
# TODO: Your code here


# Exercise 76: List all active queries
# Use spark.streams.active
# TODO: Your code here


# Exercise 77: Monitor input rate
# Check numInputRows from progress
# TODO: Your code here


# Exercise 78: Monitor processing rate
# Check processedRowsPerSecond
# TODO: Your code here


# Exercise 79: Check for exceptions
# Use query.exception()
# TODO: Your code here


# Exercise 80: Create monitoring function
# Function to monitor query health
# TODO: Your code here


# ============================================================================
# BONUS CHALLENGES (Exercises 81-85)
# ============================================================================

# Bonus 1: Create end-to-end streaming pipeline
# Bronze → Silver → Gold with transformations
# TODO: Your code here


# Bonus 2: Implement error handling
# Try-except around streaming query
# TODO: Your code here


# Bonus 3: Create reusable streaming functions
# Functions for common streaming patterns
# TODO: Your code here


# Bonus 4: Build real-time dashboard query
# Aggregation for live metrics
# TODO: Your code here


# Bonus 5: Implement streaming data quality checks
# Validate streaming data and write to DLQ
# TODO: Your code here


# ============================================================================
# VERIFICATION
# ============================================================================

print("="*80)
print("STREAMING EXERCISES VERIFICATION")
print("="*80)

# Check active queries
print("\n=== Active Streaming Queries ===")
active = spark.streams.active
print(f"Active queries: {len(active)}")
for q in active:
    print(f"  - {q.name or q.id}: {q.status['message']}")

# Check output tables
print("\n=== Output Tables ===")
tables = spark.sql("SHOW TABLES IN streaming_bronze").collect()
print(f"Bronze tables: {len(tables)}")

# Sample output
if len(tables) > 0:
    print("\n=== Sample Data ===")
    spark.table("streaming_bronze.orders").show(5)

print("\n" + "="*80)
print("✅ Exercises complete! Check solution.py for answers.")
print("="*80)
