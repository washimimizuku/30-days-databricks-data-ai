# Day 16: Structured Streaming Basics - Solutions

"""
Complete solutions for Day 16 exercises demonstrating Structured Streaming concepts.
"""

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time

# Define schema
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
# PART 1: BASIC STREAMING QUERIES
# ============================================================================

# Exercise 1: Create basic streaming reader
streaming_orders = (
    spark.readStream
    .format("json")
    .schema(orders_schema)
    .load("/tmp/streaming/input/orders/")
)
print("✓ Streaming reader created")

# Exercise 2: Display to console
console_query = (
    streaming_orders
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 10)
    .start()
)
print(f"✓ Console query started: {console_query.id}")

# Wait a bit then stop
time.sleep(10)
console_query.stop()

# Exercise 3: Write to Delta table
bronze_query = (
    streaming_orders
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/bronze_orders")
    .outputMode("append")
    .table("streaming_bronze.orders")
)
print(f"✓ Bronze query started: {bronze_query.id}")

# Exercise 4: Read from Delta as stream
delta_stream = (
    spark.readStream
    .format("delta")
    .table("streaming_bronze.orders")
)
print("✓ Reading from Delta stream")

# Exercise 5: Apply filter
filtered_stream = streaming_orders.filter(col("amount") > 100)
print("✓ Filter applied")

# Exercise 6: Add derived column
with_timestamp = streaming_orders.withColumn("processed_time", current_timestamp())
print("✓ Derived column added")

# Exercise 7: Use maxFilesPerTrigger
limited_stream = (
    spark.readStream
    .format("json")
    .schema(orders_schema)
    .option("maxFilesPerTrigger", 2)
    .load("/tmp/streaming/input/orders/")
)
print("✓ maxFilesPerTrigger configured")

# Exercise 8: Named query
named_query = (
    streaming_orders
    .writeStream
    .format("memory")
    .queryName("orders_stream")
    .outputMode("append")
    .start()
)
print(f"✓ Named query: {named_query.name}")

# Exercise 9: Check status
status = bronze_query.status
print(f"✓ Query status: {status['message']}")

# Exercise 10: Stop query
named_query.stop()
print("✓ Query stopped")

# ============================================================================
# PART 2: OUTPUT MODES
# ============================================================================

# Exercise 11: Append mode
append_query = (
    streaming_orders
    .filter(col("amount") > 0)
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/append_test")
    .table("streaming_silver.append_test")
)
print("✓ Append mode query created")

# Exercise 12: Complete mode with aggregation
complete_agg = (
    streaming_orders
    .groupBy("status")
    .count()
)

complete_query = (
    complete_agg
    .writeStream
    .format("memory")
    .queryName("status_counts")
    .outputMode("complete")
    .start()
)
print("✓ Complete mode aggregation")

# Wait and query results
time.sleep(15)
spark.sql("SELECT * FROM status_counts").show()

# Exercise 13: Update mode with aggregation
update_agg = (
    streaming_orders
    .groupBy("product_id")
    .agg(sum("amount").alias("total_amount"))
)

update_query = (
    update_agg
    .writeStream
    .format("delta")
    .outputMode("update")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/update_test")
    .table("streaming_silver.product_totals")
)
print("✓ Update mode aggregation")

# Exercise 14: Memory sink
memory_query = (
    streaming_orders
    .groupBy("payment_method")
    .count()
    .writeStream
    .format("memory")
    .queryName("payment_counts")
    .outputMode("complete")
    .start()
)
print("✓ Memory sink created")

# Exercise 15: Query memory sink
time.sleep(10)
spark.sql("SELECT * FROM payment_counts ORDER BY count DESC").show()

# Exercise 16: Append mode with aggregation (will fail)
try:
    fail_query = (
        streaming_orders
        .groupBy("status")
        .count()
        .writeStream
        .outputMode("append")  # Wrong! Aggregations need complete or update
        .format("memory")
        .queryName("will_fail")
        .start()
    )
except Exception as e:
    print(f"✓ Expected error: {str(e)[:100]}")

# Exercise 17: Complete mode for small results
small_complete = (
    streaming_orders
    .groupBy("payment_method")
    .agg(
        count("*").alias("count"),
        sum("amount").alias("total")
    )
    .writeStream
    .outputMode("complete")
    .format("memory")
    .queryName("payment_stats")
    .start()
)
print("✓ Complete mode for small cardinality")

# Exercise 18: Update mode for large results
large_update = (
    streaming_orders
    .groupBy("customer_id")
    .agg(
        count("*").alias("order_count"),
        sum("amount").alias("total_spent")
    )
    .writeStream
    .outputMode("update")
    .format("delta")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/customer_stats")
    .table("streaming_gold.customer_stats")
)
print("✓ Update mode for high cardinality")

# ============================================================================
# PART 3: TRIGGERS
# ============================================================================

# Exercise 21: Default trigger
default_trigger = (
    streaming_orders
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/default_trigger")
    .table("streaming_silver.default_trigger")
)
print("✓ Default trigger (micro-batch)")

# Exercise 22: ProcessingTime trigger
timed_trigger = (
    streaming_orders
    .writeStream
    .trigger(processingTime="10 seconds")
    .format("delta")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/timed_trigger")
    .table("streaming_silver.timed_trigger")
)
print("✓ ProcessingTime trigger: 10 seconds")

# Exercise 23: AvailableNow trigger
available_now = (
    streaming_orders
    .writeStream
    .trigger(availableNow=True)
    .format("delta")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/available_now")
    .table("streaming_silver.available_now")
)
print("✓ AvailableNow trigger")

# Wait for completion
available_now.awaitTermination()
print("✓ AvailableNow completed")

# Exercise 25: 1-minute trigger
minute_trigger = (
    streaming_orders
    .writeStream
    .trigger(processingTime="1 minute")
    .format("delta")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/minute_trigger")
    .table("streaming_silver.minute_trigger")
)
print("✓ 1-minute trigger")

# Exercise 26: Monitor trigger execution
time.sleep(20)
progress = bronze_query.lastProgress
if progress:
    print(f"✓ Trigger execution: {progress['durationMs']['triggerExecution']}ms")

# ============================================================================
# PART 4: CHECKPOINTING
# ============================================================================

# Exercise 31: Query with checkpoint
checkpoint_query = (
    streaming_orders
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/my_query")
    .table("streaming_silver.checkpoint_test")
)
print("✓ Checkpoint configured")

# Exercise 32: Unique checkpoint per query
unique_checkpoint = (
    streaming_orders
    .filter(col("status") == "completed")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/completed_orders")
    .table("streaming_silver.completed_orders")
)
print("✓ Unique checkpoint path")

# Exercise 33: Verify checkpoint contents
checkpoint_files = dbutils.fs.ls("/tmp/streaming/checkpoints/bronze_orders")
print(f"✓ Checkpoint files: {len(checkpoint_files)}")

# Exercise 34: Restart from checkpoint
# Stop query
checkpoint_query.stop()
time.sleep(5)

# Restart with same checkpoint - will resume
restarted_query = (
    streaming_orders
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/my_query")  # Same!
    .table("streaming_silver.checkpoint_test")
)
print("✓ Query restarted from checkpoint")

# Exercise 35: Clear checkpoint
dbutils.fs.rm("/tmp/streaming/checkpoints/test_checkpoint", True)
print("✓ Checkpoint cleared")

# ============================================================================
# PART 5: STREAMING TRANSFORMATIONS
# ============================================================================

# Exercise 41: Select columns
selected = streaming_orders.select("order_id", "amount", "timestamp")
print("✓ Columns selected")

# Exercise 42: Multiple filters
filtered = streaming_orders.filter(
    (col("amount") > 100) & (col("status") == "completed")
)
print("✓ Multiple filters applied")

# Exercise 43: Multiple derived columns
with_dates = (
    streaming_orders
    .withColumn("order_year", year(to_timestamp(col("timestamp"))))
    .withColumn("order_month", month(to_timestamp(col("timestamp"))))
    .withColumn("order_day", dayofmonth(to_timestamp(col("timestamp"))))
)
print("✓ Multiple derived columns")

# Exercise 44: when/otherwise
with_category = streaming_orders.withColumn(
    "price_category",
    when(col("amount") > 500, "high").otherwise("low")
)
print("✓ Conditional column added")

# Exercise 45: Cast types
with_timestamp_type = streaming_orders.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)
print("✓ Type casting applied")

# Exercise 46: Rename columns
renamed = streaming_orders.withColumnRenamed("amount", "order_amount")
print("✓ Column renamed")

# Exercise 47: Drop columns
dropped = streaming_orders.drop("payment_method")
print("✓ Column dropped")

# Exercise 48: SQL expressions
with_total = streaming_orders.selectExpr(
    "*",
    "amount * quantity as total_value"
)
print("✓ SQL expression used")

# Exercise 49: Chain transformations
chained = (
    streaming_orders
    .filter(col("amount") > 0)
    .select("order_id", "customer_id", "amount", "timestamp")
    .withColumn("processed_at", current_timestamp())
    .withColumn("amount_category", when(col("amount") > 500, "high").otherwise("low"))
)
print("✓ Transformations chained")

# ============================================================================
# PART 6: STREAM-STATIC JOINS
# ============================================================================

# Exercise 51: Join with customers
customers = spark.table("streaming_bronze.customers")

enriched_with_customers = streaming_orders.join(
    customers,
    "customer_id",
    "left"
)
print("✓ Joined with customers")

# Exercise 52: Join with products
products = spark.table("streaming_bronze.products")

enriched_with_products = streaming_orders.join(
    products,
    "product_id",
    "left"
)
print("✓ Joined with products")

# Exercise 55: Join with multiple tables
fully_enriched = (
    streaming_orders
    .join(customers, "customer_id", "left")
    .join(products, "product_id", "left")
)
print("✓ Joined with multiple tables")

# Exercise 58: Broadcast join
from pyspark.sql.functions import broadcast

broadcast_join = streaming_orders.join(
    broadcast(products),
    "product_id"
)
print("✓ Broadcast join")

# Exercise 60: Write enriched data
enriched_query = (
    fully_enriched
    .select(
        "order_id",
        "customer_id",
        "customer_name",
        "segment",
        "product_id",
        "product_name",
        "category",
        "amount",
        "quantity",
        "timestamp"
    )
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/enriched")
    .table("streaming_silver.enriched_orders")
)
print("✓ Enriched data written")

# ============================================================================
# PART 7: AGGREGATIONS
# ============================================================================

# Exercise 61: Count by status
status_counts = (
    streaming_orders
    .groupBy("status")
    .count()
    .writeStream
    .outputMode("complete")
    .format("memory")
    .queryName("status_aggregation")
    .start()
)
print("✓ Count by status")

# Exercise 62: Sum by product
product_sums = (
    streaming_orders
    .groupBy("product_id")
    .agg(sum("amount").alias("total_amount"))
    .writeStream
    .outputMode("update")
    .format("delta")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/product_sums")
    .table("streaming_gold.product_revenue")
)
print("✓ Sum by product")

# Exercise 63: Average by customer
customer_avg = (
    streaming_orders
    .groupBy("customer_id")
    .agg(avg("amount").alias("avg_order_value"))
    .writeStream
    .outputMode("update")
    .format("delta")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/customer_avg")
    .table("streaming_gold.customer_avg")
)
print("✓ Average by customer")

# Exercise 64: Multiple aggregations
multi_agg = (
    streaming_orders
    .groupBy("status")
    .agg(
        count("*").alias("order_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount")
    )
    .writeStream
    .outputMode("complete")
    .format("memory")
    .queryName("multi_aggregation")
    .start()
)
print("✓ Multiple aggregations")

# Exercise 66: Window aggregation
windowed_agg = (
    streaming_orders
    .withColumn("timestamp_typed", to_timestamp(col("timestamp")))
    .groupBy(
        window(col("timestamp_typed"), "1 hour"),
        col("status")
    )
    .count()
    .writeStream
    .outputMode("complete")
    .format("memory")
    .queryName("windowed_aggregation")
    .start()
)
print("✓ Window aggregation")

# ============================================================================
# PART 8: MONITORING
# ============================================================================

# Exercise 71: Get status
status = bronze_query.status
print(f"✓ Status: {status['message']}")

# Exercise 72: Last progress
progress = bronze_query.lastProgress
if progress:
    print(f"✓ Last batch: {progress['batchId']}, Rows: {progress['numInputRows']}")

# Exercise 73: Recent progress
recent = bronze_query.recentProgress
print(f"✓ Recent batches: {len(recent)}")

# Exercise 74: Check if active
is_active = bronze_query.isActive
print(f"✓ Query active: {is_active}")

# Exercise 75: Get ID
query_id = bronze_query.id
print(f"✓ Query ID: {query_id}")

# Exercise 76: List active queries
active_queries = spark.streams.active
print(f"✓ Active queries: {len(active_queries)}")
for q in active_queries:
    print(f"  - {q.name or q.id}")

# Exercise 80: Monitoring function
def monitor_streaming_query(query, duration=30):
    """Monitor streaming query health"""
    import time
    start = time.time()
    
    while time.time() - start < duration:
        if query.isActive:
            progress = query.lastProgress
            if progress:
                print(f"Batch {progress['batchId']}: "
                      f"{progress['numInputRows']} rows, "
                      f"{progress.get('processedRowsPerSecond', 0):.2f} rows/sec")
        time.sleep(5)
    
    return query.status

print("✓ Monitoring function created")

# ============================================================================
# BONUS CHALLENGES
# ============================================================================

# Bonus 1: End-to-end pipeline
class StreamingPipeline:
    """Complete streaming pipeline"""
    
    def __init__(self, spark):
        self.spark = spark
        self.queries = []
    
    def run_bronze(self):
        """Ingest to Bronze"""
        query = (
            spark.readStream
            .format("json")
            .schema(orders_schema)
            .load("/tmp/streaming/input/orders/")
            .withColumn("_ingestion_time", current_timestamp())
            .writeStream
            .format("delta")
            .option("checkpointLocation", "/tmp/streaming/checkpoints/pipeline_bronze")
            .table("streaming_bronze.pipeline_orders")
        )
        self.queries.append(query)
        return query
    
    def run_silver(self):
        """Transform to Silver"""
        query = (
            spark.readStream
            .table("streaming_bronze.pipeline_orders")
            .filter(col("amount") > 0)
            .withColumn("timestamp_typed", to_timestamp(col("timestamp")))
            .writeStream
            .format("delta")
            .option("checkpointLocation", "/tmp/streaming/checkpoints/pipeline_silver")
            .table("streaming_silver.pipeline_orders")
        )
        self.queries.append(query)
        return query
    
    def run_gold(self):
        """Aggregate to Gold"""
        query = (
            spark.readStream
            .table("streaming_silver.pipeline_orders")
            .groupBy("status")
            .agg(
                count("*").alias("count"),
                sum("amount").alias("total")
            )
            .writeStream
            .outputMode("complete")
            .format("delta")
            .option("checkpointLocation", "/tmp/streaming/checkpoints/pipeline_gold")
            .table("streaming_gold.pipeline_stats")
        )
        self.queries.append(query)
        return query
    
    def stop_all(self):
        """Stop all queries"""
        for q in self.queries:
            q.stop()

# Create and run pipeline
pipeline = StreamingPipeline(spark)
pipeline.run_bronze()
pipeline.run_silver()
pipeline.run_gold()

print("✓ End-to-end pipeline created")

# ============================================================================
# FINAL VERIFICATION
# ============================================================================

print("\n" + "="*80)
print("STREAMING SOLUTIONS VERIFICATION")
print("="*80)

# Active queries
print("\n=== Active Queries ===")
for q in spark.streams.active:
    print(f"{q.name or q.id}: {q.status['message']}")

# Output tables
print("\n=== Output Tables ===")
spark.sql("SHOW TABLES IN streaming_bronze").show()
spark.sql("SHOW TABLES IN streaming_silver").show()
spark.sql("SHOW TABLES IN streaming_gold").show()

# Sample data
print("\n=== Sample Bronze Data ===")
spark.table("streaming_bronze.orders").show(5)

print("\n" + "="*80)
print("✅ All solutions complete!")
print("="*80)
