# Databricks notebook source
# MAGIC %md
# MAGIC # Day 22: Week 3 Review - Solutions
# MAGIC 
# MAGIC ## Overview
# MAGIC Complete solutions for all Week 3 review exercises.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

USE week3_review;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Structured Streaming Basics - Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1.1: Basic Streaming Read

# COMMAND ----------

streaming_orders = spark.readStream \
    .format("json") \
    .schema("""
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        quantity INT,
        amount DECIMAL(10,2),
        order_timestamp TIMESTAMP,
        ingestion_timestamp TIMESTAMP,
        source_file STRING
    """) \
    .load("/tmp/week3_review/streaming_orders/")

print("✅ Created streaming DataFrame")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1.2: Streaming Write with Checkpoint

# COMMAND ----------

query_bronze_orders = streaming_orders.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/bronze_orders") \
    .table("bronze_orders")

print("✅ Started streaming to bronze_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1.3: Check Streaming Query Status

# COMMAND ----------

check_streaming_queries()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1.4: Trigger Once Processing

# COMMAND ----------

query_once = streaming_orders.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/once") \
    .trigger(once=True) \
    .table("bronze_orders_once")

query_once.awaitTermination()
print("✅ Processed available data once")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1.5: Output Mode Selection

# COMMAND ----------

query_customer_counts = streaming_orders \
    .groupBy("customer_id") \
    .count() \
    .writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/customer_counts") \
    .table("customer_order_counts")

print("✅ Created aggregation with complete output mode")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1.6: Streaming with Filter

# COMMAND ----------

query_high_value = streaming_orders \
    .filter(col("amount") > 100) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/high_value") \
    .table("high_value_orders")

print("✅ Created filtered streaming query")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1.7: Multiple Streaming Queries

# COMMAND ----------

# Query 1: All orders
query1 = streaming_orders.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/all_orders") \
    .queryName("all_orders_stream") \
    .table("all_orders")

# Query 2: High-value orders
query2 = streaming_orders \
    .filter(col("amount") > 100) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/high_value_2") \
    .queryName("high_value_stream") \
    .table("high_value_orders_2")

print("✅ Started two streaming queries")
check_streaming_queries()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1.8: Stop Streaming Query

# COMMAND ----------

# Stop specific query
if query_bronze_orders.isActive:
    query_bronze_orders.stop()
    print("✅ Stopped query_bronze_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1.9: Await Termination with Timeout

# COMMAND ----------

query_timeout = streaming_orders.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/timeout") \
    .table("timeout_test")

try:
    query_timeout.awaitTermination(timeout=30)
    print("✅ Query completed or timed out after 30 seconds")
except Exception as e:
    print(f"Query stopped: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1.10: Streaming Query Metrics

# COMMAND ----------

# Get metrics from active query
for query in spark.streams.active:
    print(f"\nQuery: {query.name}")
    print(f"ID: {query.id}")
    print(f"Status: {query.status}")
    
    if query.lastProgress:
        progress = query.lastProgress
        print(f"\nLast Progress:")
        print(f"  Batch ID: {progress.get('batchId', 'N/A')}")
        print(f"  Input Rows: {progress.get('numInputRows', 'N/A')}")
        print(f"  Processing Time: {progress.get('durationMs', {}).get('triggerExecution', 'N/A')} ms")
        print(f"  Input Rate: {progress.get('inputRowsPerSecond', 'N/A')} rows/sec")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Streaming Transformations - Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 2.1: Add Watermark

# COMMAND ----------

watermarked_orders = spark.readStream \
    .format("json") \
    .schema("""
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        quantity INT,
        amount DECIMAL(10,2),
        order_timestamp TIMESTAMP,
        ingestion_timestamp TIMESTAMP,
        source_file STRING
    """) \
    .load("/tmp/week3_review/streaming_orders/") \
    .withWatermark("order_timestamp", "10 minutes")

print("✅ Added 10-minute watermark")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 2.2: Tumbling Window Aggregation

# COMMAND ----------

windowed_counts = watermarked_orders \
    .groupBy(
        window("order_timestamp", "5 minutes"),
        "customer_id"
    ) \
    .count() \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/tumbling") \
    .table("tumbling_window_counts")

print("✅ Created 5-minute tumbling window aggregation")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 2.3: Sliding Window Aggregation

# COMMAND ----------

sliding_revenue = watermarked_orders \
    .groupBy(
        window("order_timestamp", "10 minutes", "5 minutes")
    ) \
    .agg(sum("amount").alias("total_revenue")) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/sliding") \
    .table("sliding_window_revenue")

print("✅ Created sliding window aggregation (10 min window, 5 min slide)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 2.4: Streaming Deduplication

# COMMAND ----------

deduplicated_orders = watermarked_orders \
    .dropDuplicates(["order_id"]) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/dedup") \
    .table("deduplicated_orders")

print("✅ Created streaming deduplication")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 2.5: Streaming Join with Static Data

# COMMAND ----------

products_df = spark.table("silver_products")

enriched_orders = watermarked_orders \
    .join(products_df, "product_id", "left") \
    .select(
        "order_id",
        "customer_id",
        "product_id",
        "product_name",
        "category",
        "quantity",
        "amount",
        "order_timestamp"
    ) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/enriched") \
    .table("enriched_orders")

print("✅ Created streaming join with products dimension")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 2.6: Multiple Aggregations in Window

# COMMAND ----------

multi_agg = watermarked_orders \
    .groupBy(window("order_timestamp", "5 minutes")) \
    .agg(
        count("*").alias("order_count"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value"),
        countDistinct("customer_id").alias("unique_customers")
    ) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/multi_agg") \
    .table("multi_aggregation_metrics")

print("✅ Created multiple aggregations in windows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 2.7-2.10: Additional Streaming Solutions

# COMMAND ----------

# Solution 2.7: Watermark with Late Data
late_data_query = watermarked_orders \
    .groupBy(window("order_timestamp", "5 minutes")) \
    .count() \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/late_data") \
    .table("late_data_test")

# Solution 2.8: Session Window
session_windows = watermarked_orders \
    .groupBy(
        session_window("order_timestamp", "30 minutes"),
        "customer_id"
    ) \
    .count() \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/session") \
    .table("session_window_counts")

# Solution 2.9: Multiple Filters
multi_filter = watermarked_orders \
    .filter((col("amount") > 50) & (col("quantity") > 1)) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/multi_filter") \
    .table("filtered_orders")

# Solution 2.10: Write Windowed Results to Gold
query_hourly = watermarked_orders \
    .groupBy(window("order_timestamp", "1 hour")) \
    .agg(
        count("*").alias("order_count"),
        sum("amount").alias("total_revenue"),
        countDistinct("customer_id").alias("unique_customers")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "order_count",
        "total_revenue",
        "unique_customers",
        current_timestamp().alias("last_updated")
    ) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/hourly_metrics") \
    .table("gold_hourly_metrics")

print("✅ Created all streaming transformation queries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Delta Lake MERGE - Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 3.1: Basic MERGE (UPSERT)

# COMMAND ----------

updates_df = spark.createDataFrame([
    ("P001", "Gaming Laptop", "Electronics", 1299.99),
    ("P011", "Tablet", "Electronics", 499.99)
], ["product_id", "product_name", "category", "price"]) \
    .withColumn("last_updated", current_timestamp())

updates_df.createOrReplaceTempView("product_updates")

spark.sql("""
    MERGE INTO silver_products t
    USING product_updates s
    ON t.product_id = s.product_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print("✅ Performed basic MERGE")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 3.2: MERGE with Deduplication

# COMMAND ----------

source_with_dupes = spark.createDataFrame([
    ("P001", "Laptop Pro", 1399.99, "2024-01-01"),
    ("P001", "Laptop Pro Max", 1499.99, "2024-01-02"),
    ("P002", "Wireless Mouse", 39.99, "2024-01-01")
], ["product_id", "product_name", "price", "update_date"])

# Deduplicate - keep latest
deduplicated = source_with_dupes \
    .withColumn("rn", row_number().over(
        Window.partitionBy("product_id").orderBy(desc("update_date"))
    )) \
    .filter(col("rn") == 1) \
    .drop("rn", "update_date") \
    .withColumn("last_updated", current_timestamp())

deduplicated.createOrReplaceTempView("deduped_updates")

spark.sql("""
    MERGE INTO silver_products t
    USING deduped_updates s
    ON t.product_id = s.product_id
    WHEN MATCHED THEN UPDATE SET 
        product_name = s.product_name,
        price = s.price,
        last_updated = s.last_updated
    WHEN NOT MATCHED THEN INSERT *
""")

print("✅ Performed MERGE with deduplication")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 3.3: Conditional UPDATE

# COMMAND ----------

spark.sql("""
    MERGE INTO silver_products t
    USING product_updates s
    ON t.product_id = s.product_id
    WHEN MATCHED AND t.price != s.price THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print("✅ Performed conditional MERGE (only update if price changed)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 3.4: MERGE with DELETE

# COMMAND ----------

updates_with_deletes = spark.createDataFrame([
    ("P001", "Laptop", "Electronics", 999.99, False),
    ("P007", "Notebook", "Office Supplies", 4.99, True)
], ["product_id", "product_name", "category", "price", "is_discontinued"])

updates_with_deletes.createOrReplaceTempView("updates_with_deletes")

spark.sql("""
    MERGE INTO silver_products t
    USING updates_with_deletes s
    ON t.product_id = s.product_id
    WHEN MATCHED AND s.is_discontinued = true THEN DELETE
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED AND s.is_discontinued = false THEN INSERT 
        (product_id, product_name, category, price, last_updated)
        VALUES (s.product_id, s.product_name, s.category, s.price, current_timestamp())
""")

print("✅ Performed MERGE with DELETE")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 3.5-3.10: Advanced MERGE Solutions

# COMMAND ----------

# Solution 3.5: Multiple Conditions
spark.sql("""
    MERGE INTO silver_products t
    USING updates_with_deletes s
    ON t.product_id = s.product_id
    WHEN MATCHED AND s.is_discontinued = true THEN DELETE
    WHEN MATCHED AND t.price != s.price THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# Solution 3.6: MERGE with Subquery
spark.sql("""
    MERGE INTO silver_products t
    USING (
        SELECT product_id, product_name, category, price
        FROM product_updates
        QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_updated DESC) = 1
    ) s
    ON t.product_id = s.product_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# Solution 3.7: MERGE with Schema Evolution
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

new_column_data = spark.createDataFrame([
    ("P001", "Laptop", "Electronics", 999.99, "In Stock")
], ["product_id", "product_name", "category", "price", "stock_status"])

new_column_data.createOrReplaceTempView("new_column_updates")

spark.sql("""
    MERGE INTO silver_products t
    USING new_column_updates s
    ON t.product_id = s.product_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print("✅ Completed advanced MERGE solutions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 3.8: Streaming MERGE with foreachBatch

# COMMAND ----------

def upsert_products(batch_df, batch_id):
    """Perform MERGE for each streaming batch"""
    # Deduplicate batch
    deduped = batch_df \
        .withColumn("rn", row_number().over(
            Window.partitionBy("product_id").orderBy(desc("last_updated"))
        )) \
        .filter(col("rn") == 1) \
        .drop("rn")
    
    # Create temp view
    deduped.createOrReplaceTempView(f"batch_{batch_id}")
    
    # Perform MERGE
    spark.sql(f"""
        MERGE INTO silver_products t
        USING batch_{batch_id} s
        ON t.product_id = s.product_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    print(f"✅ Processed batch {batch_id}")

# Apply to streaming query (example)
# streaming_products.writeStream \
#     .foreachBatch(upsert_products) \
#     .option("checkpointLocation", "/tmp/checkpoints/product_upsert") \
#     .start()

print("✅ Created foreachBatch MERGE function")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 3.9: MERGE Performance Analysis

# COMMAND ----------

# Perform MERGE
spark.sql("""
    MERGE INTO silver_products t
    USING product_updates s
    ON t.product_id = s.product_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# Analyze metrics
history = spark.sql("DESCRIBE HISTORY silver_products LIMIT 1").collect()[0]

print("MERGE Operation Metrics:")
print(f"  Operation: {history['operation']}")
print(f"  Metrics: {history['operationMetrics']}")

if history['operationMetrics']:
    metrics = eval(history['operationMetrics'])
    print(f"\n  Rows Updated: {metrics.get('numTargetRowsUpdated', 0)}")
    print(f"  Rows Inserted: {metrics.get('numTargetRowsInserted', 0)}")
    print(f"  Rows Deleted: {metrics.get('numTargetRowsDeleted', 0)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 3.10: Complex MERGE with Calculations

# COMMAND ----------

# MERGE with price increase calculation
spark.sql("""
    MERGE INTO silver_products t
    USING product_updates s
    ON t.product_id = s.product_id
    WHEN MATCHED THEN UPDATE SET
        product_name = s.product_name,
        category = s.category,
        price = s.price * 1.1,  -- 10% increase
        last_updated = current_timestamp()
    WHEN NOT MATCHED THEN INSERT
        (product_id, product_name, category, price, last_updated)
        VALUES (s.product_id, s.product_name, s.category, s.price * 1.1, current_timestamp())
""")

print("✅ Performed MERGE with calculations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Change Data Capture - Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 4.1-4.3: Process CDC Operations

# COMMAND ----------

# Read CDC events
cdc_df = spark.read.format("json").load("/tmp/week3_review/cdc_events/")

# Solution 4.1: INSERT operations
cdc_inserts = cdc_df.filter(col("operation") == "INSERT")
print(f"INSERT operations: {cdc_inserts.count()}")

# Solution 4.2: UPDATE operations
cdc_updates = cdc_df.filter(col("operation") == "UPDATE")
print(f"UPDATE operations: {cdc_updates.count()}")

# Solution 4.3: DELETE operations
cdc_deletes = cdc_df.filter(col("operation") == "DELETE")
print(f"DELETE operations: {cdc_deletes.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 4.4: CDC with MERGE

# COMMAND ----------

# Read CDC batch
cdc_batch = spark.read.format("json").load("/tmp/week3_review/cdc_events/batch_1.json")

cdc_batch.createOrReplaceTempView("cdc_batch")

spark.sql("""
    MERGE INTO silver_customers t
    USING cdc_batch s
    ON t.customer_id = s.customer_id AND t.is_current = true
    WHEN MATCHED AND s.operation = 'DELETE' THEN DELETE
    WHEN MATCHED AND s.operation = 'UPDATE' THEN UPDATE SET
        name = s.name,
        email = s.email,
        phone = s.phone,
        address = s.address,
        city = s.city,
        state = s.state,
        zip_code = s.zip_code
    WHEN NOT MATCHED AND s.operation != 'DELETE' THEN INSERT
        (customer_id, name, email, phone, address, city, state, zip_code,
         effective_start_date, effective_end_date, is_current, record_version)
        VALUES (s.customer_id, s.name, s.email, s.phone, s.address, s.city, s.state, s.zip_code,
                current_timestamp(), null, true, 1)
""")

print("✅ Processed CDC events with MERGE")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 4.5: Handle Out-of-Order CDC Events

# COMMAND ----------

# Create CDC events with different timestamps
cdc_events = spark.createDataFrame([
    ("C0001", "Customer A", "a@email.com", "UPDATE", "2024-01-03"),
    ("C0001", "Customer A Updated", "a2@email.com", "UPDATE", "2024-01-05"),  # Latest
    ("C0001", "Customer A Old", "a1@email.com", "UPDATE", "2024-01-02"),  # Out of order
    ("C0002", "Customer B", "b@email.com", "INSERT", "2024-01-01")
], ["customer_id", "name", "email", "operation", "operation_timestamp"])

# Deduplicate to get latest operation per key
latest_cdc = cdc_events \
    .withColumn("rn", row_number().over(
        Window.partitionBy("customer_id").orderBy(desc("operation_timestamp"))
    )) \
    .filter(col("rn") == 1) \
    .drop("rn")

print("✅ Handled out-of-order CDC events")
latest_cdc.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 4.6: SCD Type 1 Implementation

# COMMAND ----------

# SCD Type 1: Simple overwrite (no history)
updates = spark.createDataFrame([
    ("C0001", "Updated Name", "updated@email.com", "555-9999")
], ["customer_id", "name", "email", "phone"])

updates.createOrReplaceTempView("customer_updates")

spark.sql("""
    MERGE INTO silver_customers t
    USING customer_updates s
    ON t.customer_id = s.customer_id AND t.is_current = true
    WHEN MATCHED THEN UPDATE SET
        name = s.name,
        email = s.email,
        phone = s.phone
    WHEN NOT MATCHED THEN INSERT *
""")

print("✅ Implemented SCD Type 1")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 4.7-4.8: SCD Type 2 Implementation

# COMMAND ----------

# Solution 4.7: Close old records
updates_df = spark.createDataFrame([
    ("C0001", "New Name", "new@email.com", "555-1111", "123 New St", "New York", "NY", "10001")
], ["customer_id", "name", "email", "phone", "address", "city", "state", "zip_code"])

# Close old records
spark.sql("""
    UPDATE silver_customers
    SET is_current = false,
        effective_end_date = current_timestamp()
    WHERE customer_id IN (SELECT customer_id FROM customer_updates)
      AND is_current = true
""")

print("✅ Closed old records")

# Solution 4.8: Insert new records
new_records = updates_df \
    .withColumn("effective_start_date", current_timestamp()) \
    .withColumn("effective_end_date", lit(None).cast("timestamp")) \
    .withColumn("is_current", lit(True)) \
    .withColumn("record_version", lit(2))

new_records.write.format("delta").mode("append").saveAsTable("silver_customers")

print("✅ Inserted new records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 4.9: Complete SCD Type 2 Function

# COMMAND ----------

def apply_scd_type2(target_table, updates_df, key_col, timestamp_col):
    """
    Apply SCD Type 2 logic to target table
    
    Args:
        target_table: Name of target table
        updates_df: DataFrame with updates
        key_col: Primary key column
        timestamp_col: Timestamp column for effective dates
    """
    # Create temp view for updates
    updates_df.createOrReplaceTempView("scd_updates")
    
    # Step 1: Close old records
    spark.sql(f"""
        UPDATE {target_table}
        SET is_current = false,
            effective_end_date = current_timestamp()
        WHERE {key_col} IN (SELECT {key_col} FROM scd_updates)
          AND is_current = true
    """)
    
    # Step 2: Get max version for each key
    max_versions = spark.sql(f"""
        SELECT {key_col}, MAX(record_version) as max_version
        FROM {target_table}
        GROUP BY {key_col}
    """)
    
    # Step 3: Prepare new records
    new_records = updates_df \
        .join(max_versions, key_col, "left") \
        .withColumn("effective_start_date", current_timestamp()) \
        .withColumn("effective_end_date", lit(None).cast("timestamp")) \
        .withColumn("is_current", lit(True)) \
        .withColumn("record_version", coalesce(col("max_version") + 1, lit(1))) \
        .drop("max_version")
    
    # Step 4: Insert new records
    new_records.write.format("delta").mode("append").saveAsTable(target_table)
    
    print(f"✅ Applied SCD Type 2 to {target_table}")
    print(f"   Updated {updates_df.count()} records")

# Test the function
test_updates = spark.createDataFrame([
    ("C0002", "Updated Customer 2", "c2_new@email.com", "555-2222", "456 New Ave", "Chicago", "IL", "60601")
], ["customer_id", "name", "email", "phone", "address", "city", "state", "zip_code"])

apply_scd_type2("silver_customers", test_updates, "customer_id", "effective_start_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 4.10: Streaming CDC Processing

# COMMAND ----------

def process_cdc_batch(batch_df, batch_id):
    """Process CDC events in streaming mode"""
    # Deduplicate batch - get latest operation per key
    deduped = batch_df \
        .withColumn("rn", row_number().over(
            Window.partitionBy("customer_id").orderBy(desc("operation_timestamp"))
        )) \
        .filter(col("rn") == 1) \
        .drop("rn")
    
    # Create temp view
    deduped.createOrReplaceTempView(f"cdc_batch_{batch_id}")
    
    # Apply CDC logic with MERGE
    spark.sql(f"""
        MERGE INTO silver_customers t
        USING cdc_batch_{batch_id} s
        ON t.customer_id = s.customer_id AND t.is_current = true
        WHEN MATCHED AND s.operation = 'DELETE' THEN DELETE
        WHEN MATCHED AND s.operation = 'UPDATE' THEN UPDATE SET
            name = s.name,
            email = s.email,
            phone = s.phone,
            address = s.address,
            city = s.city,
            state = s.state,
            zip_code = s.zip_code
        WHEN NOT MATCHED AND s.operation IN ('INSERT', 'UPDATE') THEN INSERT
            (customer_id, name, email, phone, address, city, state, zip_code,
             effective_start_date, effective_end_date, is_current, record_version)
            VALUES (s.customer_id, s.name, s.email, s.phone, s.address, s.city, s.state, s.zip_code,
                    current_timestamp(), null, true, 1)
    """)
    
    print(f"✅ Processed CDC batch {batch_id}")

# Apply to streaming query
cdc_stream = spark.readStream \
    .format("json") \
    .schema("""
        customer_id STRING,
        name STRING,
        email STRING,
        phone STRING,
        address STRING,
        city STRING,
        state STRING,
        zip_code STRING,
        operation STRING,
        operation_timestamp TIMESTAMP,
        ingestion_timestamp TIMESTAMP
    """) \
    .load("/tmp/week3_review/cdc_events/")

query_cdc = cdc_stream.writeStream \
    .foreachBatch(process_cdc_batch) \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/cdc_processing") \
    .start()

print("✅ Started streaming CDC processing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Multi-Hop Architecture - Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 5.1: Bronze Layer Ingestion

# COMMAND ----------

bronze_ingestion = spark.readStream \
    .format("json") \
    .schema("""
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        quantity INT,
        amount DECIMAL(10,2),
        order_timestamp TIMESTAMP,
        ingestion_timestamp TIMESTAMP,
        source_file STRING
    """) \
    .load("/tmp/week3_review/streaming_orders/") \
    .withColumn("bronze_ingestion_time", current_timestamp()) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/bronze_layer") \
    .table("bronze_orders")

print("✅ Started Bronze layer ingestion")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 5.2: Silver Layer Cleaning

# COMMAND ----------

silver_cleaning = spark.readStream \
    .table("bronze_orders") \
    .filter(col("order_id").isNotNull()) \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("product_id").isNotNull()) \
    .filter(col("amount") > 0) \
    .filter(col("quantity") > 0) \
    .select(
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "amount",
        "order_timestamp",
        current_timestamp().alias("processed_timestamp")
    ) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/silver_cleaning") \
    .table("silver_orders")

print("✅ Started Silver layer cleaning")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 5.3: Silver Layer Deduplication

# COMMAND ----------

silver_dedup = spark.readStream \
    .table("bronze_orders") \
    .withWatermark("order_timestamp", "10 minutes") \
    .dropDuplicates(["order_id"]) \
    .filter(col("order_id").isNotNull()) \
    .filter(col("amount") > 0) \
    .select(
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "amount",
        "order_timestamp",
        current_timestamp().alias("processed_timestamp")
    ) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/silver_dedup") \
    .table("silver_orders_deduped")

print("✅ Started Silver layer with deduplication")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 5.4: Gold Layer Aggregation

# COMMAND ----------

gold_aggregation = spark.readStream \
    .table("silver_orders") \
    .groupBy(to_date("order_timestamp").alias("sale_date")) \
    .agg(
        count("*").alias("total_orders"),
        sum("amount").alias("total_revenue"),
        countDistinct("customer_id").alias("unique_customers"),
        avg("amount").alias("avg_order_value")
    ) \
    .withColumn("last_updated", current_timestamp()) \
    .writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/gold_daily") \
    .table("gold_daily_sales")

print("✅ Started Gold layer aggregation")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 5.5: End-to-End Pipeline

# COMMAND ----------

# Bronze: Ingest raw data
bronze_query = spark.readStream \
    .format("json") \
    .schema("""
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        quantity INT,
        amount DECIMAL(10,2),
        order_timestamp TIMESTAMP,
        ingestion_timestamp TIMESTAMP,
        source_file STRING
    """) \
    .load("/tmp/week3_review/streaming_orders/") \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/e2e_bronze") \
    .queryName("bronze_pipeline") \
    .table("bronze_orders_e2e")

# Silver: Clean and validate
silver_query = spark.readStream \
    .table("bronze_orders_e2e") \
    .filter(col("order_id").isNotNull()) \
    .filter(col("amount") > 0) \
    .withWatermark("order_timestamp", "10 minutes") \
    .dropDuplicates(["order_id"]) \
    .select(
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "amount",
        "order_timestamp",
        current_timestamp().alias("processed_timestamp")
    ) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/e2e_silver") \
    .queryName("silver_pipeline") \
    .table("silver_orders_e2e")

# Gold: Aggregate metrics
gold_query = spark.readStream \
    .table("silver_orders_e2e") \
    .groupBy(to_date("order_timestamp").alias("sale_date")) \
    .agg(
        count("*").alias("total_orders"),
        sum("amount").alias("total_revenue"),
        countDistinct("customer_id").alias("unique_customers"),
        avg("amount").alias("avg_order_value")
    ) \
    .withColumn("last_updated", current_timestamp()) \
    .writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/e2e_gold") \
    .queryName("gold_pipeline") \
    .table("gold_daily_sales_e2e")

print("✅ Started end-to-end Bronze → Silver → Gold pipeline")
check_streaming_queries()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 5.6-5.10: Additional Multi-Hop Solutions

# COMMAND ----------

# Solution 5.6: Incremental Processing
incremental_bronze = spark.readStream \
    .format("json") \
    .load("/tmp/week3_review/streaming_orders/") \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(once=True) \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/incremental") \
    .table("bronze_incremental")

# Solution 5.7: Data Quality Checks
def quality_check_bronze(df):
    """Bronze layer quality checks"""
    return df.filter(
        col("order_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("product_id").isNotNull()
    )

def quality_check_silver(df):
    """Silver layer quality checks"""
    return df.filter(
        (col("amount") > 0) &
        (col("amount") < 10000) &
        (col("quantity") > 0) &
        (col("quantity") < 100)
    )

# Solution 5.8: Silver Layer Enrichment
enriched_silver = spark.readStream \
    .table("silver_orders") \
    .join(spark.table("silver_products"), "product_id", "left") \
    .join(
        spark.table("silver_customers").filter(col("is_current") == True),
        "customer_id",
        "left"
    ) \
    .select(
        "order_id",
        "customer_id",
        col("name").alias("customer_name"),
        "product_id",
        col("product_name"),
        col("category"),
        "quantity",
        "amount",
        "order_timestamp"
    ) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/week3_review/checkpoints/enriched_silver") \
    .table("silver_orders_enriched")

# Solution 5.9: Multiple Gold Tables
# Customer LTV
gold_customer_ltv = spark.read.table("silver_orders") \
    .groupBy("customer_id") \
    .agg(
        count("*").alias("total_orders"),
        sum("amount").alias("total_revenue"),
        min(to_date("order_timestamp")).alias("first_order_date"),
        max(to_date("order_timestamp")).alias("last_order_date"),
        avg("amount").alias("avg_order_value")
    ) \
    .withColumn("customer_segment", 
        when(col("total_revenue") > 1000, "VIP")
        .when(col("total_revenue") > 500, "Premium")
        .otherwise("Standard")
    ) \
    .withColumn("last_updated", current_timestamp())

gold_customer_ltv.write.format("delta").mode("overwrite").saveAsTable("gold_customer_ltv")

# Product Performance
gold_product_perf = spark.read.table("silver_orders") \
    .join(spark.table("silver_products"), "product_id") \
    .groupBy("product_id", "product_name", "category") \
    .agg(
        sum("quantity").alias("total_quantity_sold"),
        sum("amount").alias("total_revenue"),
        count("*").alias("order_count"),
        avg("quantity").alias("avg_quantity_per_order")
    ) \
    .withColumn("last_updated", current_timestamp())

gold_product_perf.write.format("delta").mode("overwrite").saveAsTable("gold_product_performance")

# Solution 5.10: Monitor Pipeline Health
print("\n" + "="*60)
print("Pipeline Health Check")
print("="*60)

check_streaming_queries()

print("\nRow Counts by Layer:")
for table in ["bronze_orders", "silver_orders", "gold_daily_sales"]:
    try:
        count = spark.table(table).count()
        print(f"  {table}: {count:,} rows")
    except:
        print(f"  {table}: Not found")

print("✅ Multi-hop architecture solutions complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Optimization Techniques - Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 6.1: Analyze Table Details

# COMMAND ----------

# Check current state
details = spark.sql("DESCRIBE DETAIL silver_orders").collect()[0]

print("Table Details:")
print(f"  Location: {details['location']}")
print(f"  Number of files: {details['numFiles']}")
print(f"  Size in bytes: {details['sizeInBytes']:,}")
print(f"  Partitioned by: {details['partitionColumns']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 6.2: Check File Count

# COMMAND ----------

details = spark.sql("DESCRIBE DETAIL silver_orders").collect()[0]
num_files = details['numFiles']
size_bytes = details['sizeInBytes']

if num_files > 0:
    avg_file_size_mb = (size_bytes / num_files) / (1024 * 1024)
    print(f"Number of files: {num_files}")
    print(f"Total size: {size_bytes:,} bytes ({size_bytes / (1024**3):.2f} GB)")
    print(f"Average file size: {avg_file_size_mb:.2f} MB")
    
    if avg_file_size_mb < 100:
        print("⚠️  Small files problem detected! Consider running OPTIMIZE")
else:
    print("Table is empty")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 6.3: OPTIMIZE Table

# COMMAND ----------

# Run OPTIMIZE
spark.sql("OPTIMIZE silver_orders")

# Check results
details_after = spark.sql("DESCRIBE DETAIL silver_orders").collect()[0]
print(f"\nAfter OPTIMIZE:")
print(f"  Number of files: {details_after['numFiles']}")
print(f"  Size in bytes: {details_after['sizeInBytes']:,}")

print("✅ Table optimized")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 6.4: Z-ORDER Optimization

# COMMAND ----------

# Apply Z-ordering on frequently filtered columns
spark.sql("OPTIMIZE silver_orders ZORDER BY (customer_id, product_id)")

print("✅ Applied Z-ordering on customer_id and product_id")

# Verify in history
history = spark.sql("DESCRIBE HISTORY silver_orders LIMIT 1")
history.select("version", "operation", "operationParameters").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 6.5: Partition Table

# COMMAND ----------

# Create partitioned version
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_orders_partitioned (
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        quantity INT,
        amount DECIMAL(10,2),
        order_timestamp TIMESTAMP,
        processed_timestamp TIMESTAMP,
        order_date DATE
    )
    USING DELTA
    PARTITIONED BY (order_date)
""")

# Load data with partition column
spark.sql("""
    INSERT INTO silver_orders_partitioned
    SELECT *, to_date(order_timestamp) as order_date
    FROM silver_orders
""")

print("✅ Created partitioned table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 6.6: VACUUM Old Files

# COMMAND ----------

# VACUUM with 7 day retention (168 hours)
spark.sql("VACUUM silver_orders RETAIN 168 HOURS")

print("✅ Vacuumed old files (7 day retention)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 6.7: Analyze Optimization Impact

# COMMAND ----------

# Get before stats (from history)
history_df = spark.sql("DESCRIBE HISTORY silver_orders")
optimize_ops = history_df.filter(col("operation") == "OPTIMIZE").orderBy(desc("version")).limit(1)

if optimize_ops.count() > 0:
    metrics = optimize_ops.collect()[0]['operationMetrics']
    print("OPTIMIZE Impact:")
    print(f"  Metrics: {metrics}")
else:
    print("No OPTIMIZE operations found")

# Current stats
details = spark.sql("DESCRIBE DETAIL silver_orders").collect()[0]
print(f"\nCurrent State:")
print(f"  Files: {details['numFiles']}")
print(f"  Size: {details['sizeInBytes']:,} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 6.8-6.10: Advanced Optimization Solutions

# COMMAND ----------

# Solution 6.8: Data Skipping Statistics
spark.sql("ANALYZE TABLE silver_orders COMPUTE STATISTICS")
print("✅ Computed statistics for data skipping")

# Solution 6.9: Optimize All Tables
def optimize_all_tables(database_name):
    """Optimize all tables in a database"""
    tables = spark.sql(f"SHOW TABLES IN {database_name}").collect()
    
    for table in tables:
        table_name = f"{database_name}.{table['tableName']}"
        print(f"\nOptimizing {table_name}...")
        
        try:
            spark.sql(f"OPTIMIZE {table_name}")
            print(f"  ✅ Optimized {table_name}")
        except Exception as e:
            print(f"  ⚠️  Error optimizing {table_name}: {e}")

# Test the function
optimize_all_tables("week3_review")

# Solution 6.10: Optimization Strategy
print("\n" + "="*60)
print("Optimization Strategy for Week 3 Review Pipeline")
print("="*60)

strategy = """
1. PARTITIONING:
   - bronze_orders: Partition by ingestion_date (DATE(ingestion_timestamp))
   - silver_orders: Partition by order_date (DATE(order_timestamp))
   - gold_daily_sales: Already partitioned by sale_date
   
2. Z-ORDERING:
   - silver_orders: ZORDER BY (customer_id, product_id)
   - silver_customers: ZORDER BY (customer_id, city, state)
   - gold_customer_ltv: ZORDER BY (customer_segment, total_revenue)
   
3. OPTIMIZE SCHEDULE:
   - Bronze layer: Weekly (low priority, append-only)
   - Silver layer: Daily (high write volume)
   - Gold layer: After each update (small tables)
   
4. VACUUM SCHEDULE:
   - All layers: Weekly with 7-day retention
   - Run during low-traffic hours
   
5. MONITORING:
   - Track file counts and sizes daily
   - Alert if average file size < 100MB
   - Monitor query performance metrics
"""

print(strategy)
print("="*60)

print("\n✅ All optimization solutions complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Integration & Bonus - Solutions Summary
# MAGIC 
# MAGIC ### Complete Production Pipeline Class

# COMMAND ----------

class Week3Pipeline:
    """Production-ready incremental data processing pipeline"""
    
    def __init__(self, database="week3_review"):
        self.database = database
        self.checkpoint_base = "/tmp/week3_review/checkpoints"
        
    def start_bronze_ingestion(self):
        """Start Bronze layer ingestion"""
        return spark.readStream \
            .format("json") \
            .load("/tmp/week3_review/streaming_orders/") \
            .writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", f"{self.checkpoint_base}/bronze") \
            .table(f"{self.database}.bronze_orders")
    
    def start_silver_processing(self):
        """Start Silver layer with cleaning and deduplication"""
        return spark.readStream \
            .table(f"{self.database}.bronze_orders") \
            .withWatermark("order_timestamp", "10 minutes") \
            .dropDuplicates(["order_id"]) \
            .filter(col("amount") > 0) \
            .writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", f"{self.checkpoint_base}/silver") \
            .table(f"{self.database}.silver_orders")
    
    def start_gold_aggregation(self):
        """Start Gold layer aggregation"""
        return spark.readStream \
            .table(f"{self.database}.silver_orders") \
            .groupBy(to_date("order_timestamp").alias("sale_date")) \
            .agg(
                count("*").alias("total_orders"),
                sum("amount").alias("total_revenue")
            ) \
            .writeStream \
            .format("delta") \
            .outputMode("complete") \
            .option("checkpointLocation", f"{self.checkpoint_base}/gold") \
            .table(f"{self.database}.gold_daily_sales")
    
    def optimize_all_layers(self):
        """Optimize all tables"""
        for table in ["bronze_orders", "silver_orders", "gold_daily_sales"]:
            spark.sql(f"OPTIMIZE {self.database}.{table}")
        print("✅ Optimized all layers")

# Usage
pipeline = Week3Pipeline()
print("✅ Production pipeline class created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC ✅ Completed all Week 3 review solutions:
# MAGIC - Structured Streaming (20 exercises)
# MAGIC - Delta Lake MERGE (10 exercises)
# MAGIC - Change Data Capture (10 exercises)
# MAGIC - Multi-Hop Architecture (10 exercises)
# MAGIC - Optimization Techniques (10 exercises)
# MAGIC - Integration Patterns (10 exercises)
# MAGIC - Production Pipeline Class
# MAGIC 
# MAGIC **Next Steps:**
# MAGIC 1. Review these solutions
# MAGIC 2. Take the 50-question quiz
# MAGIC 3. Build your own end-to-end pipeline
# MAGIC 4. Move on to Week 4!
