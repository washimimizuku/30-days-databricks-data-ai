# Day 16: Structured Streaming Basics

## Learning Objectives

By the end of today, you will:
- Understand Structured Streaming concepts and architecture
- Create streaming queries using readStream and writeStream
- Work with different output modes (append, complete, update)
- Implement checkpointing for fault tolerance
- Use triggers to control processing frequency
- Monitor streaming queries
- Handle streaming data sources and sinks
- Apply transformations to streaming DataFrames

## Topics Covered

### 1. Structured Streaming Fundamentals

Structured Streaming is a scalable and fault-tolerant stream processing engine built on Spark SQL. It treats streaming data as an unbounded table that continuously grows.

#### Key Concepts

**Streaming DataFrame**: A DataFrame that represents an unbounded table
**Source**: Where streaming data comes from (files, Kafka, Delta, etc.)
**Sink**: Where streaming data is written (files, Delta, console, etc.)
**Trigger**: When to process new data
**Checkpoint**: Location to track progress for fault tolerance
**Watermark**: Mechanism to handle late-arriving data

#### Streaming vs Batch

```python
# Batch processing
batch_df = spark.read.format("json").load("/data/batch/")

# Streaming processing
streaming_df = spark.readStream.format("json").load("/data/streaming/")
```

The API is nearly identical - the key difference is `readStream` vs `read`.

### 2. Creating Streaming Queries

#### Basic Streaming Query

```python
from pyspark.sql.functions import *

# Read streaming data
streaming_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/tmp/schema")
    .load("/data/input/")
)

# Apply transformations (same as batch!)
transformed_df = (
    streaming_df
    .filter(col("amount") > 0)
    .withColumn("processed_time", current_timestamp())
)

# Write streaming data
query = (
    transformed_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/checkpoint")
    .outputMode("append")
    .table("output_table")
)

# Wait for termination
query.awaitTermination()
```

#### readStream Options

```python
# File source
df = (
    spark.readStream
    .format("json")  # or csv, parquet, delta
    .schema(schema)  # Schema is required for file sources
    .option("maxFilesPerTrigger", 1000)  # Rate limiting
    .load("/path/to/files")
)

# Delta source
df = (
    spark.readStream
    .format("delta")
    .option("ignoreChanges", "true")  # Ignore updates/deletes
    .option("ignoreDeletes", "true")  # Ignore deletes only
    .table("source_table")
)

# Kafka source
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "host:port")
    .option("subscribe", "topic")
    .load()
)
```

### 3. Output Modes

Output modes determine how results are written to the sink.

#### Append Mode (Default)

Only new rows are written to the sink. Use for queries without aggregations.

```python
query = (
    df.writeStream
    .outputMode("append")  # Only new rows
    .format("delta")
    .option("checkpointLocation", "/checkpoint")
    .table("output")
)
```

**Use Cases**:
- ETL pipelines
- Data ingestion
- Filtering and transformations without aggregations

**Restrictions**:
- Cannot use with aggregations (except with watermark)
- Cannot update existing rows

#### Complete Mode

The entire result table is written to the sink after every trigger.

```python
# Aggregation query
agg_df = (
    streaming_df
    .groupBy("category")
    .agg(count("*").alias("count"))
)

query = (
    agg_df.writeStream
    .outputMode("complete")  # Entire result table
    .format("memory")
    .queryName("category_counts")
    .start()
)
```

**Use Cases**:
- Small aggregation results
- Real-time dashboards
- Monitoring queries

**Restrictions**:
- Only works with aggregations
- Entire result must fit in memory
- Not suitable for large result sets

#### Update Mode

Only rows that were updated are written to the sink.

```python
query = (
    agg_df.writeStream
    .outputMode("update")  # Only updated rows
    .format("delta")
    .option("checkpointLocation", "/checkpoint")
    .table("output")
)
```

**Use Cases**:
- Aggregations with large result sets
- Incremental updates
- Change tracking

**Restrictions**:
- Only works with aggregations
- Requires sink that supports updates (Delta, Kafka)

### 4. Triggers

Triggers control when streaming queries process new data.

#### Default (Micro-batch)

Processes data as soon as previous batch completes.

```python
query = (
    df.writeStream
    # No trigger specified = default
    .format("delta")
    .option("checkpointLocation", "/checkpoint")
    .table("output")
)
```

#### Fixed Interval

Processes data at fixed intervals.

```python
query = (
    df.writeStream
    .trigger(processingTime="10 seconds")  # Every 10 seconds
    .format("delta")
    .option("checkpointLocation", "/checkpoint")
    .table("output")
)
```

**Common Intervals**:
- `"5 seconds"` - High frequency
- `"1 minute"` - Moderate frequency
- `"5 minutes"` - Low frequency

#### Available Now (Trigger.AvailableNow)

Processes all available data in micro-batches and stops.

```python
query = (
    df.writeStream
    .trigger(availableNow=True)  # Process all and stop
    .format("delta")
    .option("checkpointLocation", "/checkpoint")
    .table("output")
)

query.awaitTermination()
```

**Use Cases**:
- Scheduled batch jobs using streaming APIs
- Backfilling data
- Testing streaming queries

#### Once (Deprecated)

```python
# Deprecated - use availableNow instead
query = df.writeStream.trigger(once=True)
```

#### Continuous

Low-latency processing (experimental).

```python
query = (
    df.writeStream
    .trigger(continuous="1 second")  # Experimental
    .format("delta")
    .option("checkpointLocation", "/checkpoint")
    .table("output")
)
```

### 5. Checkpointing

Checkpoints enable fault tolerance by tracking streaming progress.

#### Checkpoint Location

```python
query = (
    df.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/my_query")  # Required!
    .table("output")
)
```

**Checkpoint Contents**:
- Offset tracking (which data has been processed)
- State information (for stateful operations)
- Metadata about the query

#### Checkpoint Best Practices

```python
# 1. Use unique checkpoint per query
checkpoint_path = f"/checkpoints/{query_name}_{timestamp}"

# 2. Never reuse checkpoints between different queries
# Bad: Using same checkpoint for different transformations
# Good: Unique checkpoint per query

# 3. Store checkpoints in reliable storage
# Good: Cloud storage (S3, ADLS, GCS)
# Bad: Local disk on cluster

# 4. Clean up old checkpoints when query is retired
dbutils.fs.rm("/checkpoints/old_query", True)
```

#### Checkpoint Recovery

```python
# Query fails and restarts
# Streaming automatically resumes from last checkpoint
query = (
    df.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/my_query")  # Same location
    .table("output")
)
# Resumes from where it left off!
```

### 6. Streaming Sinks

#### Delta Lake Sink (Recommended)

```python
query = (
    df.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoint")
    .outputMode("append")
    .table("target_table")
)
```

**Advantages**:
- ACID transactions
- Schema evolution
- Time travel
- Supports all output modes

#### File Sink

```python
query = (
    df.writeStream
    .format("parquet")  # or json, csv
    .option("checkpointLocation", "/checkpoint")
    .option("path", "/output/path")
    .outputMode("append")
    .start()
)
```

#### Console Sink (Testing)

```python
query = (
    df.writeStream
    .format("console")
    .outputMode("append")
    .start()
)
```

#### Memory Sink (Testing)

```python
query = (
    df.writeStream
    .format("memory")
    .queryName("my_query")
    .outputMode("complete")
    .start()
)

# Query the in-memory table
spark.sql("SELECT * FROM my_query").show()
```

#### Kafka Sink

```python
query = (
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "host:port")
    .option("topic", "output_topic")
    .option("checkpointLocation", "/checkpoint")
    .start()
)
```

### 7. Streaming Transformations

Most DataFrame operations work on streaming DataFrames!

#### Stateless Transformations

```python
# Select, filter, withColumn, etc.
streaming_df = (
    spark.readStream.table("source")
    .filter(col("amount") > 100)
    .withColumn("category", upper(col("category")))
    .select("id", "amount", "category")
)
```

#### Joins

```python
# Stream-static join
streaming_orders = spark.readStream.table("orders")
static_products = spark.read.table("products")

enriched = streaming_orders.join(static_products, "product_id")
```

```python
# Stream-stream join (requires watermark)
stream1 = spark.readStream.table("stream1").withWatermark("timestamp", "10 minutes")
stream2 = spark.readStream.table("stream2").withWatermark("timestamp", "10 minutes")

joined = stream1.join(stream2, "key")
```

#### Aggregations

```python
# Aggregations require output mode complete or update
agg_df = (
    streaming_df
    .groupBy("category")
    .agg(
        count("*").alias("count"),
        sum("amount").alias("total"),
        avg("amount").alias("average")
    )
)

query = (
    agg_df.writeStream
    .outputMode("complete")  # or "update"
    .format("memory")
    .queryName("category_stats")
    .start()
)
```

### 8. Monitoring Streaming Queries

#### Query Status

```python
# Get query status
status = query.status
print(f"Query ID: {status['id']}")
print(f"Running: {status['isDataAvailable']}")
print(f"Message: {status['message']}")
```

#### Recent Progress

```python
# Get recent progress
progress = query.recentProgress
for batch in progress:
    print(f"Batch: {batch['batchId']}")
    print(f"Input rows: {batch['numInputRows']}")
    print(f"Duration: {batch['durationMs']}")
```

#### Last Progress

```python
# Get last batch progress
last = query.lastProgress
if last:
    print(f"Input rows: {last['numInputRows']}")
    print(f"Processing rate: {last['processedRowsPerSecond']}")
```

#### Active Streams

```python
# List all active streaming queries
active_queries = spark.streams.active

for query in active_queries:
    print(f"Query: {query.name}")
    print(f"ID: {query.id}")
    print(f"Status: {query.status}")
```

#### Stop Query

```python
# Stop a specific query
query.stop()

# Stop all queries
for q in spark.streams.active:
    q.stop()
```

### 9. Error Handling

#### Try-Except Pattern

```python
try:
    query = (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", "/checkpoint")
        .table("output")
    )
    
    query.awaitTermination()
    
except Exception as e:
    print(f"Streaming query failed: {str(e)}")
    # Log error, send alert, etc.
```

#### Query Exception

```python
# Check for exceptions
if query.exception():
    print(f"Query failed with: {query.exception()}")
```

### 10. Complete Streaming Pipeline Example

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define schema
schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("amount", DoubleType(), False),
    StructField("timestamp", TimestampType(), False)
])

# Read streaming data
streaming_orders = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/tmp/schema")
    .schema(schema)
    .load("/data/orders/")
)

# Transform
processed_orders = (
    streaming_orders
    .filter(col("amount") > 0)
    .withColumn("order_date", to_date(col("timestamp")))
    .withColumn("processed_at", current_timestamp())
)

# Write to Bronze
bronze_query = (
    processed_orders.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/bronze_orders")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .table("bronze.orders")
)

# Aggregate for Gold
daily_stats = (
    processed_orders
    .groupBy(
        window(col("timestamp"), "1 day"),
        col("order_date")
    )
    .agg(
        count("*").alias("order_count"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value")
    )
)

# Write to Gold
gold_query = (
    daily_stats.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/gold_daily")
    .outputMode("complete")
    .trigger(processingTime="1 minute")
    .table("gold.daily_stats")
)

# Monitor
print("Streaming queries started!")
print(f"Bronze query: {bronze_query.id}")
print(f"Gold query: {gold_query.id}")

# Keep running
bronze_query.awaitTermination()
```

## Best Practices

### 1. Always Use Checkpoints

```python
# Good
.option("checkpointLocation", "/checkpoints/unique_name")

# Bad - will fail
# No checkpoint specified
```

### 2. Use Appropriate Triggers

```python
# High-frequency data
.trigger(processingTime="5 seconds")

# Moderate frequency
.trigger(processingTime="1 minute")

# Batch-like processing
.trigger(availableNow=True)
```

### 3. Choose Right Output Mode

```python
# No aggregations → append
.outputMode("append")

# Aggregations, small results → complete
.outputMode("complete")

# Aggregations, large results → update
.outputMode("update")
```

### 4. Monitor Query Health

```python
# Regular monitoring
def monitor_query(query):
    status = query.status
    if not status['isDataAvailable']:
        alert("No data available")
    
    progress = query.lastProgress
    if progress and progress['processedRowsPerSecond'] < threshold:
        alert("Processing too slow")
```

### 5. Handle Schema Evolution

```python
# Enable schema evolution
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Or use mergeSchema option
.option("mergeSchema", "true")
```

## Common Patterns

### Pattern 1: Continuous Ingestion

```python
# Ingest files as they arrive
query = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load("/landing/")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoint")
    .table("bronze.raw_data")
)
```

### Pattern 2: Real-time Aggregation

```python
# Real-time metrics
metrics = (
    spark.readStream.table("events")
    .groupBy(window(col("timestamp"), "5 minutes"))
    .agg(count("*").alias("event_count"))
    .writeStream
    .outputMode("complete")
    .format("memory")
    .queryName("realtime_metrics")
    .start()
)
```

### Pattern 3: Stream Enrichment

```python
# Enrich streaming data with static data
streaming_events = spark.readStream.table("events")
static_users = spark.read.table("users")

enriched = (
    streaming_events
    .join(static_users, "user_id")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoint")
    .table("enriched_events")
)
```

## Key Takeaways

1. **Structured Streaming** treats streaming data as an unbounded table
2. **readStream/writeStream** API is similar to batch read/write
3. **Output modes**: append (new rows), complete (all rows), update (changed rows)
4. **Triggers** control processing frequency
5. **Checkpoints** are required for fault tolerance
6. **Most transformations** work the same as batch
7. **Delta Lake** is the recommended sink for streaming
8. **Monitoring** is essential for production streaming queries

## Exam Tips

1. Know the three output modes and when to use each
2. Understand checkpoint purpose and requirements
3. Know trigger types (processingTime, availableNow)
4. Understand difference between readStream and read
5. Know which operations require watermarks
6. Understand stream-static vs stream-stream joins
7. Know how to monitor streaming queries
8. Understand fault tolerance mechanisms

## Additional Resources

- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Databricks Structured Streaming](https://docs.databricks.com/structured-streaming/index.html)
- [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Delta Lake Streaming](https://docs.delta.io/latest/delta-streaming.html)

---

**Next**: Day 17 - Streaming Transformations (Watermarking and Windowing)

