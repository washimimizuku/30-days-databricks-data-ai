# Day 17: Streaming Transformations

## Learning Objectives

By the end of today, you will:
- Understand watermarking and its purpose
- Implement time-based windowing operations
- Handle late-arriving data
- Perform stateful streaming operations
- Use event time vs processing time
- Implement stream-stream joins
- Apply deduplication in streaming
- Optimize streaming state management

## Topics Covered

### 1. Event Time vs Processing Time

**Event Time**: When the event actually occurred (in the data)  
**Processing Time**: When Spark processes the event

```python
# Event time - from the data
df.withColumn("event_time", col("timestamp"))

# Processing time - when processed
df.withColumn("processing_time", current_timestamp())
```

**Why Event Time Matters**:
- Events may arrive out of order
- Network delays cause late arrivals
- Event time reflects reality
- Processing time reflects system state

### 2. Watermarking

Watermarking defines how long to wait for late data before considering a time window complete.

#### What is a Watermark?

A watermark is a threshold that tells Spark:
- "I've seen all data up to time T - watermark"
- "Any data older than this is too late"
- "I can finalize windows and clean up state"

#### Watermark Syntax

```python
# Add watermark: wait 10 minutes for late data
df_with_watermark = df.withWatermark("timestamp", "10 minutes")
```

**Watermark Calculation**:
```
Watermark = Max Event Time Seen - Watermark Delay
```

If max event time is 12:00 PM and watermark is 10 minutes:
- Watermark = 12:00 PM - 10 min = 11:50 AM
- Events before 11:50 AM are considered late

#### Why Watermarks Are Needed

Without watermarks:
- State grows indefinitely
- Can't finalize windows
- Memory issues
- Can't use append mode with aggregations

With watermarks:
- Bounded state
- Windows can be finalized
- Late data handling
- Append mode possible

#### Watermark Example

```python
from pyspark.sql.functions import *

# Read streaming data
events = (
    spark.readStream
    .format("json")
    .schema(schema)
    .load("/data/events/")
)

# Add watermark
events_with_watermark = events.withWatermark("event_time", "10 minutes")

# Aggregate with watermark
windowed_counts = (
    events_with_watermark
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("user_id")
    )
    .count()
)

# Can use append mode now!
query = (
    windowed_counts.writeStream
    .outputMode("append")  # Possible with watermark
    .format("delta")
    .option("checkpointLocation", "/checkpoint")
    .table("windowed_counts")
)
```

### 3. Time Windows

Windows group events by time intervals.

#### Tumbling Windows

Non-overlapping, fixed-size windows.

```python
# 10-minute tumbling windows
windowed = df.groupBy(
    window(col("timestamp"), "10 minutes")
).count()

# Windows: [00:00-00:10), [00:10-00:20), [00:20-00:30), ...
```

**Characteristics**:
- Each event belongs to exactly one window
- No overlap between windows
- Fixed size

#### Sliding Windows

Overlapping windows that slide by a specified interval.

```python
# 10-minute windows, sliding every 5 minutes
windowed = df.groupBy(
    window(col("timestamp"), "10 minutes", "5 minutes")
).count()

# Windows: [00:00-00:10), [00:05-00:15), [00:10-00:20), ...
```

**Characteristics**:
- Events can belong to multiple windows
- Windows overlap
- Slide interval < window size = overlap

#### Session Windows

Windows based on periods of activity (not yet supported in Structured Streaming).

#### Window Syntax

```python
# Basic tumbling window
window(timeColumn, windowDuration)

# Sliding window
window(timeColumn, windowDuration, slideDuration)

# Window with start time offset
window(timeColumn, windowDuration, slideDuration, startTime)
```

#### Window Examples

```python
# 1-hour tumbling windows
window(col("timestamp"), "1 hour")

# 1-hour windows, sliding every 30 minutes
window(col("timestamp"), "1 hour", "30 minutes")

# 1-day windows starting at 8 AM
window(col("timestamp"), "1 day", "1 day", "8 hours")
```

### 4. Watermarking with Windows

Combining watermarks and windows for production streaming.

```python
from pyspark.sql.functions import *

# Complete example
streaming_events = (
    spark.readStream
    .format("json")
    .schema(schema)
    .load("/events/")
)

# Add watermark (wait 15 minutes for late data)
with_watermark = streaming_events.withWatermark("event_time", "15 minutes")

# Aggregate with 10-minute tumbling windows
windowed_agg = (
    with_watermark
    .groupBy(
        window(col("event_time"), "10 minutes"),
        col("event_type")
    )
    .agg(
        count("*").alias("event_count"),
        sum("value").alias("total_value")
    )
)

# Write with append mode (possible due to watermark)
query = (
    windowed_agg.writeStream
    .outputMode("append")
    .format("delta")
    .option("checkpointLocation", "/checkpoint")
    .table("event_metrics")
)
```

#### How Watermark Affects Windows

```
Current time: 12:15 PM
Watermark: 10 minutes
Max event time seen: 12:10 PM
Watermark threshold: 12:00 PM

Windows:
- [11:50-12:00): Finalized (before watermark)
- [12:00-12:10): Still open (within watermark)
- [12:10-12:20): Still open (current window)
```

### 5. Handling Late Data

#### Late Data Scenarios

```python
# Event arrives late
Event time: 11:55 AM
Arrival time: 12:15 PM
Watermark: 12:00 PM (based on 12:10 PM max - 10 min)

# Is it too late?
11:55 AM < 12:00 PM → YES, too late, dropped
```

#### Configuring Late Data Tolerance

```python
# Strict: 5-minute watermark (less late data accepted)
.withWatermark("timestamp", "5 minutes")

# Lenient: 1-hour watermark (more late data accepted)
.withWatermark("timestamp", "1 hour")

# Trade-off:
# - Shorter watermark = less state, faster finalization, more dropped data
# - Longer watermark = more state, slower finalization, less dropped data
```

#### Monitoring Late Data

```python
# Track late events
late_events = (
    streaming_df
    .withColumn("processing_time", current_timestamp())
    .withColumn("lateness", 
        unix_timestamp(col("processing_time")) - unix_timestamp(col("event_time"))
    )
    .filter(col("lateness") > 600)  # More than 10 minutes late
)
```

### 6. Stream-Stream Joins

Joining two streaming DataFrames.

#### Inner Join with Watermarks

```python
# Stream 1: Impressions
impressions = (
    spark.readStream.table("impressions")
    .withWatermark("impression_time", "10 minutes")
)

# Stream 2: Clicks
clicks = (
    spark.readStream.table("clicks")
    .withWatermark("click_time", "10 minutes")
)

# Join within time window
joined = impressions.join(
    clicks,
    expr("""
        impression_id = click_id AND
        click_time >= impression_time AND
        click_time <= impression_time + interval 1 hour
    """)
)
```

**Requirements**:
- Both streams must have watermarks
- Join condition must include time constraints
- Prevents unbounded state growth

#### Outer Joins

```python
# Left outer join
left_joined = impressions.join(
    clicks,
    expr("""
        impression_id = click_id AND
        click_time >= impression_time AND
        click_time <= impression_time + interval 1 hour
    """),
    "left"
)
```

### 7. Deduplication

Removing duplicate events in streaming.

#### Deduplication with Watermark

```python
# Deduplicate within watermark window
deduplicated = (
    streaming_df
    .withWatermark("timestamp", "10 minutes")
    .dropDuplicates(["event_id", "timestamp"])
)
```

**How it works**:
- Keeps state of seen event_ids
- State is bounded by watermark
- Old state is cleaned up

#### Deduplication Without Watermark

```python
# Unbounded state (not recommended for production)
deduplicated = streaming_df.dropDuplicates(["event_id"])
```

**Warning**: State grows indefinitely!

### 8. Stateful Operations

Operations that maintain state across batches.

#### MapGroupsWithState

Custom stateful processing.

```python
from pyspark.sql.streaming import GroupState

def update_user_session(key, values, state):
    """Track user session state"""
    # Get existing state or create new
    if state.exists:
        session = state.get
    else:
        session = {"start_time": None, "event_count": 0}
    
    # Update state with new events
    for event in values:
        if session["start_time"] is None:
            session["start_time"] = event.timestamp
        session["event_count"] += 1
    
    # Update state
    state.update(session)
    
    # Return result
    return (key, session["event_count"])

# Apply stateful operation
result = (
    streaming_df
    .groupByKey(lambda x: x.user_id)
    .mapGroupsWithState(update_user_session)
)
```

#### FlatMapGroupsWithState

More flexible stateful processing with timeout support.

```python
from pyspark.sql.streaming import GroupStateTimeout

def process_with_timeout(key, values, state):
    """Process with timeout"""
    if state.hasTimedOut:
        # Handle timeout
        result = state.get
        state.remove()
        return [result]
    else:
        # Process normally
        # ...
        state.update(new_state)
        state.setTimeoutDuration(60000)  # 60 seconds
        return []

result = (
    streaming_df
    .groupByKey(lambda x: x.user_id)
    .flatMapGroupsWithState(
        process_with_timeout,
        GroupStateTimeout.ProcessingTimeTimeout
    )
)
```

### 9. Complete Streaming Pipeline with Watermarking

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Schema
schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("value", DoubleType()),
    StructField("event_time", TimestampType())
])

# Read stream
events = (
    spark.readStream
    .format("json")
    .schema(schema)
    .load("/data/events/")
)

# Add watermark (15-minute tolerance for late data)
events_with_watermark = events.withWatermark("event_time", "15 minutes")

# Deduplicate
deduplicated = events_with_watermark.dropDuplicates(["event_id", "event_time"])

# Aggregate with 10-minute tumbling windows
windowed_metrics = (
    deduplicated
    .groupBy(
        window(col("event_time"), "10 minutes"),
        col("event_type")
    )
    .agg(
        count("*").alias("event_count"),
        sum("value").alias("total_value"),
        avg("value").alias("avg_value"),
        countDistinct("user_id").alias("unique_users")
    )
)

# Write to Delta with append mode
query = (
    windowed_metrics.writeStream
    .outputMode("append")  # Possible with watermark
    .format("delta")
    .option("checkpointLocation", "/checkpoints/windowed_metrics")
    .trigger(processingTime="1 minute")
    .table("gold.windowed_event_metrics")
)

query.awaitTermination()
```

## Best Practices

### 1. Always Use Watermarks for Aggregations

```python
# Good
df.withWatermark("timestamp", "10 minutes").groupBy(...).count()

# Bad (unbounded state)
df.groupBy(...).count()
```

### 2. Choose Appropriate Watermark Delay

```python
# Consider your data characteristics:
# - How late can data arrive?
# - What's acceptable data loss?
# - How much state can you maintain?

# Real-time (strict)
.withWatermark("timestamp", "5 minutes")

# Batch-like (lenient)
.withWatermark("timestamp", "1 hour")
```

### 3. Use Event Time, Not Processing Time

```python
# Good - based on when event occurred
.groupBy(window(col("event_time"), "10 minutes"))

# Bad - based on when processed
.groupBy(window(current_timestamp(), "10 minutes"))
```

### 4. Monitor State Size

```python
# Check state metrics
progress = query.lastProgress
if progress:
    state_size = progress.get("stateOperators", [{}])[0].get("numRowsTotal", 0)
    print(f"State size: {state_size} rows")
```

### 5. Clean Up State Regularly

```python
# Watermarks automatically clean up old state
# Ensure watermark is set appropriately
```

## Common Patterns

### Pattern 1: Real-time Metrics

```python
# 5-minute metrics with 2-minute watermark
metrics = (
    events
    .withWatermark("timestamp", "2 minutes")
    .groupBy(window(col("timestamp"), "5 minutes"))
    .agg(count("*").alias("count"))
)
```

### Pattern 2: Session Analysis

```python
# Track user sessions with 30-minute inactivity timeout
sessions = (
    events
    .withWatermark("timestamp", "30 minutes")
    .groupBy("user_id", window(col("timestamp"), "30 minutes"))
    .agg(
        count("*").alias("event_count"),
        min("timestamp").alias("session_start"),
        max("timestamp").alias("session_end")
    )
)
```

### Pattern 3: Click-through Rate

```python
# Join impressions and clicks within 1 hour
ctr = (
    impressions
    .withWatermark("impression_time", "1 hour")
    .join(
        clicks.withWatermark("click_time", "1 hour"),
        expr("""
            impression_id = click_id AND
            click_time >= impression_time AND
            click_time <= impression_time + interval 1 hour
        """),
        "left"
    )
)
```

## Key Takeaways

1. **Watermarks** define how long to wait for late data
2. **Event time** reflects when events occurred (use this!)
3. **Windows** group events by time intervals
4. **Tumbling windows** don't overlap; **sliding windows** do
5. **Watermarks enable append mode** with aggregations
6. **Stream-stream joins** require watermarks on both sides
7. **Deduplication** with watermarks bounds state
8. **State management** is critical for streaming performance

## Exam Tips

1. Know what watermarks are and why they're needed
2. Understand event time vs processing time
3. Know tumbling vs sliding windows
4. Understand when append mode is possible (with watermarks)
5. Know stream-stream join requirements
6. Understand state management implications
7. Know how to handle late data
8. Understand watermark calculation

## Additional Resources

- [Structured Streaming + Watermarking](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking)
- [Time Windows](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time)
- [Stream-Stream Joins](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#stream-stream-joins)
- [Arbitrary Stateful Operations](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#arbitrary-stateful-operations)

---

**Next**: Day 18 - Delta Lake MERGE (UPSERT)

