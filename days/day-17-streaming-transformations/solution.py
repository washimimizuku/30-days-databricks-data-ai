# Day 17: Streaming Transformations - Solutions

"""
Complete solutions for Day 17 exercises demonstrating watermarking, windowing,
and advanced streaming transformations.
"""

from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# ============================================================================
# PART 1: WATERMARKING BASICS
# ============================================================================

# Exercise 1: Read streaming events
events = (
    spark.readStream
    .format("json")
    .load("/tmp/streaming/events/")
    .withColumn("event_time", to_timestamp(col("event_time")))
    .withColumn("processing_time", to_timestamp(col("processing_time")))
)
print("✓ Streaming events loaded")

# Exercise 2: Add 10-minute watermark
events_10min = events.withWatermark("event_time", "10 minutes")
print("✓ 10-minute watermark added")

# Exercise 3: Add 5-minute watermark (strict)
events_5min = events.withWatermark("event_time", "5 minutes")
print("✓ 5-minute watermark added")

# Exercise 4: Add 30-minute watermark (lenient)
events_30min = events.withWatermark("event_time", "30 minutes")
print("✓ 30-minute watermark added")

# Exercise 5: Aggregate with watermark
watermarked_agg = (
    events_10min
    .groupBy("event_type")
    .count()
)
print("✓ Aggregation with watermark")

# Exercise 6: Write with append mode
append_query = (
    watermarked_agg
    .writeStream
    .outputMode("append")  # Possible with watermark!
    .format("memory")
    .queryName("watermarked_counts")
    .start()
)
print("✓ Append mode with watermark")

# Exercise 9: Calculate lateness
with_lateness = events.withColumn(
    "lateness_seconds",
    unix_timestamp(col("processing_time")) - unix_timestamp(col("event_time"))
)
print("✓ Lateness calculated")

# Exercise 10: Filter late events
late_events = with_lateness.filter(col("lateness_seconds") > 600)  # > 10 minutes
print("✓ Late events filtered")

# ============================================================================
# PART 2: TUMBLING WINDOWS
# ============================================================================

# Exercise 16: 10-minute tumbling windows
tumbling_10min = (
    events_10min
    .groupBy(
        window(col("event_time"), "10 minutes"),
        col("event_type")
    )
    .count()
)
print("✓ 10-minute tumbling windows")

# Exercise 17: 1-hour tumbling windows
tumbling_1hour = (
    events_10min
    .groupBy(
        window(col("event_time"), "1 hour"),
        col("event_type")
    )
    .count()
)
print("✓ 1-hour tumbling windows")

# Exercise 20: Multiple aggregations
multi_agg = (
    events_10min
    .groupBy(window(col("event_time"), "10 minutes"))
    .agg(
        count("*").alias("event_count"),
        sum("value").alias("total_value"),
        avg("value").alias("avg_value"),
        countDistinct("user_id").alias("unique_users")
    )
)
print("✓ Multiple aggregations per window")

# Exercise 22: Extract window start/end
with_window_times = (
    tumbling_10min
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("event_type"),
        col("count")
    )
)
print("✓ Window times extracted")

# Exercise 24: Write windowed results
windowed_query = (
    multi_agg
    .writeStream
    .outputMode("append")
    .format("delta")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/windowed_agg")
    .table("streaming_gold.windowed_metrics")
)
print("✓ Windowed results written")

# ============================================================================
# PART 3: SLIDING WINDOWS
# ============================================================================

# Exercise 26: Sliding windows (10-min window, 5-min slide)
sliding_windows = (
    events_10min
    .groupBy(
        window(col("event_time"), "10 minutes", "5 minutes"),
        col("event_type")
    )
    .count()
)
print("✓ Sliding windows created")

# Exercise 27: Overlapping windows (1-hour window, 30-min slide)
sliding_1hour = (
    events_10min
    .groupBy(
        window(col("event_time"), "1 hour", "30 minutes")
    )
    .agg(
        count("*").alias("count"),
        avg("value").alias("avg_value")
    )
)
print("✓ 1-hour sliding windows")

# Exercise 30: Moving average
moving_avg = (
    events_10min
    .groupBy(
        window(col("event_time"), "10 minutes", "1 minute")
    )
    .agg(avg("value").alias("moving_avg"))
)
print("✓ Moving average calculated")

# ============================================================================
# PART 4: DEDUPLICATION
# ============================================================================

# Exercise 36: Deduplicate by event_id
deduplicated = events.dropDuplicates(["event_id"])
print("✓ Deduplicated by event_id")

# Exercise 37: Deduplicate with watermark
deduplicated_watermarked = (
    events_10min
    .dropDuplicates(["event_id", "event_time"])
)
print("✓ Deduplicated with watermark")

# Exercise 42: Write deduplicated stream
dedup_query = (
    deduplicated_watermarked
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/dedup")
    .table("streaming_silver.deduplicated_events")
)
print("✓ Deduplicated stream written")

# ============================================================================
# PART 5: STREAM-STREAM JOINS
# ============================================================================

# Exercise 46-47: Read impressions and clicks
impressions = (
    spark.readStream
    .format("json")
    .load("/tmp/streaming/impressions/")
    .withColumn("impression_time", to_timestamp(col("impression_time")))
)

clicks = (
    spark.readStream
    .format("json")
    .load("/tmp/streaming/clicks/")
    .withColumn("click_time", to_timestamp(col("click_time")))
)
print("✓ Impressions and clicks loaded")

# Exercise 48: Add watermarks
impressions_wm = impressions.withWatermark("impression_time", "10 minutes")
clicks_wm = clicks.withWatermark("click_time", "10 minutes")
print("✓ Watermarks added to both streams")

# Exercise 49: Inner join with time constraint
joined = impressions_wm.join(
    clicks_wm,
    expr("""
        impression_id = click_id AND
        click_time >= impression_time AND
        click_time <= impression_time + interval 10 minutes
    """)
)
print("✓ Stream-stream join created")

# Exercise 50: Left outer join
left_joined = impressions_wm.join(
    clicks_wm,
    expr("""
        impression_id = click_id AND
        click_time >= impression_time AND
        click_time <= impression_time + interval 10 minutes
    """),
    "left"
)
print("✓ Left outer join created")

# Exercise 51: Calculate CTR
ctr_metrics = (
    left_joined
    .groupBy(window(col("impression_time"), "10 minutes"))
    .agg(
        count("impression_id").alias("impressions"),
        count("click_id").alias("clicks")
    )
    .withColumn("ctr", col("clicks") / col("impressions") * 100)
)
print("✓ CTR calculated")

# Exercise 54: Write joined results
join_query = (
    joined
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/stream_join")
    .table("streaming_gold.impression_clicks")
)
print("✓ Joined results written")

# ============================================================================
# PART 6: ADVANCED PATTERNS
# ============================================================================

# Exercise 56: Session windows (manual implementation)
# Group by user with 30-minute inactivity gap
sessions = (
    events_30min
    .groupBy(
        "user_id",
        window(col("event_time"), "30 minutes")
    )
    .agg(
        count("*").alias("event_count"),
        min("event_time").alias("session_start"),
        max("event_time").alias("session_end")
    )
    .withColumn(
        "session_duration_minutes",
        (unix_timestamp(col("session_end")) - unix_timestamp(col("session_start"))) / 60
    )
)
print("✓ Session windows implemented")

# Exercise 57: Real-time dashboard query
dashboard_metrics = (
    events_10min
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("event_type")
    )
    .agg(
        count("*").alias("count"),
        sum("value").alias("total"),
        avg("value").alias("average")
    )
    .writeStream
    .outputMode("complete")
    .format("memory")
    .queryName("realtime_dashboard")
    .start()
)
print("✓ Real-time dashboard query")

# Exercise 58: Late data monitoring
late_data_monitor = (
    with_lateness
    .filter(col("lateness_seconds") > 600)
    .groupBy(window(col("processing_time"), "10 minutes"))
    .agg(
        count("*").alias("late_event_count"),
        avg("lateness_seconds").alias("avg_lateness")
    )
)
print("✓ Late data monitoring")

# Exercise 59: Multi-level aggregation
# 5-minute, 10-minute, and 1-hour windows
agg_5min = (
    events_10min
    .groupBy(window(col("event_time"), "5 minutes"))
    .count()
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/agg_5min")
    .table("streaming_gold.metrics_5min")
)

agg_10min = (
    events_10min
    .groupBy(window(col("event_time"), "10 minutes"))
    .count()
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/agg_10min")
    .table("streaming_gold.metrics_10min")
)

agg_1hour = (
    events_10min
    .groupBy(window(col("event_time"), "1 hour"))
    .count()
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/agg_1hour")
    .table("streaming_gold.metrics_1hour")
)
print("✓ Multi-level aggregation")

# Exercise 65: Complete streaming pipeline
class WatermarkedPipeline:
    """Complete pipeline with watermarking"""
    
    def __init__(self, spark):
        self.spark = spark
        self.queries = []
    
    def run_bronze(self):
        """Ingest to Bronze"""
        query = (
            spark.readStream
            .format("json")
            .load("/tmp/streaming/events/")
            .withColumn("event_time", to_timestamp(col("event_time")))
            .withColumn("_ingestion_time", current_timestamp())
            .writeStream
            .format("delta")
            .option("checkpointLocation", "/tmp/streaming/checkpoints/pipeline_bronze")
            .table("streaming_bronze.events")
        )
        self.queries.append(query)
        return query
    
    def run_silver(self):
        """Transform to Silver with deduplication"""
        query = (
            spark.readStream
            .table("streaming_bronze.events")
            .withWatermark("event_time", "10 minutes")
            .dropDuplicates(["event_id", "event_time"])
            .filter(col("value") > 0)
            .writeStream
            .format("delta")
            .option("checkpointLocation", "/tmp/streaming/checkpoints/pipeline_silver")
            .table("streaming_silver.events")
        )
        self.queries.append(query)
        return query
    
    def run_gold(self):
        """Aggregate to Gold with windowing"""
        query = (
            spark.readStream
            .table("streaming_silver.events")
            .withWatermark("event_time", "10 minutes")
            .groupBy(
                window(col("event_time"), "10 minutes"),
                col("event_type")
            )
            .agg(
                count("*").alias("count"),
                sum("value").alias("total"),
                avg("value").alias("average")
            )
            .writeStream
            .outputMode("append")
            .format("delta")
            .option("checkpointLocation", "/tmp/streaming/checkpoints/pipeline_gold")
            .table("streaming_gold.event_metrics")
        )
        self.queries.append(query)
        return query
    
    def stop_all(self):
        """Stop all queries"""
        for q in self.queries:
            q.stop()

# Create and run pipeline
pipeline = WatermarkedPipeline(spark)
pipeline.run_bronze()
pipeline.run_silver()
pipeline.run_gold()

print("✓ Complete watermarked pipeline created")

# ============================================================================
# MONITORING AND VERIFICATION
# ============================================================================

def monitor_watermark_progress(query, duration=30):
    """Monitor watermark advancement"""
    import time
    start = time.time()
    
    print(f"\nMonitoring {query.name or query.id} for {duration} seconds...")
    
    while time.time() - start < duration:
        if query.isActive:
            progress = query.lastProgress
            if progress:
                print(f"\nBatch {progress['batchId']}:")
                print(f"  Input rows: {progress['numInputRows']}")
                
                if "watermark" in progress:
                    print(f"  Watermark: {progress['watermark']}")
                
                if "stateOperators" in progress:
                    for op in progress["stateOperators"]:
                        if "numRowsTotal" in op:
                            print(f"  State size: {op['numRowsTotal']} rows")
        
        time.sleep(5)

# Monitor a query
if len(spark.streams.active) > 0:
    monitor_watermark_progress(spark.streams.active[0], 30)

# ============================================================================
# FINAL VERIFICATION
# ============================================================================

print("\n" + "="*80)
print("STREAMING TRANSFORMATIONS VERIFICATION")
print("="*80)

# Active queries
print("\n=== Active Queries ===")
for q in spark.streams.active:
    print(f"{q.name or q.id}:")
    print(f"  Status: {q.status['message']}")
    progress = q.lastProgress
    if progress and "watermark" in progress:
        print(f"  Watermark: {progress['watermark']}")

# Output tables
print("\n=== Output Tables ===")
spark.sql("SHOW TABLES IN streaming_gold").show()

# Sample windowed data
if spark.catalog.tableExists("streaming_gold.windowed_metrics"):
    print("\n=== Sample Windowed Metrics ===")
    spark.table("streaming_gold.windowed_metrics").orderBy("window_start").show(5)

print("\n" + "="*80)
print("✅ All solutions complete!")
print("="*80)
