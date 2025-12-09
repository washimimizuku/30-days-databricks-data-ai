# Day 17: Streaming Transformations - Exercises

"""
Complete these exercises to practice watermarking, windowing, and advanced streaming transformations.

Setup: Run setup.md first to create time-series event data
"""

from pyspark.sql.functions import *
from pyspark.sql.types import *

# ============================================================================
# PART 1: WATERMARKING BASICS (Exercises 1-15)
# ============================================================================

# Exercise 1: Read streaming events
# Read from /tmp/streaming/events/ and convert event_time to timestamp
# TODO: Your code here


# Exercise 2: Add watermark with 10-minute delay
# Use withWatermark on event_time column
# TODO: Your code here


# Exercise 3: Add watermark with 5-minute delay
# Create another stream with stricter watermark
# TODO: Your code here


# Exercise 4: Add watermark with 30-minute delay
# Create stream with lenient watermark
# TODO: Your code here


# Exercise 5: Aggregate with watermark
# Count events by event_type with watermark
# TODO: Your code here


# Exercise 6: Write with append mode
# Use append mode (possible with watermark)
# TODO: Your code here


# Exercise 7: Monitor watermark progress
# Check lastProgress for watermark value
# TODO: Your code here


# Exercise 8: Compare with and without watermark
# Create two queries and observe state differences
# TODO: Your code here


# Exercise 9: Calculate event lateness
# Add column showing seconds between event_time and processing_time
# TODO: Your code here


# Exercise 10: Filter late events
# Identify events more than 10 minutes late
# TODO: Your code here


# Exercise 11: Track watermark advancement
# Monitor how watermark advances over time
# TODO: Your code here


# Exercise 12: Understand watermark calculation
# Watermark = max_event_time - watermark_delay
# TODO: Your code here


# Exercise 13: Test watermark with late data
# Generate late events and see if they're processed
# TODO: Your code here


# Exercise 14: Optimize watermark delay
# Find balance between state size and data loss
# TODO: Your code here


# Exercise 15: Watermark best practices
# Document when to use short vs long watermarks
# TODO: Your code here


# ============================================================================
# PART 2: TUMBLING WINDOWS (Exercises 16-25)
# ============================================================================

# Exercise 16: Create 10-minute tumbling windows
# Group events by 10-minute windows
# TODO: Your code here


# Exercise 17: Create 1-hour tumbling windows
# Group events by 1-hour windows
# TODO: Your code here


# Exercise 18: Create 5-minute tumbling windows
# Group events by 5-minute windows
# TODO: Your code here


# Exercise 19: Aggregate within windows
# Count events per window
# TODO: Your code here


# Exercise 20: Multiple aggregations per window
# Count, sum, avg within each window
# TODO: Your code here


# Exercise 21: Group by window and event_type
# Aggregate by both window and event_type
# TODO: Your code here


# Exercise 22: Extract window start and end
# Select window.start and window.end columns
# TODO: Your code here


# Exercise 23: Filter windows
# Only keep windows with count > 10
# TODO: Your code here


# Exercise 24: Write windowed results
# Write to Delta table with append mode
# TODO: Your code here


# Exercise 25: Monitor window completion
# Track which windows are finalized
# TODO: Your code here


# ============================================================================
# PART 3: SLIDING WINDOWS (Exercises 26-35)
# ============================================================================

# Exercise 26: Create sliding windows
# 10-minute windows sliding every 5 minutes
# TODO: Your code here


# Exercise 27: Create overlapping windows
# 1-hour windows sliding every 30 minutes
# TODO: Your code here


# Exercise 28: Compare tumbling vs sliding
# Create both and observe differences
# TODO: Your code here


# Exercise 29: Aggregate in sliding windows
# Count events in overlapping windows
# TODO: Your code here


# Exercise 30: Calculate moving averages
# Use sliding windows for moving average
# TODO: Your code here


# Exercise 31: Detect trends
# Use sliding windows to detect increasing/decreasing trends
# TODO: Your code here


# Exercise 32: Window with custom start time
# Start windows at specific time offset
# TODO: Your code here


# Exercise 33: Multiple window sizes
# Create 5-min, 10-min, and 1-hour windows
# TODO: Your code here


# Exercise 34: Write sliding window results
# Write to separate tables per window size
# TODO: Your code here


# Exercise 35: Optimize sliding window performance
# Monitor state size with different slide intervals
# TODO: Your code here


# ============================================================================
# PART 4: DEDUPLICATION (Exercises 36-45)
# ============================================================================

# Exercise 36: Deduplicate by event_id
# Remove duplicate events
# TODO: Your code here


# Exercise 37: Deduplicate with watermark
# Use watermark to bound deduplication state
# TODO: Your code here


# Exercise 38: Deduplicate by multiple columns
# Deduplicate by event_id and user_id
# TODO: Your code here


# Exercise 39: Compare with and without watermark
# Observe state size differences
# TODO: Your code here


# Exercise 40: Deduplicate within time window
# Only deduplicate within 10-minute window
# TODO: Your code here


# Exercise 41: Track duplicate rate
# Calculate percentage of duplicates
# TODO: Your code here


# Exercise 42: Write deduplicated stream
# Write to Delta table
# TODO: Your code here


# Exercise 43: Monitor deduplication state
# Check state size over time
# TODO: Your code here


# Exercise 44: Handle duplicate events
# Write duplicates to separate table
# TODO: Your code here


# Exercise 45: Optimize deduplication
# Find optimal watermark for deduplication
# TODO: Your code here


# ============================================================================
# PART 5: STREAM-STREAM JOINS (Exercises 46-55)
# ============================================================================

# Exercise 46: Read impressions stream
# Read from /tmp/streaming/impressions/
# TODO: Your code here


# Exercise 47: Read clicks stream
# Read from /tmp/streaming/clicks/
# TODO: Your code here


# Exercise 48: Add watermarks to both streams
# Add 10-minute watermark to each
# TODO: Your code here


# Exercise 49: Inner join with time constraint
# Join impressions and clicks within 10 minutes
# TODO: Your code here


# Exercise 50: Left outer join
# Keep all impressions, match clicks if available
# TODO: Your code here


# Exercise 51: Calculate click-through rate
# Count impressions and clicks, calculate CTR
# TODO: Your code here


# Exercise 52: Join with multiple conditions
# Join on impression_id and time window
# TODO: Your code here


# Exercise 53: Monitor join state
# Check state size for stream-stream join
# TODO: Your code here


# Exercise 54: Write joined results
# Write to Delta table
# TODO: Your code here


# Exercise 55: Optimize join performance
# Adjust watermarks for better performance
# TODO: Your code here


# ============================================================================
# PART 6: ADVANCED PATTERNS (Exercises 56-65)
# ============================================================================

# Exercise 56: Session windows (manual implementation)
# Group events by user with 30-minute inactivity gap
# TODO: Your code here


# Exercise 57: Real-time dashboard query
# Create query for live metrics
# TODO: Your code here


# Exercise 58: Late data monitoring
# Track and report late-arriving events
# TODO: Your code here


# Exercise 59: Multi-level aggregation
# Aggregate at multiple time granularities
# TODO: Your code here


# Exercise 60: Event time vs processing time comparison
# Compare metrics using both time types
# TODO: Your code here


# Exercise 61: Watermark tuning
# Experiment with different watermark delays
# TODO: Your code here


# Exercise 62: State size monitoring
# Create dashboard for state metrics
# TODO: Your code here


# Exercise 63: Complex windowed aggregation
# Multiple windows with multiple aggregations
# TODO: Your code here


# Exercise 64: Stream enrichment with watermark
# Join streaming events with static data
# TODO: Your code here


# Exercise 65: Complete streaming pipeline
# Bronze → Silver → Gold with watermarking
# TODO: Your code here


# ============================================================================
# BONUS CHALLENGES (Exercises 66-70)
# ============================================================================

# Bonus 1: Implement custom stateful operation
# Use mapGroupsWithState for custom logic
# TODO: Your code here


# Bonus 2: Build real-time anomaly detection
# Detect unusual patterns in windowed data
# TODO: Your code here


# Bonus 3: Create adaptive watermark
# Adjust watermark based on data characteristics
# TODO: Your code here


# Bonus 4: Implement exactly-once semantics
# Ensure no duplicates in output
# TODO: Your code here


# Bonus 5: Build streaming data quality monitor
# Track quality metrics in real-time
# TODO: Your code here


# ============================================================================
# VERIFICATION
# ============================================================================

print("="*80)
print("STREAMING TRANSFORMATIONS VERIFICATION")
print("="*80)

# Check active queries
print("\n=== Active Queries ===")
for q in spark.streams.active:
    print(f"{q.name or q.id}: {q.status['message']}")
    progress = q.lastProgress
    if progress and "watermark" in progress:
        print(f"  Watermark: {progress['watermark']}")

# Check output tables
print("\n=== Output Tables ===")
spark.sql("SHOW TABLES IN streaming_gold").show()

print("\n" + "="*80)
print("✅ Exercises complete! Check solution.py for answers.")
print("="*80)
