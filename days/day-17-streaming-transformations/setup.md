# Day 17: Streaming Transformations - Setup Guide

## Overview

Today we'll set up an environment to practice watermarking, windowing, and advanced streaming transformations. We'll create time-series event data with realistic late arrivals.

## Prerequisites

- Completed Day 16 setup
- Databricks workspace access
- Cluster with DBR 13.0+

## Setup Steps

### Step 1: Create Databases

```sql
-- Reuse streaming databases from Day 16
-- Or create if not exists
CREATE DATABASE IF NOT EXISTS streaming_bronze;
CREATE DATABASE IF NOT EXISTS streaming_silver;
CREATE DATABASE IF NOT EXISTS streaming_gold;
```

### Step 2: Generate Time-Series Event Data

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import time

# ============================================================================
# Time-Series Event Generator
# ============================================================================

def generate_events_with_timestamps(batch_id, num_events=100, base_time=None):
    """Generate events with realistic timestamps and late arrivals"""
    
    if base_time is None:
        base_time = datetime.now()
    
    data = []
    event_types = ["page_view", "click", "purchase", "add_to_cart", "search"]
    
    for i in range(num_events):
        # Most events are recent
        if random.random() > 0.1:  # 90% recent
            event_time = base_time - timedelta(seconds=random.randint(0, 300))  # Last 5 min
        else:  # 10% late arrivals
            event_time = base_time - timedelta(seconds=random.randint(300, 1800))  # 5-30 min late
        
        data.append({
            "event_id": f"EVT{batch_id:04d}{i:04d}",
            "user_id": f"USER{random.randint(1, 100):03d}",
            "event_type": random.choice(event_types),
            "value": round(random.uniform(1, 100), 2),
            "event_time": event_time.strftime("%Y-%m-%d %H:%M:%S"),
            "processing_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    
    return data

# Generate initial batches
print("Generating time-series event data...")

for batch_id in range(10):
    events = generate_events_with_timestamps(batch_id, 100)
    df = spark.createDataFrame(events)
    
    (df.coalesce(1)
        .write
        .format("json")
        .mode("append")
        .save(f"/tmp/streaming/events/batch_{batch_id:04d}/")
    )
    
    print(f"✓ Generated batch {batch_id}")
    time.sleep(1)  # Small delay between batches

print(f"✓ Generated 10 batches (1,000 events)")
```

### Step 3: Generate Click-Stream Data for Joins

```python
# ============================================================================
# Generate Impressions and Clicks for Stream-Stream Joins
# ============================================================================

def generate_impressions(batch_id, num_impressions=50):
    """Generate ad impressions"""
    data = []
    base_time = datetime.now()
    
    for i in range(num_impressions):
        impression_time = base_time - timedelta(seconds=random.randint(0, 600))
        
        data.append({
            "impression_id": f"IMP{batch_id:04d}{i:04d}",
            "ad_id": f"AD{random.randint(1, 20):03d}",
            "user_id": f"USER{random.randint(1, 100):03d}",
            "impression_time": impression_time.strftime("%Y-%m-%d %H:%M:%S")
        })
    
    return data

def generate_clicks(batch_id, num_clicks=10):
    """Generate ad clicks (subset of impressions)"""
    data = []
    base_time = datetime.now()
    
    for i in range(num_clicks):
        # Click happens after impression
        click_time = base_time - timedelta(seconds=random.randint(0, 300))
        
        # Reference an impression (may or may not exist)
        imp_batch = random.randint(max(0, batch_id-2), batch_id)
        imp_id = random.randint(0, 49)
        
        data.append({
            "click_id": f"CLK{batch_id:04d}{i:04d}",
            "impression_id": f"IMP{imp_batch:04d}{imp_id:04d}",
            "click_time": click_time.strftime("%Y-%m-%d %H:%M:%S")
        })
    
    return data

# Generate impressions and clicks
print("Generating click-stream data...")

for batch_id in range(10):
    # Impressions
    impressions = generate_impressions(batch_id, 50)
    imp_df = spark.createDataFrame(impressions)
    (imp_df.coalesce(1)
        .write
        .format("json")
        .mode("append")
        .save(f"/tmp/streaming/impressions/batch_{batch_id:04d}/")
    )
    
    # Clicks
    clicks = generate_clicks(batch_id, 10)
    click_df = spark.createDataFrame(clicks)
    (click_df.coalesce(1)
        .write
        .format("json")
        .mode("append")
        .save(f"/tmp/streaming/clicks/batch_{batch_id:04d}/")
    )
    
    print(f"✓ Generated batch {batch_id}: {len(impressions)} impressions, {len(clicks)} clicks")

print("✓ Click-stream data generated")
```

### Step 4: Create Checkpoint Directories

```python
# Create checkpoint directories for Day 17
checkpoint_dirs = [
    "/tmp/streaming/checkpoints/watermark_test",
    "/tmp/streaming/checkpoints/windowed_agg",
    "/tmp/streaming/checkpoints/dedup",
    "/tmp/streaming/checkpoints/stream_join",
    "/tmp/streaming/checkpoints/late_data"
]

for dir_path in checkpoint_dirs:
    dbutils.fs.mkdirs(dir_path)

print("✓ Checkpoint directories created")
```

### Step 5: Create Helper Functions

```python
# ============================================================================
# Helper Functions for Watermarking and Windowing
# ============================================================================

def create_watermarked_stream(source_path, time_column, watermark_delay):
    """Create streaming DataFrame with watermark"""
    
    df = (
        spark.readStream
        .format("json")
        .load(source_path)
        .withColumn(time_column, to_timestamp(col(time_column)))
    )
    
    return df.withWatermark(time_column, watermark_delay)

def create_windowed_aggregation(df, time_column, window_duration, 
                                group_columns=None, agg_exprs=None):
    """Create windowed aggregation"""
    
    if group_columns is None:
        group_columns = []
    
    if agg_exprs is None:
        agg_exprs = [count("*").alias("count")]
    
    group_cols = [window(col(time_column), window_duration)] + group_columns
    
    return df.groupBy(*group_cols).agg(*agg_exprs)

def monitor_watermark(query, duration=30):
    """Monitor watermark progress"""
    import time
    start = time.time()
    
    while time.time() - start < duration:
        if query.isActive:
            progress = query.lastProgress
            if progress and "watermark" in progress:
                print(f"Watermark: {progress['watermark']}")
        time.sleep(5)

print("✓ Helper functions loaded")
```

### Step 6: Verify Setup

```python
print("="*80)
print("STREAMING TRANSFORMATIONS SETUP VERIFICATION")
print("="*80)

# Check event data
print("\n=== Event Data ===")
event_files = dbutils.fs.ls("/tmp/streaming/events/")
print(f"Event batches: {len(event_files)}")

# Sample events
print("\n=== Sample Events ===")
sample_events = spark.read.json("/tmp/streaming/events/batch_0000/")
sample_events.show(5)

# Check click-stream data
print("\n=== Click-Stream Data ===")
impression_files = dbutils.fs.ls("/tmp/streaming/impressions/")
click_files = dbutils.fs.ls("/tmp/streaming/clicks/")
print(f"Impression batches: {len(impression_files)}")
print(f"Click batches: {len(click_files)}")

# Sample impressions and clicks
print("\n=== Sample Impressions ===")
spark.read.json("/tmp/streaming/impressions/batch_0000/").show(3)

print("\n=== Sample Clicks ===")
spark.read.json("/tmp/streaming/clicks/batch_0000/").show(3)

print("\n" + "="*80)
print("✅ Setup Complete! Ready for watermarking and windowing exercises.")
print("="*80)
```

## Data Summary

### Event Data
- **Location**: `/tmp/streaming/events/`
- **Format**: JSON
- **Batches**: 10 batches (100 events each)
- **Total**: 1,000 events
- **Late Data**: ~10% of events are 5-30 minutes late
- **Schema**: event_id, user_id, event_type, value, event_time, processing_time

### Click-Stream Data
- **Impressions**: `/tmp/streaming/impressions/` (500 total)
- **Clicks**: `/tmp/streaming/clicks/` (100 total)
- **Click Rate**: ~20%
- **Time Window**: Clicks within 10 minutes of impressions

## Key Concepts to Practice

1. **Watermarking**: Handle late-arriving data
2. **Tumbling Windows**: Non-overlapping time windows
3. **Sliding Windows**: Overlapping time windows
4. **Stream-Stream Joins**: Join impressions with clicks
5. **Deduplication**: Remove duplicate events
6. **State Management**: Monitor and optimize state

## Troubleshooting

### Issue: Watermark not advancing
**Solution**: Ensure events have timestamps and data is flowing

### Issue: State growing too large
**Solution**: Reduce watermark delay or increase window size

### Issue: Too much late data dropped
**Solution**: Increase watermark delay

### Issue: Stream-stream join not working
**Solution**: Ensure both streams have watermarks and time constraints

## Next Steps

After completing setup:
1. Proceed to `exercise.py` for hands-on practice
2. Experiment with different watermark delays
3. Compare tumbling vs sliding windows
4. Practice stream-stream joins
5. Complete the quiz

---

**Setup Time**: 10-15 minutes  
**Data Generated**: 1,000 events, 500 impressions, 100 clicks  
**Ready for**: Watermarking and windowing exercises

