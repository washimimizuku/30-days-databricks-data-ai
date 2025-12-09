# Day 20: Setup - Multi-Hop Architecture

## Environment Setup

### 1. Create Databases

```python
# Create databases for each layer
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# Set default database
spark.sql("USE bronze")

# Create checkpoint directories
checkpoint_base = "/tmp/checkpoints/day20"
dbutils.fs.mkdirs(checkpoint_base)
dbutils.fs.mkdirs(f"{checkpoint_base}/bronze")
dbutils.fs.mkdirs(f"{checkpoint_base}/silver")
dbutils.fs.mkdirs(f"{checkpoint_base}/gold")

print("✓ Databases and checkpoints created")
```

### 2. Sample Data Creation

#### Dataset 1: Raw Event Data (Bronze Source)

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import json

# Generate raw event data (simulating source system)
raw_events = []
base_time = datetime(2024, 1, 1, 10, 0, 0)

for i in range(1, 101):
    event_time = base_time + timedelta(minutes=random.randint(0, 1440))
    raw_events.append({
        "event_id": i,
        "user_id": random.randint(1, 20),
        "event_type": random.choice(["login", "view", "click", "purchase"]),
        "amount": round(random.uniform(10, 500), 2) if random.random() > 0.3 else None,
        "timestamp": event_time.strftime("%Y-%m-%d %H:%M:%S"),
        "device": random.choice(["mobile", "desktop", "tablet"]),
        "country": random.choice(["US", "UK", "CA", "AU"])
    })

# Add some duplicates and bad data
raw_events.append(raw_events[0])  # Duplicate
raw_events.append({"event_id": None, "user_id": 1, "event_type": "bad"})  # Bad data

# Create Bronze table (raw data)
bronze_schema = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("device", StringType(), True),
    StructField("country", StringType(), True)
])

bronze_df = spark.createDataFrame([tuple(e.values()) for e in raw_events], 
                                  schema=["event_id", "user_id", "event_type", "amount", 
                                         "timestamp", "device", "country"])

# Add ingestion metadata
bronze_with_metadata = (
    bronze_df
    .withColumn("ingestion_time", current_timestamp())
    .withColumn("source_file", lit("raw_events_batch_1.json"))
)

# Write to Bronze
(bronze_with_metadata
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("bronze.events")
)

print(f"✓ Created bronze.events with {bronze_with_metadata.count()} records (includes duplicates and bad data)")
```

#### Dataset 2: Additional Raw Data Batches

```python
# Batch 2 - More recent data
raw_events_batch2 = []
base_time2 = datetime(2024, 1, 2, 10, 0, 0)

for i in range(101, 151):
    event_time = base_time2 + timedelta(minutes=random.randint(0, 720))
    raw_events_batch2.append({
        "event_id": i,
        "user_id": random.randint(1, 20),
        "event_type": random.choice(["login", "view", "click", "purchase"]),
        "amount": round(random.uniform(10, 500), 2) if random.random() > 0.3 else None,
        "timestamp": event_time.strftime("%Y-%m-%d %H:%M:%S"),
        "device": random.choice(["mobile", "desktop", "tablet"]),
        "country": random.choice(["US", "UK", "CA", "AU"])
    })

bronze_df_batch2 = spark.createDataFrame([tuple(e.values()) for e in raw_events_batch2],
                                         schema=["event_id", "user_id", "event_type", "amount",
                                                "timestamp", "device", "country"])

bronze_batch2_with_metadata = (
    bronze_df_batch2
    .withColumn("ingestion_time", current_timestamp())
    .withColumn("source_file", lit("raw_events_batch_2.json"))
)

# Save for incremental processing exercises
(bronze_batch2_with_metadata
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("bronze.events_batch2")
)

print(f"✓ Created bronze.events_batch2 with {bronze_batch2_with_metadata.count()} records")
```

#### Dataset 3: Empty Silver Table

```python
# Create empty Silver table with proper schema
silver_schema = StructType([
    StructField("event_id", IntegerType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("event_type", StringType(), False),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", TimestampType(), False),
    StructField("device", StringType(), True),
    StructField("country", StringType(), True),
    StructField("ingestion_time", TimestampType(), False),
    StructField("processed_time", TimestampType(), False)
])

empty_silver_df = spark.createDataFrame([], silver_schema)

(empty_silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver.events")
)

print("✓ Created empty silver.events table")
```

#### Dataset 4: Empty Gold Tables

```python
# Gold - Daily Metrics
gold_daily_schema = StructType([
    StructField("date", DateType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_count", LongType(), False),
    StructField("total_amount", DoubleType(), True),
    StructField("avg_amount", DoubleType(), True),
    StructField("unique_users", LongType(), False)
])

empty_gold_daily = spark.createDataFrame([], gold_daily_schema)

(empty_gold_daily
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold.daily_metrics")
)

print("✓ Created empty gold.daily_metrics table")

# Gold - Hourly Metrics
gold_hourly_schema = StructType([
    StructField("hour_start", TimestampType(), False),
    StructField("hour_end", TimestampType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_count", LongType(), False),
    StructField("total_amount", DoubleType(), True),
    StructField("unique_users", LongType(), False)
])

empty_gold_hourly = spark.createDataFrame([], gold_hourly_schema)

(empty_gold_hourly
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold.hourly_metrics")
)

print("✓ Created empty gold.hourly_metrics table")

# Gold - User Summary
gold_user_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("total_events", LongType(), False),
    StructField("total_amount", DoubleType(), True),
    StructField("first_event", TimestampType(), True),
    StructField("last_event", TimestampType(), True),
    StructField("favorite_device", StringType(), True)
])

empty_gold_user = spark.createDataFrame([], gold_user_schema)

(empty_gold_user
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold.user_summary")
)

print("✓ Created empty gold.user_summary table")
```

#### Dataset 5: Product Data (for enrichment)

```python
# Dimension table for enrichment
products_data = [
    (1, "Premium Subscription", "subscription", 99.99),
    (2, "Basic Subscription", "subscription", 49.99),
    (3, "E-book", "digital", 19.99),
    (4, "Video Course", "digital", 79.99),
    (5, "Consulting Hour", "service", 150.00)
]

products_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("price", DoubleType(), False)
])

products_df = spark.createDataFrame(products_data, products_schema)

(products_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver.products")
)

print("✓ Created silver.products dimension table")
```

### 3. Helper Functions

```python
def show_layer_summary():
    """Display record counts for all layers"""
    print("\n" + "="*70)
    print("MEDALLION ARCHITECTURE - LAYER SUMMARY")
    print("="*70)
    
    # Bronze
    bronze_count = spark.table("bronze.events").count()
    print(f"\n🥉 BRONZE LAYER (Raw Data)")
    print(f"   bronze.events: {bronze_count} records")
    
    # Silver
    silver_count = spark.table("silver.events").count()
    print(f"\n🥈 SILVER LAYER (Cleaned Data)")
    print(f"   silver.events: {silver_count} records")
    print(f"   silver.products: {spark.table('silver.products').count()} records")
    
    # Gold
    print(f"\n🥇 GOLD LAYER (Business Aggregates)")
    print(f"   gold.daily_metrics: {spark.table('gold.daily_metrics').count()} records")
    print(f"   gold.hourly_metrics: {spark.table('gold.hourly_metrics').count()} records")
    print(f"   gold.user_summary: {spark.table('gold.user_summary').count()} records")
    
    print("="*70 + "\n")

def show_data_quality_issues():
    """Show data quality issues in Bronze"""
    print("\n" + "="*70)
    print("BRONZE LAYER - DATA QUALITY ISSUES")
    print("="*70)
    
    bronze_df = spark.table("bronze.events")
    
    total = bronze_df.count()
    null_event_ids = bronze_df.filter(col("event_id").isNull()).count()
    null_timestamps = bronze_df.filter(col("timestamp").isNull()).count()
    duplicates = total - bronze_df.select("event_id").distinct().count()
    
    print(f"Total records: {total}")
    print(f"Null event_ids: {null_event_ids}")
    print(f"Null timestamps: {null_timestamps}")
    print(f"Duplicate event_ids: {duplicates}")
    print("="*70 + "\n")

def compare_layers(event_id):
    """Compare how a record looks across layers"""
    print(f"\n{'='*70}")
    print(f"RECORD COMPARISON ACROSS LAYERS - Event ID: {event_id}")
    print('='*70)
    
    # Bronze
    print("\n🥉 BRONZE (Raw):")
    spark.table("bronze.events").filter(col("event_id") == event_id).show(truncate=False)
    
    # Silver
    print("\n🥈 SILVER (Cleaned):")
    silver_df = spark.table("silver.events").filter(col("event_id") == event_id)
    if silver_df.count() > 0:
        silver_df.show(truncate=False)
    else:
        print("   (Not yet processed to Silver)")
    
    print('='*70 + "\n")

def show_pipeline_flow():
    """Visualize the pipeline flow"""
    print("""
╔════════════════════════════════════════════════════════════════════╗
║                    MEDALLION ARCHITECTURE FLOW                     ║
╠════════════════════════════════════════════════════════════════════╣
║                                                                    ║
║  📁 Raw Files                                                      ║
║       ↓                                                            ║
║  🥉 BRONZE LAYER (bronze.events)                                   ║
║       • Raw data as-is                                             ║
║       • Minimal transformation                                     ║
║       • Includes bad data & duplicates                             ║
║       • Add ingestion metadata                                     ║
║       ↓                                                            ║
║  🥈 SILVER LAYER (silver.events)                                   ║
║       • Data quality checks                                        ║
║       • Deduplication                                              ║
║       • Type conversions                                           ║
║       • Standardization                                            ║
║       • Enrichment                                                 ║
║       ↓                                                            ║
║  🥇 GOLD LAYER (gold.*)                                            ║
║       • Business aggregates                                        ║
║       • Daily/hourly metrics                                       ║
║       • User summaries                                             ║
║       • Ready for BI tools                                         ║
║                                                                    ║
╚════════════════════════════════════════════════════════════════════╝
    """)

# Display initial state
show_layer_summary()
show_data_quality_issues()
show_pipeline_flow()
```

### 4. Quick Reference

```python
print("""
╔════════════════════════════════════════════════════════════════════╗
║         DAY 20: MULTI-HOP ARCHITECTURE - QUICK REFERENCE           ║
╠════════════════════════════════════════════════════════════════════╣
║ Databases Created:                                                 ║
║   • bronze - Raw data layer                                        ║
║   • silver - Cleaned data layer                                    ║
║   • gold - Business aggregates layer                               ║
║                                                                    ║
║ Tables Created:                                                    ║
║   🥉 Bronze:                                                        ║
║      • bronze.events (102 records with issues)                     ║
║      • bronze.events_batch2 (50 records for incremental)           ║
║                                                                    ║
║   🥈 Silver:                                                        ║
║      • silver.events (empty - to be populated)                     ║
║      • silver.products (5 products for enrichment)                 ║
║                                                                    ║
║   🥇 Gold:                                                          ║
║      • gold.daily_metrics (empty)                                  ║
║      • gold.hourly_metrics (empty)                                 ║
║      • gold.user_summary (empty)                                   ║
║                                                                    ║
║ Helper Functions:                                                  ║
║   • show_layer_summary()                                           ║
║   • show_data_quality_issues()                                     ║
║   • compare_layers(event_id)                                       ║
║   • show_pipeline_flow()                                           ║
║                                                                    ║
║ Key Concepts:                                                      ║
║   • Bronze = Raw data (as-is)                                      ║
║   • Silver = Cleaned & validated                                   ║
║   • Gold = Business aggregates                                     ║
║   • Incremental processing at each layer                           ║
╚════════════════════════════════════════════════════════════════════╝
""")
```

## Setup Complete! ✅

You now have:
- ✅ Three databases (bronze, silver, gold)
- ✅ Bronze layer with raw data (includes quality issues)
- ✅ Empty Silver and Gold tables ready for processing
- ✅ Dimension table for enrichment
- ✅ Helper functions for analysis
- ✅ Ready for multi-hop pipeline exercises!

**Next Step**: Open `exercise.py` and start building your Medallion Architecture!
