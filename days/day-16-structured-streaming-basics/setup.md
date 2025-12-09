# Day 16: Structured Streaming Basics - Setup Guide

## Overview

Today we'll set up a streaming environment to practice Structured Streaming concepts. We'll create sample streaming data sources and configure the necessary infrastructure for streaming queries.

## Prerequisites

- Databricks workspace access
- Cluster with DBR 13.0+
- Python 3.8+
- Permissions to create databases and tables

## Setup Steps

### Step 1: Create Databases

```sql
-- Create databases for streaming project
CREATE DATABASE IF NOT EXISTS streaming_bronze
COMMENT 'Streaming project - Bronze layer';

CREATE DATABASE IF NOT EXISTS streaming_silver
COMMENT 'Streaming project - Silver layer';

CREATE DATABASE IF NOT EXISTS streaming_gold
COMMENT 'Streaming project - Gold layer';

-- Verify
SHOW DATABASES LIKE 'streaming%';
```

### Step 2: Create Checkpoint Directories

```python
# Create checkpoint directories
checkpoint_dirs = [
    "/tmp/streaming/checkpoints/bronze_orders",
    "/tmp/streaming/checkpoints/silver_orders",
    "/tmp/streaming/checkpoints/gold_metrics",
    "/tmp/streaming/checkpoints/schema_location",
    "/tmp/streaming/checkpoints/test_queries"
]

for dir_path in checkpoint_dirs:
    dbutils.fs.mkdirs(dir_path)

print("✓ Checkpoint directories created")
```

### Step 3: Generate Streaming Data Source

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import time
import json

# ============================================================================
# Data Generator for Streaming
# ============================================================================

def generate_order_batch(batch_id, num_orders=100):
    """Generate a batch of orders"""
    
    data = []
    base_time = datetime.now()
    
    statuses = ["pending", "completed", "cancelled", "shipped"]
    payment_methods = ["credit_card", "debit_card", "paypal", "bank_transfer"]
    
    for i in range(num_orders):
        order_time = base_time + timedelta(seconds=random.randint(0, 60))
        
        data.append({
            "order_id": f"ORD{batch_id:04d}{i:04d}",
            "customer_id": random.randint(1, 1000),
            "product_id": f"PROD{random.randint(1, 100):03d}",
            "quantity": random.randint(1, 10),
            "amount": round(random.uniform(10, 1000), 2),
            "status": random.choice(statuses),
            "payment_method": random.choice(payment_methods),
            "timestamp": order_time.strftime("%Y-%m-%d %H:%M:%S"),
            "order_date": order_time.strftime("%Y-%m-%d")
        })
    
    return data

# Generate initial batches
print("Generating initial streaming data...")

for batch_id in range(10):
    orders = generate_order_batch(batch_id, 100)
    
    # Convert to DataFrame
    df = spark.createDataFrame(orders)
    
    # Write as JSON files (simulating streaming source)
    (df.coalesce(1)
        .write
        .format("json")
        .mode("append")
        .save(f"/tmp/streaming/input/orders/batch_{batch_id:04d}/")
    )
    
    print(f"✓ Generated batch {batch_id}")

print(f"✓ Generated 10 batches (1,000 orders)")
```

### Step 4: Create Continuous Data Generator

```python
# ============================================================================
# Continuous Data Generator (for testing)
# ============================================================================

def start_continuous_generator(output_path, interval_seconds=10, num_batches=None):
    """
    Generate streaming data continuously
    
    Args:
        output_path: Where to write data
        interval_seconds: Seconds between batches
        num_batches: Number of batches (None = infinite)
    """
    
    batch_id = 100  # Start from 100 to avoid conflicts
    
    try:
        while num_batches is None or batch_id < (100 + num_batches):
            # Generate batch
            orders = generate_order_batch(batch_id, 50)
            df = spark.createDataFrame(orders)
            
            # Write batch
            (df.coalesce(1)
                .write
                .format("json")
                .mode("append")
                .save(f"{output_path}/batch_{batch_id:04d}/")
            )
            
            print(f"✓ Generated batch {batch_id} at {datetime.now()}")
            
            batch_id += 1
            
            # Wait before next batch
            if num_batches is None or batch_id < (100 + num_batches):
                time.sleep(interval_seconds)
                
    except KeyboardInterrupt:
        print(f"\n✓ Generator stopped after {batch_id - 100} batches")

# Example usage (run in separate cell if needed):
# start_continuous_generator("/tmp/streaming/input/orders", interval_seconds=10, num_batches=5)
```

### Step 5: Create Static Dimension Tables

```python
# ============================================================================
# Create Static Dimension Tables for Enrichment
# ============================================================================

# Customers dimension
customers_data = [
    {"customer_id": i, "customer_name": f"Customer {i}", 
     "segment": random.choice(["Premium", "Standard", "Basic"]),
     "country": random.choice(["USA", "Canada", "UK", "Germany"])}
    for i in range(1, 1001)
]

customers_df = spark.createDataFrame(customers_data)
customers_df.write.format("delta").mode("overwrite").saveAsTable("streaming_bronze.customers")

print(f"✓ Created customers dimension: {customers_df.count()} records")

# Products dimension
products_data = [
    {"product_id": f"PROD{i:03d}", "product_name": f"Product {i}",
     "category": random.choice(["Electronics", "Clothing", "Home", "Sports", "Books"]),
     "price": round(random.uniform(10, 500), 2)}
    for i in range(1, 101)
]

products_df = spark.createDataFrame(products_data)
products_df.write.format("delta").mode("overwrite").saveAsTable("streaming_bronze.products")

print(f"✓ Created products dimension: {products_df.count()} records")
```

### Step 6: Create Helper Functions

```python
# ============================================================================
# Helper Functions for Streaming
# ============================================================================

def create_streaming_reader(source_path, format_type="json", schema=None):
    """Create a streaming reader with common options"""
    
    reader = spark.readStream.format(format_type)
    
    if schema:
        reader = reader.schema(schema)
    
    if format_type == "json":
        reader = reader.option("maxFilesPerTrigger", 1)
    
    return reader.load(source_path)

def create_streaming_writer(df, output_table, checkpoint_path, 
                            output_mode="append", trigger_interval="10 seconds"):
    """Create a streaming writer with common options"""
    
    return (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode(output_mode)
        .trigger(processingTime=trigger_interval)
        .table(output_table)
    )

def monitor_query(query, duration_seconds=30):
    """Monitor a streaming query for specified duration"""
    
    import time
    start_time = time.time()
    
    while time.time() - start_time < duration_seconds:
        if query.isActive:
            progress = query.lastProgress
            if progress:
                print(f"Batch {progress['batchId']}: "
                      f"{progress['numInputRows']} rows, "
                      f"{progress['durationMs']['triggerExecution']}ms")
        time.sleep(5)
    
    return query.status

def stop_all_streams():
    """Stop all active streaming queries"""
    for query in spark.streams.active:
        print(f"Stopping query: {query.name or query.id}")
        query.stop()
    print("✓ All streams stopped")

print("✓ Helper functions loaded")
```

### Step 7: Create Sample Streaming Query (Test)

```python
# ============================================================================
# Test Streaming Query
# ============================================================================

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

# Create test streaming query
test_stream = (
    spark.readStream
    .format("json")
    .schema(orders_schema)
    .option("maxFilesPerTrigger", 1)
    .load("/tmp/streaming/input/orders/")
)

# Display to console (for testing)
test_query = (
    test_stream
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 5)
    .start()
)

print("✓ Test streaming query started")
print(f"Query ID: {test_query.id}")
print("Monitoring for 30 seconds...")

# Monitor for 30 seconds
import time
time.sleep(30)

# Stop test query
test_query.stop()
print("✓ Test query stopped")
```

### Step 8: Verify Setup

```python
# ============================================================================
# Verification
# ============================================================================

print("="*80)
print("STREAMING SETUP VERIFICATION")
print("="*80)

# Check databases
print("\n=== Databases ===")
spark.sql("SHOW DATABASES LIKE 'streaming%'").show()

# Check dimension tables
print("\n=== Dimension Tables ===")
print(f"Customers: {spark.table('streaming_bronze.customers').count():,}")
print(f"Products: {spark.table('streaming_bronze.products').count():,}")

# Check input files
print("\n=== Input Files ===")
input_files = dbutils.fs.ls("/tmp/streaming/input/orders/")
print(f"Batches available: {len(input_files)}")

# Check checkpoints
print("\n=== Checkpoint Directories ===")
checkpoints = dbutils.fs.ls("/tmp/streaming/checkpoints/")
print(f"Checkpoint dirs: {len(checkpoints)}")

# Sample data
print("\n=== Sample Input Data ===")
sample_df = spark.read.json("/tmp/streaming/input/orders/batch_0000/")
sample_df.show(5)

print("\n" + "="*80)
print("✅ Setup Complete! Ready for streaming exercises.")
print("="*80)
```

## Data Summary

### Streaming Orders
- **Location**: `/tmp/streaming/input/orders/`
- **Format**: JSON
- **Batches**: 10 initial batches (100 orders each)
- **Total Records**: 1,000 orders
- **Schema**: order_id, customer_id, product_id, quantity, amount, status, payment_method, timestamp, order_date

### Customers Dimension
- **Table**: `streaming_bronze.customers`
- **Records**: 1,000
- **Segments**: Premium, Standard, Basic
- **Countries**: USA, Canada, UK, Germany

### Products Dimension
- **Table**: `streaming_bronze.products`
- **Records**: 100
- **Categories**: Electronics, Clothing, Home, Sports, Books
- **Price Range**: $10 - $500

## Streaming Architecture

```
Input Files (JSON)
    ↓
readStream
    ↓
Transformations
    ↓
writeStream → Checkpoint
    ↓
Delta Tables (Bronze/Silver/Gold)
```

## Common Commands

### Start Streaming Query
```python
query = (
    df.writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/streaming/checkpoints/my_query")
    .table("output_table")
)
```

### Monitor Query
```python
# Check status
query.status

# View progress
query.lastProgress

# List active queries
spark.streams.active
```

### Stop Query
```python
# Stop specific query
query.stop()

# Stop all queries
stop_all_streams()
```

### Generate More Data
```python
# Generate 5 more batches
start_continuous_generator(
    "/tmp/streaming/input/orders",
    interval_seconds=10,
    num_batches=5
)
```

## Troubleshooting

### Issue: No data being processed
**Solution**: Check if input files exist
```python
dbutils.fs.ls("/tmp/streaming/input/orders/")
```

### Issue: Checkpoint errors
**Solution**: Clear checkpoint and restart
```python
dbutils.fs.rm("/tmp/streaming/checkpoints/my_query", True)
```

### Issue: Query not starting
**Solution**: Check for schema mismatches
```python
# Verify schema
df = spark.read.json("/tmp/streaming/input/orders/batch_0000/")
df.printSchema()
```

### Issue: Out of memory
**Solution**: Reduce batch size
```python
.option("maxFilesPerTrigger", 1)  # Process fewer files per batch
```

## Cleanup

```python
# Stop all streaming queries
stop_all_streams()

# Clean up checkpoints
dbutils.fs.rm("/tmp/streaming/checkpoints/", True)

# Clean up input data
dbutils.fs.rm("/tmp/streaming/input/", True)

# Drop tables
spark.sql("DROP DATABASE IF EXISTS streaming_bronze CASCADE")
spark.sql("DROP DATABASE IF EXISTS streaming_silver CASCADE")
spark.sql("DROP DATABASE IF EXISTS streaming_gold CASCADE")

print("✓ Cleanup complete")
```

## Next Steps

After completing setup:
1. Proceed to `exercise.py` for hands-on streaming practice
2. Create your first streaming query
3. Experiment with different output modes
4. Practice monitoring streaming queries
5. Complete the quiz to test your knowledge

---

**Setup Time**: 10-15 minutes  
**Data Generated**: 1,000 orders, 1,000 customers, 100 products  
**Ready for**: Structured Streaming exercises

