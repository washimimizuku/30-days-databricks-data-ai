# Day 22 Setup: Week 3 Review & Incremental Project

## Overview

This setup creates a comprehensive environment for reviewing Week 3 concepts and building an end-to-end incremental data processing pipeline. You'll work with streaming data, CDC events, and a multi-hop architecture.

## Project Scenario

You're building a real-time analytics platform for an e-commerce company. The system needs to:
- Ingest streaming order and customer events
- Process CDC changes from the operational database
- Maintain historical customer data (SCD Type 2)
- Build a medallion architecture (Bronze → Silver → Gold)
- Optimize tables for query performance

## Setup Instructions

### Step 1: Create Database

```sql
-- Create database for Week 3 review
CREATE DATABASE IF NOT EXISTS week3_review;
USE week3_review;
```

### Step 2: Create Bronze Layer Tables

```python
# Bronze: Raw streaming events
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_orders (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    amount DECIMAL(10,2),
    order_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
USING DELTA
PARTITIONED BY (DATE(ingestion_timestamp))
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_customer_events (
    event_id STRING,
    customer_id STRING,
    event_type STRING,
    event_data STRING,
    event_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (DATE(ingestion_timestamp))
""")

# Bronze: CDC events from operational database
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_cdc_customers (
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
)
USING DELTA
""")
```

### Step 3: Create Silver Layer Tables

```python
# Silver: Cleaned orders
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_orders (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    amount DECIMAL(10,2),
    order_timestamp TIMESTAMP,
    processed_timestamp TIMESTAMP
)
USING DELTA
""")

# Silver: Customer dimension (SCD Type 2)
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_customers (
    customer_id STRING,
    name STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_current BOOLEAN,
    record_version INT
)
USING DELTA
""")

# Silver: Product dimension
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_products (
    product_id STRING,
    product_name STRING,
    category STRING,
    price DECIMAL(10,2),
    last_updated TIMESTAMP
)
USING DELTA
""")
```

### Step 4: Create Gold Layer Tables

```python
# Gold: Daily sales summary
spark.sql("""
CREATE TABLE IF NOT EXISTS gold_daily_sales (
    sale_date DATE,
    total_orders INT,
    total_revenue DECIMAL(12,2),
    unique_customers INT,
    avg_order_value DECIMAL(10,2),
    last_updated TIMESTAMP
)
USING DELTA
PARTITIONED BY (sale_date)
""")

# Gold: Customer lifetime value
spark.sql("""
CREATE TABLE IF NOT EXISTS gold_customer_ltv (
    customer_id STRING,
    total_orders INT,
    total_revenue DECIMAL(12,2),
    first_order_date DATE,
    last_order_date DATE,
    avg_order_value DECIMAL(10,2),
    customer_segment STRING,
    last_updated TIMESTAMP
)
USING DELTA
""")

# Gold: Product performance
spark.sql("""
CREATE TABLE IF NOT EXISTS gold_product_performance (
    product_id STRING,
    product_name STRING,
    category STRING,
    total_quantity_sold INT,
    total_revenue DECIMAL(12,2),
    order_count INT,
    avg_quantity_per_order DECIMAL(10,2),
    last_updated TIMESTAMP
)
USING DELTA
""")

# Gold: Hourly metrics (for streaming aggregation)
spark.sql("""
CREATE TABLE IF NOT EXISTS gold_hourly_metrics (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    order_count INT,
    total_revenue DECIMAL(12,2),
    unique_customers INT,
    last_updated TIMESTAMP
)
USING DELTA
""")
```

### Step 5: Load Initial Data

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# Generate sample products
products_data = [
    ("P001", "Laptop", "Electronics", 999.99),
    ("P002", "Mouse", "Electronics", 29.99),
    ("P003", "Keyboard", "Electronics", 79.99),
    ("P004", "Monitor", "Electronics", 299.99),
    ("P005", "Desk Chair", "Furniture", 199.99),
    ("P006", "Desk", "Furniture", 399.99),
    ("P007", "Notebook", "Office Supplies", 4.99),
    ("P008", "Pen Set", "Office Supplies", 12.99),
    ("P009", "Backpack", "Accessories", 49.99),
    ("P010", "Water Bottle", "Accessories", 19.99)
]

products_df = spark.createDataFrame(products_data, 
    ["product_id", "product_name", "category", "price"]) \
    .withColumn("last_updated", current_timestamp())

products_df.write.format("delta").mode("overwrite").saveAsTable("silver_products")

# Generate initial customer data
customers_data = []
for i in range(1, 101):
    customers_data.append((
        f"C{i:04d}",
        f"Customer {i}",
        f"customer{i}@email.com",
        f"555-{random.randint(1000, 9999)}",
        f"{random.randint(100, 9999)} Main St",
        random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]),
        random.choice(["NY", "CA", "IL", "TX", "AZ"]),
        f"{random.randint(10000, 99999)}"
    ))

customers_df = spark.createDataFrame(customers_data,
    ["customer_id", "name", "email", "phone", "address", "city", "state", "zip_code"]) \
    .withColumn("effective_start_date", lit(datetime(2024, 1, 1))) \
    .withColumn("effective_end_date", lit(None).cast("timestamp")) \
    .withColumn("is_current", lit(True)) \
    .withColumn("record_version", lit(1))

customers_df.write.format("delta").mode("overwrite").saveAsTable("silver_customers")

# Generate historical orders (last 30 days)
orders_data = []
base_date = datetime.now() - timedelta(days=30)

for day in range(30):
    current_date = base_date + timedelta(days=day)
    # Generate 20-50 orders per day
    num_orders = random.randint(20, 50)
    
    for order_num in range(num_orders):
        order_time = current_date + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        customer_id = f"C{random.randint(1, 100):04d}"
        product_id = f"P{random.randint(1, 10):03d}"
        quantity = random.randint(1, 5)
        
        # Get product price
        price = dict(products_data)[product_id]
        amount = price * quantity
        
        orders_data.append((
            f"ORD{len(orders_data)+1:06d}",
            customer_id,
            product_id,
            quantity,
            amount,
            order_time,
            order_time
        ))

orders_df = spark.createDataFrame(orders_data,
    ["order_id", "customer_id", "product_id", "quantity", "amount", 
     "order_timestamp", "processed_timestamp"])

orders_df.write.format("delta").mode("overwrite").saveAsTable("silver_orders")

print(f"✅ Loaded {len(products_data)} products")
print(f"✅ Loaded {len(customers_data)} customers")
print(f"✅ Loaded {len(orders_data)} historical orders")
```

### Step 6: Generate Streaming Data Sources

```python
# Create directory for streaming sources
dbutils.fs.mkdirs("/tmp/week3_review/streaming_orders")
dbutils.fs.mkdirs("/tmp/week3_review/cdc_events")

# Function to generate streaming order batches
def generate_order_batch(batch_num, num_orders=10):
    """Generate a batch of orders for streaming"""
    orders = []
    base_time = datetime.now()
    
    for i in range(num_orders):
        order_time = base_time + timedelta(seconds=i*5)
        customer_id = f"C{random.randint(1, 100):04d}"
        product_id = f"P{random.randint(1, 10):03d}"
        quantity = random.randint(1, 5)
        
        # Get product price
        price = dict(products_data)[product_id]
        amount = price * quantity
        
        orders.append({
            "order_id": f"ORD{batch_num:04d}{i:03d}",
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": quantity,
            "amount": float(amount),
            "order_timestamp": order_time.isoformat(),
            "ingestion_timestamp": datetime.now().isoformat(),
            "source_file": f"batch_{batch_num}.json"
        })
    
    return orders

# Function to generate CDC events
def generate_cdc_batch(batch_num, num_events=5):
    """Generate CDC events for customers"""
    events = []
    operations = ["INSERT", "UPDATE", "DELETE"]
    
    for i in range(num_events):
        customer_id = f"C{random.randint(1, 100):04d}"
        operation = random.choice(operations)
        
        if operation == "DELETE":
            event = {
                "customer_id": customer_id,
                "name": None,
                "email": None,
                "phone": None,
                "address": None,
                "city": None,
                "state": None,
                "zip_code": None,
                "operation": operation,
                "operation_timestamp": datetime.now().isoformat(),
                "ingestion_timestamp": datetime.now().isoformat()
            }
        else:
            event = {
                "customer_id": customer_id,
                "name": f"Updated Customer {customer_id}",
                "email": f"updated_{customer_id}@email.com",
                "phone": f"555-{random.randint(1000, 9999)}",
                "address": f"{random.randint(100, 9999)} Updated St",
                "city": random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]),
                "state": random.choice(["NY", "CA", "IL", "TX", "AZ"]),
                "zip_code": f"{random.randint(10000, 99999)}",
                "operation": operation,
                "operation_timestamp": datetime.now().isoformat(),
                "ingestion_timestamp": datetime.now().isoformat()
            }
        
        events.append(event)
    
    return events

# Generate initial batches
import json

# Generate 3 order batches
for batch in range(1, 4):
    orders = generate_order_batch(batch, 10)
    batch_json = "\n".join([json.dumps(order) for order in orders])
    
    dbutils.fs.put(
        f"/tmp/week3_review/streaming_orders/batch_{batch}.json",
        batch_json,
        overwrite=True
    )

# Generate 3 CDC batches
for batch in range(1, 4):
    events = generate_cdc_batch(batch, 5)
    batch_json = "\n".join([json.dumps(event) for event in events])
    
    dbutils.fs.put(
        f"/tmp/week3_review/cdc_events/batch_{batch}.json",
        batch_json,
        overwrite=True
    )

print("✅ Generated streaming data sources")
print("   - Order batches: /tmp/week3_review/streaming_orders/")
print("   - CDC batches: /tmp/week3_review/cdc_events/")
```

### Step 7: Create Helper Functions

```python
# Helper function to display table statistics
def show_table_stats(table_name):
    """Display statistics for a Delta table"""
    print(f"\n{'='*60}")
    print(f"Statistics for {table_name}")
    print(f"{'='*60}")
    
    # Row count
    count = spark.table(table_name).count()
    print(f"Row count: {count:,}")
    
    # Table details
    details = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    print(f"Number of files: {details['numFiles']}")
    print(f"Size in bytes: {details['sizeInBytes']:,}")
    
    # Recent history
    print("\nRecent operations:")
    history = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 3")
    history.select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

# Helper function to check streaming query status
def check_streaming_queries():
    """Display status of all active streaming queries"""
    queries = spark.streams.active
    
    if not queries:
        print("No active streaming queries")
        return
    
    print(f"\n{'='*60}")
    print(f"Active Streaming Queries: {len(queries)}")
    print(f"{'='*60}")
    
    for query in queries:
        print(f"\nQuery ID: {query.id}")
        print(f"Name: {query.name}")
        print(f"Status: {query.status}")
        print(f"Recent Progress:")
        if query.recentProgress:
            latest = query.recentProgress[-1]
            print(f"  - Batch: {latest.get('batchId', 'N/A')}")
            print(f"  - Input Rows: {latest.get('numInputRows', 'N/A')}")
            print(f"  - Processing Time: {latest.get('durationMs', {}).get('triggerExecution', 'N/A')} ms")

# Helper function to simulate new data arrival
def simulate_new_orders(batch_num, num_orders=10):
    """Simulate new orders arriving"""
    orders = generate_order_batch(batch_num, num_orders)
    batch_json = "\n".join([json.dumps(order) for order in orders])
    
    dbutils.fs.put(
        f"/tmp/week3_review/streaming_orders/batch_{batch_num}.json",
        batch_json,
        overwrite=True
    )
    
    print(f"✅ Generated {num_orders} new orders in batch {batch_num}")

def simulate_cdc_events(batch_num, num_events=5):
    """Simulate CDC events"""
    events = generate_cdc_batch(batch_num, num_events)
    batch_json = "\n".join([json.dumps(event) for event in events])
    
    dbutils.fs.put(
        f"/tmp/week3_review/cdc_events/batch_{batch_num}.json",
        batch_json,
        overwrite=True
    )
    
    print(f"✅ Generated {num_events} CDC events in batch {batch_num}")

# Helper function to reset environment
def reset_environment():
    """Reset all tables and streaming sources"""
    tables = [
        "bronze_orders", "bronze_customer_events", "bronze_cdc_customers",
        "silver_orders", "silver_customers", "silver_products",
        "gold_daily_sales", "gold_customer_ltv", "gold_product_performance", "gold_hourly_metrics"
    ]
    
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    
    # Clean up streaming sources
    dbutils.fs.rm("/tmp/week3_review", recurse=True)
    
    # Stop all streaming queries
    for query in spark.streams.active:
        query.stop()
    
    print("✅ Environment reset complete")
    print("   Run setup again to recreate tables and data")

print("\n" + "="*60)
print("Setup Complete!")
print("="*60)
print("\nAvailable helper functions:")
print("  - show_table_stats(table_name)")
print("  - check_streaming_queries()")
print("  - simulate_new_orders(batch_num, num_orders)")
print("  - simulate_cdc_events(batch_num, num_events)")
print("  - reset_environment()")
print("\nYou're ready to start the Week 3 review exercises!")
```

## Verification

Run these commands to verify your setup:

```python
# Check all tables exist
tables = spark.sql("SHOW TABLES IN week3_review").collect()
print(f"Created {len(tables)} tables")

# Check data counts
print("\nData Summary:")
show_table_stats("silver_products")
show_table_stats("silver_customers")
show_table_stats("silver_orders")

# Check streaming sources
print("\nStreaming Sources:")
print("Order batches:", len(dbutils.fs.ls("/tmp/week3_review/streaming_orders/")))
print("CDC batches:", len(dbutils.fs.ls("/tmp/week3_review/cdc_events/")))
```

## Project Structure

```
week3_review/
├── Bronze Layer (Raw Data)
│   ├── bronze_orders (streaming ingestion)
│   ├── bronze_customer_events (event stream)
│   └── bronze_cdc_customers (CDC events)
│
├── Silver Layer (Cleaned Data)
│   ├── silver_orders (deduplicated, validated)
│   ├── silver_customers (SCD Type 2)
│   └── silver_products (dimension)
│
└── Gold Layer (Business Metrics)
    ├── gold_daily_sales (daily aggregates)
    ├── gold_customer_ltv (customer analytics)
    ├── gold_product_performance (product analytics)
    └── gold_hourly_metrics (real-time metrics)
```

## Next Steps

1. Complete the review exercises in `exercise.py`
2. Build streaming pipelines for each layer
3. Implement CDC processing with SCD Type 2
4. Apply optimization techniques
5. Take the 50-question quiz

---

**Note**: This setup creates a realistic environment for practicing all Week 3 concepts in an integrated project.
