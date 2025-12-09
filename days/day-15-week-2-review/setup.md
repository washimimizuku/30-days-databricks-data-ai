# Day 15: Week 2 Review & ELT Project - Setup Guide

## Overview

Today's setup prepares you for a comprehensive Week 2 review and an integrated ELT project. We'll create a realistic e-commerce dataset with multiple data sources and quality issues to practice everything learned in Days 8-14.

## Prerequisites

- Databricks workspace access
- Cluster with DBR 13.0+
- Completed Days 8-14
- Python 3.8+
- Permissions to create databases and tables

## Setup Steps

### Step 1: Create Project Databases

```sql
-- Create databases for the project
CREATE DATABASE IF NOT EXISTS week2_bronze
COMMENT 'Week 2 Project - Bronze layer (raw data)';

CREATE DATABASE IF NOT EXISTS week2_silver
COMMENT 'Week 2 Project - Silver layer (cleaned data)';

CREATE DATABASE IF NOT EXISTS week2_gold
COMMENT 'Week 2 Project - Gold layer (business metrics)';

CREATE DATABASE IF NOT EXISTS week2_monitoring
COMMENT 'Week 2 Project - Monitoring and quality metrics';

-- Verify databases
SHOW DATABASES LIKE 'week2%';
```

### Step 2: Generate E-Commerce Sample Data

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import json

# Set seed for reproducibility
random.seed(42)

# ============================================================================
# Generate Orders Data (with quality issues)
# ============================================================================

def generate_orders(num_orders=50000):
    """Generate orders with realistic quality issues"""
    
    data = []
    start_date = datetime(2024, 1, 1)
    
    statuses = ["pending", "completed", "cancelled", "shipped", "returned"]
    payment_methods = ["credit_card", "debit_card", "paypal", "bank_transfer", "cash"]
    
    for i in range(num_orders):
        order_date = start_date + timedelta(days=random.randint(0, 365))
        
        # Introduce quality issues
        order_id = f"ORD{i:06d}" if random.random() > 0.01 else None  # 1% nulls
        customer_id = random.randint(1, 5000) if random.random() > 0.02 else None  # 2% nulls
        
        # Some invalid amounts
        if random.random() > 0.03:
            amount = round(random.uniform(10, 2000), 2)
        else:
            amount = round(random.uniform(-100, 0), 2)  # 3% invalid
        
        # Inconsistent date formats
        if random.random() > 0.3:
            order_date_str = order_date.strftime("%Y-%m-%d %H:%M:%S")
        elif random.random() > 0.5:
            order_date_str = order_date.strftime("%m/%d/%Y")
        else:
            order_date_str = order_date.strftime("%Y-%m-%d")
        
        # Product info
        product_id = f"PROD{random.randint(1, 500):04d}"
        quantity = random.randint(1, 20)
        
        # Shipping info
        shipping_cost = round(random.uniform(0, 50), 2)
        
        # Status with inconsistent casing
        status = random.choice(statuses)
        if random.random() > 0.5:
            status = status.upper()
        
        data.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": quantity,
            "amount": amount,
            "shipping_cost": shipping_cost,
            "order_date": order_date_str,
            "status": status,
            "payment_method": random.choice(payment_methods),
            "discount_percent": random.choice([0, 5, 10, 15, 20, 25]),
            "notes": f"Order notes for {order_id}" if random.random() > 0.7 else None
        })
    
    return spark.createDataFrame(data)

# Generate orders
orders_df = generate_orders(50000)

# Save as JSON files (simulating source system)
(orders_df
    .repartition(20)  # Create multiple files
    .write
    .format("json")
    .mode("overwrite")
    .save("/tmp/week2_project/raw/orders/")
)

print(f"✓ Generated {orders_df.count():,} orders")
orders_df.show(10)

# ============================================================================
# Generate Customers Data
# ============================================================================

def generate_customers(num_customers=5000):
    """Generate customer dimension data"""
    
    data = []
    countries = ["USA", "Canada", "UK", "Germany", "France", "Spain", "Italy", "Australia"]
    segments = ["Premium", "Standard", "Basic", "VIP"]
    
    for i in range(1, num_customers + 1):
        segment = random.choice(segments)
        
        # Lifetime value based on segment
        if segment == "VIP":
            lifetime_value = round(random.uniform(10000, 100000), 2)
        elif segment == "Premium":
            lifetime_value = round(random.uniform(5000, 10000), 2)
        elif segment == "Standard":
            lifetime_value = round(random.uniform(1000, 5000), 2)
        else:
            lifetime_value = round(random.uniform(100, 1000), 2)
        
        # Email with some quality issues
        if random.random() > 0.05:
            email = f"customer{i}@example.com"
        else:
            email = f"invalid_email_{i}"  # 5% invalid
        
        data.append({
            "customer_id": i,
            "customer_name": f"Customer {i}",
            "email": email,
            "phone": f"({random.randint(200, 999)}) {random.randint(200, 999)}-{random.randint(1000, 9999)}",
            "segment": segment,
            "lifetime_value": lifetime_value,
            "registration_date": (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1460))).strftime("%Y-%m-%d"),
            "country": random.choice(countries),
            "is_active": random.choice([True, False]),
            "credit_limit": round(random.uniform(1000, 50000), 2)
        })
    
    return spark.createDataFrame(data)

# Generate customers
customers_df = generate_customers(5000)

# Save as CSV
(customers_df.write
    .format("csv")
    .option("header", "true")
    .mode("overwrite")
    .save("/tmp/week2_project/raw/customers/")
)

print(f"✓ Generated {customers_df.count():,} customers")
customers_df.show(10)

# ============================================================================
# Generate Products Data
# ============================================================================

def generate_products(num_products=500):
    """Generate product dimension data"""
    
    categories = [
        "Electronics", "Clothing", "Home & Garden", "Sports & Outdoors",
        "Books", "Toys & Games", "Health & Beauty", "Automotive",
        "Food & Beverage", "Office Supplies"
    ]
    
    data = []
    
    for i in range(1, num_products + 1):
        category = random.choice(categories)
        
        # Price based on category
        if category == "Electronics":
            price = round(random.uniform(100, 3000), 2)
        elif category == "Clothing":
            price = round(random.uniform(20, 300), 2)
        else:
            price = round(random.uniform(10, 500), 2)
        
        # Cost (60-80% of price)
        cost = round(price * random.uniform(0.6, 0.8), 2)
        
        data.append({
            "product_id": f"PROD{i:04d}",
            "product_name": f"{category} Product {i}",
            "category": category,
            "price": price,
            "cost": cost,
            "in_stock": random.choice([True, False]),
            "stock_quantity": random.randint(0, 1000),
            "supplier_id": f"SUP{random.randint(1, 50):03d}",
            "weight_kg": round(random.uniform(0.1, 50), 2),
            "dimensions": f"{random.randint(10, 100)}x{random.randint(10, 100)}x{random.randint(10, 100)}"
        })
    
    return spark.createDataFrame(data)

# Generate products
products_df = generate_products(500)

# Save as Parquet
(products_df.write
    .format("parquet")
    .mode("overwrite")
    .save("/tmp/week2_project/raw/products/")
)

print(f"✓ Generated {products_df.count():,} products")
products_df.show(10)

# ============================================================================
# Generate Order Items (nested structure)
# ============================================================================

def generate_order_items(num_orders=10000):
    """Generate orders with nested items array"""
    
    data = []
    
    for i in range(num_orders):
        order_id = f"ORD{i:06d}"
        customer_id = random.randint(1, 5000)
        order_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))
        
        # Generate 1-5 items per order
        num_items = random.randint(1, 5)
        items = []
        
        for j in range(num_items):
            items.append({
                "product_id": f"PROD{random.randint(1, 500):04d}",
                "quantity": random.randint(1, 10),
                "unit_price": round(random.uniform(10, 500), 2)
            })
        
        data.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "order_date": order_date.strftime("%Y-%m-%d"),
            "items": items,
            "total_amount": sum(item["quantity"] * item["unit_price"] for item in items)
        })
    
    return spark.createDataFrame(data)

# Generate nested orders
nested_orders_df = generate_order_items(10000)

# Save as JSON
(nested_orders_df.write
    .format("json")
    .mode("overwrite")
    .save("/tmp/week2_project/raw/nested_orders/")
)

print(f"✓ Generated {nested_orders_df.count():,} nested orders")
nested_orders_df.show(5, truncate=False)
```

### Step 3: Create Monitoring Tables

```python
# Create pipeline metrics table
spark.sql("""
    CREATE TABLE IF NOT EXISTS week2_monitoring.pipeline_metrics (
        pipeline_name STRING,
        layer STRING,
        status STRING,
        records_processed BIGINT,
        duration_seconds DOUBLE,
        error_message STRING,
        execution_timestamp TIMESTAMP
    )
    USING DELTA
    COMMENT 'Pipeline execution metrics'
""")

# Create data quality metrics table
spark.sql("""
    CREATE TABLE IF NOT EXISTS week2_monitoring.quality_metrics (
        table_name STRING,
        check_name STRING,
        check_result STRING,
        records_checked BIGINT,
        records_passed BIGINT,
        records_failed BIGINT,
        pass_rate DOUBLE,
        check_timestamp TIMESTAMP
    )
    USING DELTA
    COMMENT 'Data quality check results'
""")

# Create dead letter queue table
spark.sql("""
    CREATE TABLE IF NOT EXISTS week2_monitoring.dead_letter_queue (
        source_table STRING,
        error_type STRING,
        error_message STRING,
        failed_record STRING,
        error_timestamp TIMESTAMP
    )
    USING DELTA
    COMMENT 'Failed records for investigation'
""")

print("✓ Monitoring tables created")
```

### Step 4: Create Checkpoint Directories

```python
# Create checkpoint directories
checkpoint_dirs = [
    "/tmp/week2_project/checkpoints/bronze_orders",
    "/tmp/week2_project/checkpoints/silver_orders",
    "/tmp/week2_project/checkpoints/gold_metrics",
    "/tmp/week2_project/checkpoints/schema_location"
]

for dir_path in checkpoint_dirs:
    dbutils.fs.mkdirs(dir_path)

print("✓ Checkpoint directories created")
```

### Step 5: Create Helper Functions

```python
# ============================================================================
# Helper Functions Library
# ============================================================================

from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Week2Helpers:
    """Helper functions for Week 2 project"""
    
    @staticmethod
    def log_pipeline_execution(pipeline_name, layer, status, records, duration, error=None):
        """Log pipeline execution metrics"""
        metrics_df = spark.createDataFrame([{
            "pipeline_name": pipeline_name,
            "layer": layer,
            "status": status,
            "records_processed": records,
            "duration_seconds": duration,
            "error_message": error,
            "execution_timestamp": datetime.now()
        }])
        
        (metrics_df.write
            .format("delta")
            .mode("append")
            .saveAsTable("week2_monitoring.pipeline_metrics")
        )
    
    @staticmethod
    def log_quality_check(table_name, check_name, total, passed, failed):
        """Log data quality check results"""
        pass_rate = passed / total if total > 0 else 0
        
        metrics_df = spark.createDataFrame([{
            "table_name": table_name,
            "check_name": check_name,
            "check_result": "PASS" if pass_rate >= 0.95 else "FAIL",
            "records_checked": total,
            "records_passed": passed,
            "records_failed": failed,
            "pass_rate": pass_rate,
            "check_timestamp": datetime.now()
        }])
        
        (metrics_df.write
            .format("delta")
            .mode("append")
            .saveAsTable("week2_monitoring.quality_metrics")
        )
    
    @staticmethod
    def write_to_dlq(failed_df, source_table, error_type, error_message):
        """Write failed records to dead letter queue"""
        dlq_df = failed_df.withColumn(
            "source_table", lit(source_table)
        ).withColumn(
            "error_type", lit(error_type)
        ).withColumn(
            "error_message", lit(error_message)
        ).withColumn(
            "failed_record", to_json(struct("*"))
        ).withColumn(
            "error_timestamp", current_timestamp()
        ).select(
            "source_table", "error_type", "error_message", 
            "failed_record", "error_timestamp"
        )
        
        (dlq_df.write
            .format("delta")
            .mode("append")
            .saveAsTable("week2_monitoring.dead_letter_queue")
        )

# Create instance
helpers = Week2Helpers()

print("✓ Helper functions loaded")
```

### Step 6: Verify Setup

```python
# Verify databases
print("=== Databases ===")
spark.sql("SHOW DATABASES LIKE 'week2%'").show()

# Verify raw data files
print("\n=== Raw Data Files ===")
print("Orders:", len(dbutils.fs.ls("/tmp/week2_project/raw/orders/")))
print("Customers:", len(dbutils.fs.ls("/tmp/week2_project/raw/customers/")))
print("Products:", len(dbutils.fs.ls("/tmp/week2_project/raw/products/")))
print("Nested Orders:", len(dbutils.fs.ls("/tmp/week2_project/raw/nested_orders/")))

# Sample data
print("\n=== Sample Orders ===")
spark.read.json("/tmp/week2_project/raw/orders/").show(5)

print("\n=== Sample Customers ===")
spark.read.csv("/tmp/week2_project/raw/customers/", header=True).show(5)

print("\n=== Sample Products ===")
spark.read.parquet("/tmp/week2_project/raw/products/").show(5)

# Verify monitoring tables
print("\n=== Monitoring Tables ===")
spark.sql("SHOW TABLES IN week2_monitoring").show()

print("\n✅ Setup complete! Ready for Week 2 project.")
```

## Data Summary

### Orders Dataset
- **Location**: `/tmp/week2_project/raw/orders/`
- **Format**: JSON
- **Records**: 50,000
- **Quality Issues**:
  - 1% null order_ids
  - 2% null customer_ids
  - 3% negative amounts
  - Inconsistent date formats (3 different formats)
  - Mixed case status values
  - Some missing notes

### Customers Dataset
- **Location**: `/tmp/week2_project/raw/customers/`
- **Format**: CSV
- **Records**: 5,000
- **Segments**: Premium, Standard, Basic, VIP
- **Quality Issues**:
  - 5% invalid email formats
  - Some inactive customers

### Products Dataset
- **Location**: `/tmp/week2_project/raw/products/`
- **Format**: Parquet
- **Records**: 500
- **Categories**: 10 different categories
- **Attributes**: price, cost, stock, supplier info

### Nested Orders Dataset
- **Location**: `/tmp/week2_project/raw/nested_orders/`
- **Format**: JSON with nested arrays
- **Records**: 10,000
- **Structure**: Each order contains 1-5 items

## Project Architecture

```
Raw Data (Multiple Formats)
    ↓
Bronze Layer (week2_bronze)
    - Raw data with metadata
    - Append-only
    ↓
Silver Layer (week2_silver)
    - Cleaned and validated
    - Standardized formats
    - Joined with dimensions
    ↓
Gold Layer (week2_gold)
    - Business metrics
    - Aggregations
    - Optimized for reporting
    ↓
Monitoring (week2_monitoring)
    - Pipeline metrics
    - Quality checks
    - Dead letter queue
```

## Troubleshooting

### Issue: Cannot create database
**Solution**: Ensure you have CREATE DATABASE permissions

### Issue: File not found errors
**Solution**: Verify data generation completed successfully:
```python
dbutils.fs.ls("/tmp/week2_project/raw/orders/")
```

### Issue: Out of memory
**Solution**: Reduce data size:
```python
orders_df = generate_orders(10000)  # Instead of 50000
```

### Issue: Checkpoint errors
**Solution**: Clear checkpoints:
```python
dbutils.fs.rm("/tmp/week2_project/checkpoints/", True)
```

## Next Steps

After completing setup:
1. Proceed to `exercise.py` for the Week 2 integration project
2. Build Bronze → Silver → Gold pipeline
3. Implement data quality checks
4. Optimize pipeline performance
5. Complete the 50-question quiz

---

**Setup Time**: 15-20 minutes  
**Data Generated**: 50,000 orders, 5,000 customers, 500 products, 10,000 nested orders  
**Ready for**: Complete ELT pipeline development
