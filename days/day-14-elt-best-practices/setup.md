# Day 14: ELT Best Practices - Setup Guide

## Overview

Today we'll implement a complete Medallion Architecture pipeline with Bronze, Silver, and Gold layers. We'll create sample data representing an e-commerce system and build production-ready ELT workflows.

## Prerequisites

- Databricks workspace access
- Cluster with DBR 13.0+ (includes Delta Lake)
- Python 3.8+
- Permissions to create databases and tables

## Setup Steps

### Step 1: Create Database Structure

```sql
-- Create databases for each layer
CREATE DATABASE IF NOT EXISTS bronze
COMMENT 'Raw data layer - minimal transformations';

CREATE DATABASE IF NOT EXISTS silver
COMMENT 'Cleaned and validated data layer';

CREATE DATABASE IF NOT EXISTS gold
COMMENT 'Business-level aggregations and metrics';

CREATE DATABASE IF NOT EXISTS monitoring
COMMENT 'Pipeline monitoring and metrics';

-- Verify databases
SHOW DATABASES;
```

### Step 2: Create Monitoring Tables

```python
# Create pipeline metrics table
spark.sql("""
    CREATE TABLE IF NOT EXISTS monitoring.pipeline_metrics (
        pipeline_name STRING,
        status STRING,
        duration_seconds DOUBLE,
        details STRING,
        execution_timestamp TIMESTAMP
    )
    USING DELTA
    COMMENT 'Pipeline execution metrics and logs'
""")

# Create data quality metrics table
spark.sql("""
    CREATE TABLE IF NOT EXISTS monitoring.data_quality_metrics (
        table_name STRING,
        check_name STRING,
        check_result STRING,
        metric_value DOUBLE,
        check_timestamp TIMESTAMP
    )
    USING DELTA
    COMMENT 'Data quality check results'
""")

# Create dead letter queue table
spark.sql("""
    CREATE TABLE IF NOT EXISTS monitoring.dead_letter_queue (
        source_table STRING,
        error_message STRING,
        error_timestamp TIMESTAMP,
        failed_record STRING
    )
    USING DELTA
    COMMENT 'Failed records for investigation'
""")
```

### Step 3: Generate Sample Raw Data

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# Set seed for reproducibility
random.seed(42)

# Generate raw orders data (simulating JSON files from source system)
def generate_raw_orders(num_records=10000):
    """Generate raw orders data with quality issues"""
    
    data = []
    start_date = datetime(2024, 1, 1)
    
    for i in range(num_records):
        # Introduce data quality issues
        order_id = f"ORD{i:06d}" if random.random() > 0.01 else None  # 1% nulls
        customer_id = random.randint(1, 1000) if random.random() > 0.02 else None  # 2% nulls
        
        # Some invalid amounts
        if random.random() > 0.03:
            amount = round(random.uniform(10, 1000), 2)
        else:
            amount = round(random.uniform(-50, 0), 2)  # 3% invalid
        
        # Various date formats (inconsistent)
        order_date = start_date + timedelta(days=random.randint(0, 365))
        if random.random() > 0.5:
            order_date_str = order_date.strftime("%Y-%m-%d %H:%M:%S")
        else:
            order_date_str = order_date.strftime("%m/%d/%Y")
        
        # Email with quality issues
        if random.random() > 0.05:
            email = f"customer{customer_id}@example.com"
        else:
            email = f"invalid_email_{customer_id}"  # 5% invalid
        
        # Product info
        product_id = f"PROD{random.randint(1, 100):03d}"
        quantity = random.randint(1, 10)
        
        # Status with typos
        statuses = ["pending", "completed", "cancelled", "Pending", "COMPLETED", "Cancelled"]
        status = random.choice(statuses)
        
        data.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": quantity,
            "amount": amount,
            "order_date": order_date_str,
            "email": email,
            "status": status,
            "payment_method": random.choice(["credit_card", "debit_card", "paypal", "bank_transfer"]),
            "shipping_address": f"{random.randint(1, 9999)} Main St, City, State {random.randint(10000, 99999)}"
        })
    
    # Create DataFrame
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("order_date", StringType(), True),
        StructField("email", StringType(), True),
        StructField("status", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("shipping_address", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)

# Generate and save raw data
raw_orders_df = generate_raw_orders(10000)

# Save as JSON files (simulating source system)
(raw_orders_df
    .repartition(10)  # Create multiple files
    .write
    .format("json")
    .mode("overwrite")
    .save("/tmp/raw_data/orders/")
)

print(f"Generated {raw_orders_df.count()} raw order records")
raw_orders_df.show(10)
```

### Step 4: Generate Customer Dimension Data

```python
def generate_customers(num_customers=1000):
    """Generate customer dimension data"""
    
    data = []
    
    for i in range(1, num_customers + 1):
        # Customer segments
        segment = random.choice(["Premium", "Standard", "Basic"])
        
        # Lifetime value based on segment
        if segment == "Premium":
            lifetime_value = round(random.uniform(5000, 50000), 2)
        elif segment == "Standard":
            lifetime_value = round(random.uniform(1000, 5000), 2)
        else:
            lifetime_value = round(random.uniform(100, 1000), 2)
        
        data.append({
            "customer_id": i,
            "customer_name": f"Customer {i}",
            "email": f"customer{i}@example.com",
            "phone": f"({random.randint(200, 999)}) {random.randint(200, 999)}-{random.randint(1000, 9999)}",
            "segment": segment,
            "lifetime_value": lifetime_value,
            "registration_date": (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1460))).strftime("%Y-%m-%d"),
            "country": random.choice(["USA", "Canada", "UK", "Germany", "France"]),
            "is_active": random.choice([True, False])
        })
    
    return spark.createDataFrame(data)

# Generate customers
customers_df = generate_customers(1000)

# Save to Silver layer (dimension table)
(customers_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver.customers")
)

print(f"Generated {customers_df.count()} customer records")
customers_df.show(10)
```

### Step 5: Generate Product Dimension Data

```python
def generate_products(num_products=100):
    """Generate product dimension data"""
    
    categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books"]
    
    data = []
    
    for i in range(1, num_products + 1):
        category = random.choice(categories)
        
        # Price based on category
        if category == "Electronics":
            price = round(random.uniform(100, 2000), 2)
        elif category == "Clothing":
            price = round(random.uniform(20, 200), 2)
        else:
            price = round(random.uniform(10, 500), 2)
        
        data.append({
            "product_id": f"PROD{i:03d}",
            "product_name": f"{category} Product {i}",
            "category": category,
            "price": price,
            "cost": round(price * 0.6, 2),  # 40% margin
            "in_stock": random.choice([True, False]),
            "stock_quantity": random.randint(0, 1000)
        })
    
    return spark.createDataFrame(data)

# Generate products
products_df = generate_products(100)

# Save to Silver layer (dimension table)
(products_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver.products")
)

print(f"Generated {products_df.count()} product records")
products_df.show(10)
```

### Step 6: Create Checkpoint Directories

```python
# Create checkpoint directories for streaming
dbutils.fs.mkdirs("/tmp/checkpoints/bronze_orders")
dbutils.fs.mkdirs("/tmp/checkpoints/silver_orders")
dbutils.fs.mkdirs("/tmp/checkpoints/gold_daily_summary")
dbutils.fs.mkdirs("/tmp/checkpoints/schema_location")

print("Checkpoint directories created")
```

### Step 7: Create Helper Functions Library

```python
# Create reusable transformation functions
class DataTransformations:
    """Reusable data transformation functions"""
    
    @staticmethod
    def standardize_email(df, col_name):
        """Standardize email addresses"""
        return df.withColumn(
            col_name,
            lower(trim(col(col_name)))
        )
    
    @staticmethod
    def standardize_status(df, col_name):
        """Standardize status values"""
        return df.withColumn(
            col_name,
            lower(trim(col(col_name)))
        )
    
    @staticmethod
    def parse_flexible_date(df, col_name):
        """Parse dates in multiple formats"""
        return df.withColumn(
            col_name,
            coalesce(
                to_timestamp(col(col_name), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(col(col_name), "MM/dd/yyyy"),
                to_timestamp(col(col_name), "yyyy-MM-dd")
            )
        )
    
    @staticmethod
    def add_audit_columns(df):
        """Add standard audit columns"""
        return (df
            .withColumn("_created_at", current_timestamp())
            .withColumn("_created_by", current_user())
        )
    
    @staticmethod
    def validate_email(email_col):
        """Return expression to validate email format"""
        return col(email_col).rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

# Create instance for use in exercises
transforms = DataTransformations()

print("Helper functions loaded")
```

### Step 8: Create Data Quality Check Functions

```python
def run_quality_checks(df, table_name, checks):
    """
    Run data quality checks and log results
    
    Args:
        df: DataFrame to check
        table_name: Name of table being checked
        checks: List of (check_name, check_expression) tuples
    """
    from datetime import datetime
    
    results = []
    
    for check_name, check_expr in checks:
        # Count passing records
        passing = df.filter(check_expr).count()
        total = df.count()
        pass_rate = passing / total if total > 0 else 0
        
        result = {
            "table_name": table_name,
            "check_name": check_name,
            "check_result": "PASS" if pass_rate >= 0.95 else "FAIL",
            "metric_value": pass_rate,
            "check_timestamp": datetime.now()
        }
        
        results.append(result)
        
        # Log result
        print(f"{check_name}: {pass_rate:.2%} ({passing}/{total})")
    
    # Save results to monitoring table
    results_df = spark.createDataFrame(results)
    (results_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("monitoring.data_quality_metrics")
    )
    
    return results

print("Quality check functions loaded")
```

### Step 9: Verify Setup

```python
# Verify all databases exist
print("=== Databases ===")
spark.sql("SHOW DATABASES").show()

# Verify monitoring tables
print("\n=== Monitoring Tables ===")
spark.sql("SHOW TABLES IN monitoring").show()

# Verify dimension tables
print("\n=== Silver Dimension Tables ===")
spark.sql("SHOW TABLES IN silver").show()

# Check raw data
print("\n=== Raw Data Files ===")
display(dbutils.fs.ls("/tmp/raw_data/orders/"))

# Sample raw data
print("\n=== Sample Raw Data ===")
spark.read.json("/tmp/raw_data/orders/").show(5)

print("\n✅ Setup complete! Ready for exercises.")
```

## Data Summary

### Raw Orders Data
- **Location**: `/tmp/raw_data/orders/`
- **Format**: JSON
- **Records**: 10,000
- **Quality Issues**:
  - 1% null order_ids
  - 2% null customer_ids
  - 3% negative amounts
  - 5% invalid emails
  - Inconsistent date formats
  - Mixed case status values

### Customer Dimension
- **Table**: `silver.customers`
- **Records**: 1,000
- **Segments**: Premium, Standard, Basic
- **Attributes**: name, email, phone, segment, lifetime_value, registration_date

### Product Dimension
- **Table**: `silver.products`
- **Records**: 100
- **Categories**: Electronics, Clothing, Home & Garden, Sports, Books
- **Attributes**: name, category, price, cost, stock info

## Expected Pipeline Flow

```
Raw JSON Files
    ↓
Bronze Layer (bronze.orders)
    - Raw data with metadata
    - Append-only
    ↓
Silver Layer (silver.orders)
    - Cleaned and validated
    - Standardized formats
    - Joined with dimensions
    ↓
Gold Layer (gold.daily_customer_summary)
    - Business aggregations
    - Optimized for reporting
```

## Troubleshooting

### Issue: Cannot create database
**Solution**: Ensure you have CREATE DATABASE permissions

### Issue: Checkpoint directory errors
**Solution**: Clear checkpoints and restart:
```python
dbutils.fs.rm("/tmp/checkpoints/", True)
```

### Issue: Schema evolution errors
**Solution**: Enable schema evolution:
```python
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

### Issue: Out of memory
**Solution**: Reduce data size or increase cluster size:
```python
# Generate fewer records
raw_orders_df = generate_raw_orders(1000)  # Instead of 10000
```

## Next Steps

After completing setup:
1. Proceed to `exercise.py` for hands-on practice
2. Implement Bronze → Silver → Gold pipeline
3. Add data quality checks
4. Optimize pipeline performance
5. Complete the quiz to test your knowledge

---

**Setup Time**: 10-15 minutes  
**Data Generated**: ~10,000 orders, 1,000 customers, 100 products  
**Ready for**: Production-grade ELT pipeline development
