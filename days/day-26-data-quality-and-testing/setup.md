# Day 26: Data Quality & Testing - Setup

## Prerequisites

Before starting today's exercises, ensure you have:
- ✅ Completed Days 1-25
- ✅ Access to a Databricks workspace
- ✅ Understanding of Spark DataFrames and SQL
- ✅ Familiarity with Python basics

## Setup Instructions

### Step 1: Create Database

```sql
-- Create database for quality testing
CREATE DATABASE IF NOT EXISTS quality_testing;
USE quality_testing;
```

### Step 2: Create Sample Data with Quality Issues

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

# Create customers table with quality issues
customers_data = [
    (1, "John Doe", "john@email.com", 25, "CA", "2024-01-01"),
    (2, "Jane Smith", "JANE@EMAIL.COM", 30, "NY", "2024-01-02"),
    (3, "Bob Johnson", "bob.invalid", 150, "TX", "2024-01-03"),  # Invalid email, age
    (4, None, "alice@email.com", 28, "FL", "2024-01-04"),  # Null name
    (5, "Charlie Brown", None, 35, "CA", "2024-01-05"),  # Null email
    (6, "Diana Prince", "diana@email.com", -5, "NY", "2024-01-06"),  # Negative age
    (7, "Eve Wilson", "eve@email.com", 40, "XX", "2024-01-07"),  # Invalid state
    (8, "Frank Miller", "frank@email.com", 45, "CA", "2024-01-08"),
    (9, "Grace Lee", "grace@email.com", 32, "TX", "2024-01-09"),
    (10, "Henry Taylor", "henry@email.com", 38, "FL", "2024-01-10"),
    (10, "Henry Taylor", "henry@email.com", 38, "FL", "2024-01-10"),  # Duplicate
]

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("state", StringType(), True),
    StructField("created_date", StringType(), True)
])

customers_df = spark.createDataFrame(customers_data, customers_schema)
customers_df.write.mode("overwrite").saveAsTable("quality_testing.customers_raw")

print("✅ Created customers_raw table with quality issues")
```

### Step 3: Create Orders Table

```python
# Create orders table with quality issues
orders_data = [
    (101, 1, "2024-01-15", 100.00, "completed"),
    (102, 2, "2024-01-16", -50.00, "completed"),  # Negative amount
    (103, 3, "2024-01-17", 200.00, "pending"),
    (104, 999, "2024-01-18", 150.00, "completed"),  # Non-existent customer
    (105, 5, None, 300.00, "completed"),  # Null date
    (106, 6, "2024-01-20", 0.00, "completed"),  # Zero amount
    (107, 7, "2024-01-21", 175.50, "invalid_status"),  # Invalid status
    (108, 8, "2024-01-22", 225.00, "completed"),
    (109, 9, "2024-01-23", 125.75, "cancelled"),
    (110, 10, "2024-01-24", 400.00, "completed"),
]

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True)
])

orders_df = spark.createDataFrame(orders_data, orders_schema)
orders_df.write.mode("overwrite").saveAsTable("quality_testing.orders_raw")

print("✅ Created orders_raw table with quality issues")
```

### Step 4: Create Products Table

```python
# Create products table
products_data = [
    (1001, "Laptop", "Electronics", 999.99, 50),
    (1002, "Mouse", "Electronics", 29.99, 200),
    (1003, "Keyboard", "Electronics", None, 150),  # Null price
    (1004, "Monitor", "Electronics", 399.99, -10),  # Negative stock
    (1005, "Desk", "Furniture", 599.99, 30),
    (1006, "", "Furniture", 149.99, 75),  # Empty name
    (1007, "Chair", "Furniture", 249.99, 100),
    (1008, "Lamp", "Furniture", 79.99, 0),  # Zero stock
    (1009, "Notebook", "Stationery", 12.99, 500),
    (1010, "Pen", "Stationery", 2.99, 1000),
]

products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("stock_quantity", IntegerType(), True)
])

products_df = spark.createDataFrame(products_data, products_schema)
products_df.write.mode("overwrite").saveAsTable("quality_testing.products_raw")

print("✅ Created products_raw table with quality issues")
```

### Step 5: Create Quality Metrics Table

```python
# Create table to store quality metrics
quality_metrics_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("column_name", StringType(), False),
    StructField("metric_name", StringType(), False),
    StructField("metric_value", DoubleType(), False),
    StructField("row_count", LongType(), False),
    StructField("measured_at", TimestampType(), False)
])

spark.createDataFrame([], quality_metrics_schema).write.mode("overwrite").saveAsTable("quality_testing.quality_metrics")

print("✅ Created quality_metrics table")
```

### Step 6: Create Quarantine Table

```python
# Create quarantine table for bad data
spark.sql("""
    CREATE TABLE IF NOT EXISTS quality_testing.quarantine_customers (
        customer_id INT,
        name STRING,
        email STRING,
        age INT,
        state STRING,
        created_date STRING,
        quarantine_reason STRING,
        quarantined_at TIMESTAMP
    )
""")

print("✅ Created quarantine_customers table")
```

### Step 7: Create Clean Tables (Empty)

```python
# Create clean tables for validated data
spark.sql("""
    CREATE TABLE IF NOT EXISTS quality_testing.customers_clean (
        customer_id INT NOT NULL,
        name STRING NOT NULL,
        email STRING NOT NULL,
        age INT,
        state STRING,
        created_date DATE
    )
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS quality_testing.orders_clean (
        order_id INT NOT NULL,
        customer_id INT NOT NULL,
        order_date DATE NOT NULL,
        amount DOUBLE,
        status STRING
    )
""")

print("✅ Created clean tables")
```

### Step 8: Add Delta Constraints (Optional)

```sql
-- Add constraints to clean tables
ALTER TABLE quality_testing.customers_clean
ADD CONSTRAINT valid_age CHECK (age >= 0 AND age <= 120);

ALTER TABLE quality_testing.customers_clean
ADD CONSTRAINT valid_email CHECK (
    email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
);

ALTER TABLE quality_testing.orders_clean
ADD CONSTRAINT valid_amount CHECK (amount > 0);

ALTER TABLE quality_testing.orders_clean
ADD CONSTRAINT valid_status CHECK (
    status IN ('pending', 'completed', 'cancelled')
);
```

### Step 9: Create Helper Functions

```python
# Data Quality Checker Class
class DataQualityChecker:
    def __init__(self, df):
        self.df = df
        self.checks = []
        
    def check_not_null(self, column):
        """Check column has no nulls"""
        null_count = self.df.filter(col(column).isNull()).count()
        self.checks.append({
            "check": f"{column}_not_null",
            "passed": null_count == 0,
            "details": f"Found {null_count} nulls"
        })
        return self
    
    def check_unique(self, column):
        """Check column has unique values"""
        total = self.df.count()
        unique = self.df.select(column).distinct().count()
        self.checks.append({
            "check": f"{column}_unique",
            "passed": total == unique,
            "details": f"Found {total - unique} duplicates"
        })
        return self
    
    def check_range(self, column, min_val, max_val):
        """Check column values in range"""
        out_of_range = self.df.filter(
            (col(column) < min_val) | (col(column) > max_val)
        ).count()
        self.checks.append({
            "check": f"{column}_range_{min_val}_to_{max_val}",
            "passed": out_of_range == 0,
            "details": f"Found {out_of_range} out of range"
        })
        return self
    
    def check_format(self, column, pattern):
        """Check column matches regex pattern"""
        invalid = self.df.filter(~col(column).rlike(pattern)).count()
        self.checks.append({
            "check": f"{column}_format",
            "passed": invalid == 0,
            "details": f"Found {invalid} invalid formats"
        })
        return self
    
    def get_results(self):
        """Return check results"""
        return self.checks
    
    def assert_all_passed(self):
        """Raise error if any check failed"""
        failed = [c for c in self.checks if not c["passed"]]
        if failed:
            raise ValueError(f"Quality checks failed: {failed}")

print("✅ Created DataQualityChecker class")
```

## Verification

Run these queries to verify your setup:

```sql
-- Check tables exist
SHOW TABLES IN quality_testing;

-- Check data counts
SELECT 'customers_raw' as table_name, COUNT(*) as row_count 
FROM quality_testing.customers_raw
UNION ALL
SELECT 'orders_raw', COUNT(*) 
FROM quality_testing.orders_raw
UNION ALL
SELECT 'products_raw', COUNT(*) 
FROM quality_testing.products_raw;

-- Preview data with issues
SELECT * FROM quality_testing.customers_raw WHERE age < 0 OR age > 120;
SELECT * FROM quality_testing.orders_raw WHERE amount <= 0;
SELECT * FROM quality_testing.products_raw WHERE price IS NULL;
```

Expected output:
- ✅ 3 raw tables created
- ✅ customers_raw: 11 rows (with duplicates)
- ✅ orders_raw: 10 rows
- ✅ products_raw: 10 rows
- ✅ Quality issues present in data
- ✅ Clean tables created (empty)
- ✅ Quarantine table created (empty)
- ✅ Quality metrics table created (empty)

## Troubleshooting

### Issue: Tables already exist

**Solution**: Use `DROP TABLE IF EXISTS` before creating:
```sql
DROP TABLE IF EXISTS quality_testing.customers_raw;
```

### Issue: Constraint violations

**Solution**: Constraints only apply to new data. Existing data is not validated.

### Issue: Cannot add constraints

**Solution**: Ensure table is Delta format and has no existing violations.

## Clean Up (Optional)

To remove all objects created today:

```sql
DROP DATABASE IF EXISTS quality_testing CASCADE;
```

## Ready to Start!

Once you've completed the setup:
1. ✅ Database created
2. ✅ Sample data with quality issues loaded
3. ✅ Clean and quarantine tables created
4. ✅ Helper functions defined
5. ✅ Verification queries successful

You're ready to begin the exercises in `exercise.py`!

---

**Setup Time**: 10-15 minutes  
**Total Tables Created**: 7 tables  
**Total Rows**: ~30 rows with intentional quality issues
