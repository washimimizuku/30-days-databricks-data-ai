# Day 21: Setup - Optimization Techniques

## Environment Setup

### 1. Create Database

```python
# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS day21_optimization")
spark.sql("USE day21_optimization")

print("✓ Database created")
```

### 2. Sample Data Creation

#### Dataset 1: Large Sales Table (Unoptimized)

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# Generate large dataset with small files
sales_data = []
base_date = datetime(2024, 1, 1)

for i in range(1, 10001):
    sale_date = base_date + timedelta(days=random.randint(0, 90))
    sales_data.append((
        i,
        random.randint(1, 1000),  # customer_id
        random.randint(1, 100),   # product_id
        random.randint(1, 10),    # quantity
        round(random.uniform(10, 1000), 2),  # amount
        sale_date.strftime("%Y-%m-%d"),
        random.choice(["US", "UK", "CA", "AU", "DE"])  # region
    ))

sales_schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("amount", DoubleType(), False),
    StructField("date", StringType(), False),
    StructField("region", StringType(), False)
])

sales_df = spark.createDataFrame(sales_data, sales_schema)

# Write with many small files (unoptimized)
(sales_df
    .repartition(100)  # Create many small files
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("sales_unoptimized")
)

print(f"✓ Created sales_unoptimized with {sales_df.count()} records in many small files")
```

#### Dataset 2: Sales Table for Partitioning

```python
# Same data for partitioning exercises
(sales_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("sales_for_partitioning")
)

print("✓ Created sales_for_partitioning")
```

#### Dataset 3: Sales Table for Z-Ordering

```python
# Same data for Z-ordering exercises
(sales_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("sales_for_zorder")
)

print("✓ Created sales_for_zorder")
```

#### Dataset 4: Customer Dimension

```python
# Customer dimension for join optimization
customers_data = [(i, f"Customer_{i}", random.choice(["Gold", "Silver", "Bronze"])) 
                  for i in range(1, 1001)]

customers_df = spark.createDataFrame(customers_data, ["customer_id", "name", "tier"])

(customers_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("customers")
)

print(f"✓ Created customers with {customers_df.count()} records")
```

### 3. Helper Functions

```python
def show_table_stats(table_name):
    """Display table statistics"""
    print(f"\n{'='*70}")
    print(f"TABLE STATISTICS: {table_name}")
    print('='*70)
    
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").first()
    
    print(f"Location: {detail['location']}")
    print(f"Number of files: {detail['numFiles']}")
    print(f"Size in bytes: {detail['sizeInBytes']:,}")
    print(f"Size in MB: {detail['sizeInBytes'] / 1024 / 1024:.2f}")
    
    if detail['numFiles'] > 0:
        avg_file_size = detail['sizeInBytes'] / detail['numFiles']
        print(f"Average file size: {avg_file_size / 1024 / 1024:.2f} MB")
        
        if avg_file_size < 128 * 1024 * 1024:
            print("⚠ WARNING: Small file problem detected!")
    
    print(f"Partition columns: {detail['partitionColumns']}")
    print('='*70 + "\n")

def compare_query_performance(query, iterations=3):
    """Compare query performance"""
    import time
    
    times = []
    for i in range(iterations):
        start = time.time()
        result = spark.sql(query)
        result.count()  # Force execution
        duration = time.time() - start
        times.append(duration)
        print(f"  Run {i+1}: {duration:.3f}s")
    
    avg_time = sum(times) / len(times)
    print(f"  Average: {avg_time:.3f}s")
    return avg_time

def show_file_distribution(table_name):
    """Show file size distribution"""
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").first()
    
    print(f"\nFile Distribution for {table_name}:")
    print(f"  Total files: {detail['numFiles']}")
    print(f"  Total size: {detail['sizeInBytes'] / 1024 / 1024:.2f} MB")
    
    if detail['numFiles'] > 0:
        avg_size = detail['sizeInBytes'] / detail['numFiles'] / 1024 / 1024
        print(f"  Average file size: {avg_size:.2f} MB")
        
        if avg_size < 10:
            print("  Status: ⚠ Many small files")
        elif avg_size < 128:
            print("  Status: ⚠ Small files")
        elif avg_size <= 1024:
            print("  Status: ✓ Good file size")
        else:
            print("  Status: ⚠ Large files")

# Display initial statistics
show_table_stats("sales_unoptimized")
show_file_distribution("sales_unoptimized")
```

### 4. Quick Reference

```python
print("""
╔════════════════════════════════════════════════════════════════════╗
║       DAY 21: OPTIMIZATION TECHNIQUES - QUICK REFERENCE            ║
╠════════════════════════════════════════════════════════════════════╣
║ Tables Created:                                                    ║
║   • sales_unoptimized (10,000 records, many small files)           ║
║   • sales_for_partitioning (10,000 records)                        ║
║   • sales_for_zorder (10,000 records)                              ║
║   • customers (1,000 records)                                      ║
║                                                                    ║
║ Key Commands:                                                      ║
║   • OPTIMIZE table_name                                            ║
║   • OPTIMIZE table_name ZORDER BY (col1, col2)                     ║
║   • VACUUM table_name                                              ║
║   • VACUUM table_name RETAIN 168 HOURS                             ║
║   • DESCRIBE DETAIL table_name                                     ║
║                                                                    ║
║ Helper Functions:                                                  ║
║   • show_table_stats(table_name)                                   ║
║   • compare_query_performance(query)                               ║
║   • show_file_distribution(table_name)                             ║
║                                                                    ║
║ Optimization Goals:                                                ║
║   • File size: 128 MB - 1 GB                                       ║
║   • Partition count: < 10,000                                      ║
║   • Z-order columns: Max 4                                         ║
║   • VACUUM retention: Min 7 days                                   ║
╚════════════════════════════════════════════════════════════════════╝
""")
```

## Setup Complete! ✅

You now have:
- ✅ Database with unoptimized tables
- ✅ Large sales dataset (10,000 records)
- ✅ Tables with small file problem
- ✅ Customer dimension for joins
- ✅ Helper functions for analysis
- ✅ Ready for optimization exercises!

**Next Step**: Open `exercise.py` and start optimizing!
