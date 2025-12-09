# Day 18: Setup - Delta Lake MERGE (UPSERT)

## Environment Setup

### 1. Create Database

```python
# Create database for today's exercises
spark.sql("CREATE DATABASE IF NOT EXISTS day18_merge")
spark.sql("USE day18_merge")

# Set checkpoint location
checkpoint_base = "/tmp/checkpoints/day18"
dbutils.fs.mkdirs(checkpoint_base)
```

### 2. Sample Data Creation

#### Dataset 1: Customer Data

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# Initial customer data
customers_data = [
    (1, "John Doe", "john@email.com", "123 Main St", "555-0101", "2024-01-01"),
    (2, "Jane Smith", "jane@email.com", "456 Oak Ave", "555-0102", "2024-01-02"),
    (3, "Bob Johnson", "bob@email.com", "789 Pine Rd", "555-0103", "2024-01-03"),
    (4, "Alice Brown", "alice@email.com", "321 Elm St", "555-0104", "2024-01-04"),
    (5, "Charlie Wilson", "charlie@email.com", "654 Maple Dr", "555-0105", "2024-01-05"),
]

customers_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("created_at", StringType(), True)
])

customers_df = spark.createDataFrame(customers_data, customers_schema)

# Create Delta table
(customers_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("customers")
)

print("✓ Created customers table with 5 records")
```

#### Dataset 2: Customer Updates

```python
# Customer updates (mix of updates and new inserts)
customer_updates_data = [
    (2, "Jane Smith-Jones", "jane.new@email.com", "456 Oak Ave", "555-0102", "2024-01-10"),  # Updated
    (3, "Bob Johnson", "bob.new@email.com", "999 New St", "555-0199", "2024-01-10"),  # Updated
    (6, "David Lee", "david@email.com", "111 First St", "555-0106", "2024-01-10"),  # New
    (7, "Emma Davis", "emma@email.com", "222 Second St", "555-0107", "2024-01-10"),  # New
]

customer_updates_df = spark.createDataFrame(customer_updates_data, customers_schema)

# Create staging table
(customer_updates_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("customer_updates")
)

print("✓ Created customer_updates table with 4 records (2 updates, 2 new)")
```

#### Dataset 3: Product Inventory

```python
# Product inventory
inventory_data = [
    (101, "Laptop", 50, 999.99, "2024-01-01"),
    (102, "Mouse", 200, 29.99, "2024-01-01"),
    (103, "Keyboard", 150, 79.99, "2024-01-01"),
    (104, "Monitor", 75, 299.99, "2024-01-01"),
    (105, "Headphones", 100, 149.99, "2024-01-01"),
]

inventory_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("last_updated", StringType(), True)
])

inventory_df = spark.createDataFrame(inventory_data, inventory_schema)

(inventory_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("inventory")
)

print("✓ Created inventory table with 5 products")
```

#### Dataset 4: Inventory Updates

```python
# Inventory updates (quantity changes, price changes, deletions)
inventory_updates_data = [
    (101, "Laptop", 45, 999.99, "2024-01-10"),  # Quantity decreased
    (102, "Mouse", 0, 29.99, "2024-01-10"),  # Out of stock (should delete)
    (103, "Keyboard", 175, 69.99, "2024-01-10"),  # Quantity increased, price decreased
    (106, "Webcam", 80, 89.99, "2024-01-10"),  # New product
]

inventory_updates_df = spark.createDataFrame(inventory_updates_data, inventory_schema)

(inventory_updates_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("inventory_updates")
)

print("✓ Created inventory_updates table")
```

#### Dataset 5: Sales Transactions

```python
# Sales transactions (for incremental processing)
sales_data = []
base_date = datetime(2024, 1, 1)

for i in range(1, 101):
    sale_date = base_date + timedelta(days=random.randint(0, 9))
    sales_data.append((
        i,
        random.randint(1, 5),  # customer_id
        random.randint(101, 105),  # product_id
        random.randint(1, 5),  # quantity
        round(random.uniform(50, 1000), 2),  # amount
        sale_date.strftime("%Y-%m-%d"),
        "COMPLETED"
    ))

sales_schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("sale_date", StringType(), True),
    StructField("status", StringType(), True)
])

sales_df = spark.createDataFrame(sales_data, sales_schema)

(sales_df
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("sale_date")
    .saveAsTable("sales")
)

print("✓ Created sales table with 100 transactions")
```

#### Dataset 6: Sales Updates

```python
# Sales updates (status changes, corrections)
sales_updates_data = [
    (5, 1, 101, 2, 1999.98, "2024-01-01", "CANCELLED"),  # Cancelled
    (10, 2, 102, 3, 89.97, "2024-01-02", "REFUNDED"),  # Refunded
    (15, 3, 103, 1, 79.99, "2024-01-03", "COMPLETED"),  # Amount corrected
    (101, 4, 104, 1, 299.99, "2024-01-10", "COMPLETED"),  # New sale
    (102, 5, 105, 2, 299.98, "2024-01-10", "COMPLETED"),  # New sale
]

sales_updates_df = spark.createDataFrame(sales_updates_data, sales_schema)

(sales_updates_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("sales_updates")
)

print("✓ Created sales_updates table")
```

#### Dataset 7: User Activity (with duplicates)

```python
# User activity with duplicates (needs deduplication)
activity_data = []
base_time = datetime(2024, 1, 1, 10, 0, 0)

for i in range(1, 51):
    user_id = random.randint(1, 10)
    event_time = base_time + timedelta(minutes=random.randint(0, 120))
    activity_data.append((
        i,
        user_id,
        random.choice(["login", "view", "click", "purchase"]),
        event_time
    ))

# Add some duplicates
for i in range(5):
    activity_data.append(activity_data[i])

activity_schema = StructType([
    StructField("event_id", IntegerType(), False),
    StructField("user_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", TimestampType(), True)
])

activity_df = spark.createDataFrame(activity_data, activity_schema)

(activity_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("user_activity_raw")
)

print(f"✓ Created user_activity_raw table with {activity_df.count()} records (includes duplicates)")
```

#### Dataset 8: User Activity Target (for deduplication)

```python
# Create empty target table for deduplicated activity
(spark.createDataFrame([], activity_schema)
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("user_activity")
)

print("✓ Created empty user_activity table")
```

#### Dataset 9: Orders with Versions

```python
# Orders with version numbers (for conditional updates)
orders_data = [
    (1001, "ORD-001", 100.00, 1, "2024-01-01 10:00:00"),
    (1002, "ORD-002", 200.00, 1, "2024-01-01 11:00:00"),
    (1003, "ORD-003", 150.00, 1, "2024-01-01 12:00:00"),
]

orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("order_number", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("version", IntegerType(), True),
    StructField("updated_at", StringType(), True)
])

orders_df = spark.createDataFrame(orders_data, orders_schema)

(orders_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("orders")
)

print("✓ Created orders table with version tracking")
```

#### Dataset 10: Order Updates with Versions

```python
# Order updates with different versions
order_updates_data = [
    (1001, "ORD-001", 120.00, 2, "2024-01-02 10:00:00"),  # Newer version
    (1002, "ORD-002", 180.00, 1, "2024-01-02 11:00:00"),  # Same version, different amount
    (1003, "ORD-003", 150.00, 0, "2024-01-02 12:00:00"),  # Older version (should not update)
    (1004, "ORD-004", 300.00, 1, "2024-01-02 13:00:00"),  # New order
]

order_updates_df = spark.createDataFrame(order_updates_data, orders_schema)

(order_updates_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("order_updates")
)

print("✓ Created order_updates table")
```

### 3. Helper Functions

```python
def show_table_counts():
    """Display record counts for all tables"""
    tables = [
        "customers", "customer_updates",
        "inventory", "inventory_updates",
        "sales", "sales_updates",
        "user_activity_raw", "user_activity",
        "orders", "order_updates"
    ]
    
    print("\n" + "="*50)
    print("TABLE RECORD COUNTS")
    print("="*50)
    for table in tables:
        try:
            count = spark.table(table).count()
            print(f"{table:25} {count:>10} rows")
        except:
            print(f"{table:25} {'NOT FOUND':>10}")
    print("="*50 + "\n")

def show_merge_history(table_name, limit=5):
    """Show MERGE operation history for a table"""
    print(f"\n{'='*60}")
    print(f"MERGE HISTORY: {table_name}")
    print('='*60)
    
    history = spark.sql(f"DESCRIBE HISTORY {table_name}").filter(col("operation") == "MERGE")
    
    if history.count() == 0:
        print("No MERGE operations found")
    else:
        history.select(
            "version",
            "timestamp",
            "operation",
            "operationMetrics"
        ).limit(limit).show(truncate=False)
    
    print('='*60 + "\n")

def compare_tables(table1, table2, key_col):
    """Compare two tables to see differences"""
    df1 = spark.table(table1)
    df2 = spark.table(table2)
    
    print(f"\n{'='*60}")
    print(f"COMPARING: {table1} vs {table2}")
    print('='*60)
    
    # Records only in table1
    only_in_1 = df1.join(df2, key_col, "left_anti")
    print(f"Only in {table1}: {only_in_1.count()} records")
    
    # Records only in table2
    only_in_2 = df2.join(df1, key_col, "left_anti")
    print(f"Only in {table2}: {only_in_2.count()} records")
    
    # Records in both
    in_both = df1.join(df2, key_col, "inner")
    print(f"In both tables: {in_both.count()} records")
    
    print('='*60 + "\n")

def reset_table(table_name, source_table):
    """Reset a table to its original state"""
    spark.sql(f"""
        CREATE OR REPLACE TABLE {table_name}
        AS SELECT * FROM {source_table}
    """)
    print(f"✓ Reset {table_name} from {source_table}")

# Display initial state
show_table_counts()
```

### 4. Verification Queries

```python
# Verify customers table
print("Sample customers:")
spark.table("customers").show(5)

# Verify customer updates
print("\nCustomer updates:")
spark.table("customer_updates").show()

# Verify inventory
print("\nInventory:")
spark.table("inventory").show()

# Verify sales
print("\nSample sales:")
spark.table("sales").show(5)

# Check for duplicates in user_activity_raw
print("\nDuplicate check in user_activity_raw:")
spark.sql("""
    SELECT event_id, COUNT(*) as count
    FROM user_activity_raw
    GROUP BY event_id
    HAVING COUNT(*) > 1
    ORDER BY count DESC
""").show()
```

### 5. Quick Reference

```python
# Print quick reference
print("""
╔════════════════════════════════════════════════════════════╗
║           DAY 18: DELTA LAKE MERGE - QUICK REFERENCE       ║
╠════════════════════════════════════════════════════════════╣
║ Tables Created:                                            ║
║   • customers (5 records)                                  ║
║   • customer_updates (4 records: 2 updates, 2 new)         ║
║   • inventory (5 products)                                 ║
║   • inventory_updates (4 records: includes deletions)      ║
║   • sales (100 transactions, partitioned by date)          ║
║   • sales_updates (5 records: status changes + new)        ║
║   • user_activity_raw (55 records with duplicates)         ║
║   • user_activity (empty, for deduplication)               ║
║   • orders (3 records with versions)                       ║
║   • order_updates (4 records with version conflicts)       ║
║                                                            ║
║ Helper Functions:                                          ║
║   • show_table_counts()                                    ║
║   • show_merge_history(table_name)                         ║
║   • compare_tables(table1, table2, key_col)                ║
║   • reset_table(table_name, source_table)                  ║
║                                                            ║
║ Key Concepts:                                              ║
║   • MERGE = UPSERT (Update + Insert)                       ║
║   • WHEN MATCHED / WHEN NOT MATCHED                        ║
║   • Conditional updates and deletes                        ║
║   • Deduplication before MERGE                             ║
║   • Version-based conflict resolution                      ║
╚════════════════════════════════════════════════════════════╝
""")
```

## Setup Complete! ✅

You now have:
- ✅ 10 Delta tables with various MERGE scenarios
- ✅ Customer data (updates and new records)
- ✅ Inventory data (with deletions)
- ✅ Sales data (partitioned, with status changes)
- ✅ User activity (with duplicates to handle)
- ✅ Orders (with version conflicts)
- ✅ Helper functions for analysis
- ✅ Ready for MERGE exercises!

**Next Step**: Open `exercise.py` and start practicing MERGE operations!
