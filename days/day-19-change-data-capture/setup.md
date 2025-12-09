# Day 19: Setup - Change Data Capture (CDC)

## Environment Setup

### 1. Create Database

```python
# Create database for today's exercises
spark.sql("CREATE DATABASE IF NOT EXISTS day19_cdc")
spark.sql("USE day19_cdc")

# Set checkpoint location
checkpoint_base = "/tmp/checkpoints/day19"
dbutils.fs.mkdirs(checkpoint_base)
```

### 2. Sample Data Creation

#### Dataset 1: Initial Customer Data

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# Initial customer data (current state)
customers_data = [
    (1, "John Doe", "john@email.com", "123 Main St", 1, "2024-01-01 10:00:00"),
    (2, "Jane Smith", "jane@email.com", "456 Oak Ave", 1, "2024-01-01 10:00:00"),
    (3, "Bob Johnson", "bob@email.com", "789 Pine Rd", 1, "2024-01-01 10:00:00"),
    (4, "Alice Brown", "alice@email.com", "321 Elm St", 1, "2024-01-01 10:00:00"),
    (5, "Charlie Wilson", "charlie@email.com", "654 Maple Dr", 1, "2024-01-01 10:00:00"),
]

customers_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("sequence_number", IntegerType(), True),
    StructField("updated_at", StringType(), True)
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

#### Dataset 2: CDC Events

```python
# CDC events (INSERT, UPDATE, DELETE operations)
cdc_events_data = [
    # UPDATE events
    (2, "Jane Smith-Jones", "jane.new@email.com", "456 Oak Ave", "UPDATE", 2, "2024-01-02 11:00:00"),
    (3, "Bob Johnson", "bob.new@email.com", "999 New Address", "UPDATE", 2, "2024-01-02 11:30:00"),
    
    # INSERT events (new customers)
    (6, "David Lee", "david@email.com", "111 First St", "INSERT", 1, "2024-01-02 12:00:00"),
    (7, "Emma Davis", "emma@email.com", "222 Second St", "INSERT", 1, "2024-01-02 12:30:00"),
    
    # DELETE event
    (5, None, None, None, "DELETE", 3, "2024-01-02 13:00:00"),
    
    # More UPDATE events
    (1, "John Doe Jr", "john.jr@email.com", "123 Main St", "UPDATE", 2, "2024-01-02 14:00:00"),
]

cdc_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("operation", StringType(), False),
    StructField("sequence_number", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

cdc_events_df = spark.createDataFrame(cdc_events_data, cdc_schema)

(cdc_events_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("cdc_events")
)

print(f"✓ Created cdc_events table with {len(cdc_events_data)} events")
```

#### Dataset 3: SCD Type 2 Customer Dimension

```python
# SCD Type 2 table schema
scd_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("effective_date", TimestampType(), False),
    StructField("end_date", TimestampType(), True),
    StructField("is_current", BooleanType(), False)
])

# Initial SCD Type 2 data
scd_data = [
    (1, "John Doe", "john@email.com", "123 Main St", 
     datetime(2024, 1, 1, 10, 0, 0), None, True),
    (2, "Jane Smith", "jane@email.com", "456 Oak Ave", 
     datetime(2024, 1, 1, 10, 0, 0), None, True),
    (3, "Bob Johnson", "bob@email.com", "789 Pine Rd", 
     datetime(2024, 1, 1, 10, 0, 0), None, True),
]

scd_df = spark.createDataFrame(scd_data, scd_schema)

(scd_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("customers_scd2")
)

print("✓ Created customers_scd2 table with 3 records")
```

#### Dataset 4: SCD Type 2 Updates

```python
# Updates for SCD Type 2 processing
scd_updates_data = [
    (1, "John Doe Jr", "john.jr@email.com", "123 Main St", datetime(2024, 1, 5, 10, 0, 0)),
    (2, "Jane Smith-Jones", "jane.new@email.com", "456 Oak Ave", datetime(2024, 1, 5, 11, 0, 0)),
    (4, "Alice Brown", "alice@email.com", "321 Elm St", datetime(2024, 1, 5, 12, 0, 0)),  # New
]

scd_updates_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("effective_date", TimestampType(), False)
])

scd_updates_df = spark.createDataFrame(scd_updates_data, scd_updates_schema)

(scd_updates_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("scd2_updates")
)

print("✓ Created scd2_updates table")
```

#### Dataset 5: Product CDC Events

```python
# Product CDC events
product_cdc_data = [
    (101, "Laptop", 999.99, 50, "INSERT", 1, "2024-01-01 10:00:00"),
    (102, "Mouse", 29.99, 200, "INSERT", 1, "2024-01-01 10:00:00"),
    (103, "Keyboard", 79.99, 150, "INSERT", 1, "2024-01-01 10:00:00"),
    (101, "Laptop Pro", 1099.99, 45, "UPDATE", 2, "2024-01-02 11:00:00"),  # Price & name change
    (102, None, None, None, "DELETE", 2, "2024-01-02 12:00:00"),  # Discontinued
    (104, "Monitor", 299.99, 75, "INSERT", 1, "2024-01-02 13:00:00"),  # New product
]

product_cdc_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("operation", StringType(), False),
    StructField("sequence_number", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

product_cdc_df = spark.createDataFrame(product_cdc_data, product_cdc_schema)

(product_cdc_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("product_cdc_events")
)

print("✓ Created product_cdc_events table")
```

#### Dataset 6: Empty Product Table

```python
# Create empty product table for CDC processing
product_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("sequence_number", IntegerType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True)
])

empty_product_df = spark.createDataFrame([], product_schema)

(empty_product_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("products")
)

print("✓ Created empty products table")
```

#### Dataset 7: CDC Events with Duplicates

```python
# CDC events with duplicates (for deduplication exercises)
duplicate_cdc_data = [
    (10, "Test User 1", "test1@email.com", "Address 1", "INSERT", 1, "2024-01-01 10:00:00"),
    (10, "Test User 1", "test1@email.com", "Address 1", "INSERT", 1, "2024-01-01 10:00:00"),  # Duplicate
    (11, "Test User 2", "test2@email.com", "Address 2", "INSERT", 1, "2024-01-01 10:00:00"),
    (10, "Test User 1 Updated", "test1.new@email.com", "Address 1", "UPDATE", 2, "2024-01-01 11:00:00"),
    (10, "Test User 1 Updated", "test1.new@email.com", "Address 1", "UPDATE", 2, "2024-01-01 11:00:00"),  # Duplicate
    (10, "Test User 1 Final", "test1.final@email.com", "Address 1", "UPDATE", 3, "2024-01-01 12:00:00"),
]

duplicate_cdc_df = spark.createDataFrame(duplicate_cdc_data, cdc_schema)

(duplicate_cdc_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("cdc_events_with_duplicates")
)

print("✓ Created cdc_events_with_duplicates table")
```

#### Dataset 8: Out-of-Order CDC Events

```python
# CDC events arriving out of order
out_of_order_cdc_data = [
    (20, "User 20", "user20@email.com", "Address 20", "INSERT", 1, "2024-01-01 10:00:00"),
    (20, "User 20 v3", "user20.v3@email.com", "Address 20", "UPDATE", 3, "2024-01-01 12:00:00"),  # Arrives first
    (20, "User 20 v2", "user20.v2@email.com", "Address 20", "UPDATE", 2, "2024-01-01 11:00:00"),  # Arrives second (out of order)
    (21, "User 21 v2", "user21.v2@email.com", "Address 21", "UPDATE", 2, "2024-01-01 11:00:00"),  # No INSERT yet
    (21, "User 21", "user21@email.com", "Address 21", "INSERT", 1, "2024-01-01 10:00:00"),  # Arrives late
]

out_of_order_df = spark.createDataFrame(out_of_order_cdc_data, cdc_schema)

(out_of_order_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("cdc_events_out_of_order")
)

print("✓ Created cdc_events_out_of_order table")
```

### 3. Helper Functions

```python
def show_table_state(table_name):
    """Display current state of a table"""
    print(f"\n{'='*60}")
    print(f"TABLE: {table_name}")
    print('='*60)
    df = spark.table(table_name)
    print(f"Total records: {df.count()}")
    df.show(truncate=False)
    print('='*60 + "\n")

def show_cdc_summary(cdc_table):
    """Show summary of CDC events"""
    print(f"\n{'='*60}")
    print(f"CDC EVENTS SUMMARY: {cdc_table}")
    print('='*60)
    
    summary = spark.sql(f"""
        SELECT 
            operation,
            COUNT(*) as event_count,
            MIN(timestamp) as first_event,
            MAX(timestamp) as last_event
        FROM {cdc_table}
        GROUP BY operation
        ORDER BY operation
    """)
    
    summary.show(truncate=False)
    print('='*60 + "\n")

def show_scd2_history(table_name, customer_id):
    """Show SCD Type 2 history for a customer"""
    print(f"\n{'='*60}")
    print(f"SCD TYPE 2 HISTORY: Customer {customer_id}")
    print('='*60)
    
    history = spark.sql(f"""
        SELECT *
        FROM {table_name}
        WHERE customer_id = {customer_id}
        ORDER BY effective_date
    """)
    
    history.show(truncate=False)
    print('='*60 + "\n")

def compare_before_after(table_name, operation_desc):
    """Compare table state before and after operation"""
    before_count = spark.table(table_name).count()
    print(f"Before {operation_desc}: {before_count} records")
    return before_count

def show_after(table_name, before_count):
    """Show table state after operation"""
    after_count = spark.table(table_name).count()
    print(f"After: {after_count} records")
    print(f"Change: {after_count - before_count:+d} records")
    spark.table(table_name).show()

# Display initial state
print("\n" + "="*60)
print("INITIAL TABLE STATES")
print("="*60)

show_table_state("customers")
show_cdc_summary("cdc_events")
show_table_state("customers_scd2")
```

### 4. CDC Schema Definitions

```python
# Standard CDC schema
cdc_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("operation", StringType(), False),
    StructField("sequence_number", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

# SCD Type 2 schema
scd2_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("effective_date", TimestampType(), False),
    StructField("end_date", TimestampType(), True),
    StructField("is_current", BooleanType(), False)
])

print("✓ Schema definitions loaded")
```

### 5. Quick Reference

```python
print("""
╔════════════════════════════════════════════════════════════╗
║           DAY 19: CHANGE DATA CAPTURE - QUICK REFERENCE    ║
╠════════════════════════════════════════════════════════════╣
║ Tables Created:                                            ║
║   • customers (5 records - target for CDC)                 ║
║   • cdc_events (7 events: INSERT, UPDATE, DELETE)          ║
║   • customers_scd2 (3 records - SCD Type 2)                ║
║   • scd2_updates (3 records - updates for SCD2)            ║
║   • product_cdc_events (6 events)                          ║
║   • products (empty - for CDC processing)                  ║
║   • cdc_events_with_duplicates (6 events with dupes)       ║
║   • cdc_events_out_of_order (5 out-of-order events)        ║
║                                                            ║
║ CDC Operations:                                            ║
║   • INSERT: Add new records                                ║
║   • UPDATE: Modify existing records                        ║
║   • DELETE: Remove records                                 ║
║                                                            ║
║ SCD Types:                                                 ║
║   • Type 1: Overwrite (no history)                         ║
║   • Type 2: Historical tracking (effective dates)          ║
║                                                            ║
║ Helper Functions:                                          ║
║   • show_table_state(table_name)                           ║
║   • show_cdc_summary(cdc_table)                            ║
║   • show_scd2_history(table_name, customer_id)             ║
║   • compare_before_after(table_name, operation_desc)       ║
║   • show_after(table_name, before_count)                   ║
╚════════════════════════════════════════════════════════════╝
""")
```

## Setup Complete! ✅

You now have:
- ✅ 8 Delta tables for CDC scenarios
- ✅ Customer data with CDC events
- ✅ SCD Type 2 dimension table
- ✅ Product CDC events
- ✅ Duplicate and out-of-order events for practice
- ✅ Helper functions for analysis
- ✅ Ready for CDC exercises!

**Next Step**: Open `exercise.py` and start practicing CDC operations!
