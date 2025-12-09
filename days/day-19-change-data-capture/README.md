# Day 19: Change Data Capture (CDC)

## Learning Objectives

By the end of today, you will:
- Understand Change Data Capture (CDC) concepts
- Process CDC events (INSERT, UPDATE, DELETE)
- Implement Slowly Changing Dimensions (SCD) Type 1 and Type 2
- Use MERGE for CDC processing
- Handle CDC with streaming
- Apply CDC in real-world scenarios
- Understand CDC file formats and patterns
- Implement historical tracking with SCD Type 2
- Optimize CDC pipelines

## Topics Covered

### 1. What is Change Data Capture (CDC)?

CDC is a design pattern that tracks changes in source systems and propagates them to target systems.

**Key Concepts**:
- Captures INSERT, UPDATE, DELETE operations
- Maintains change history
- Enables incremental data processing
- Reduces data transfer volume
- Supports real-time data synchronization

**Common CDC Sources**:
- Database transaction logs
- Application event streams
- API change feeds
- File-based CDC exports

### 2. CDC Event Types

#### INSERT Events
New records added to the source system.

```python
{
  "operation": "INSERT",
  "customer_id": 123,
  "name": "John Doe",
  "email": "john@email.com",
  "timestamp": "2024-01-15 10:00:00"
}
```

#### UPDATE Events
Existing records modified in the source system.

```python
{
  "operation": "UPDATE",
  "customer_id": 123,
  "name": "John Doe",
  "email": "john.new@email.com",  # Changed
  "timestamp": "2024-01-16 11:00:00"
}
```

#### DELETE Events
Records removed from the source system.

```python
{
  "operation": "DELETE",
  "customer_id": 123,
  "timestamp": "2024-01-17 12:00:00"
}
```

### 3. Processing CDC with MERGE

#### Basic CDC Processing

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import *

# Read CDC events
cdc_events = spark.table("cdc_events")

# Get target table
target = DeltaTable.forName(spark, "customers")

# Process CDC events
(target.alias("target")
    .merge(
        cdc_events.alias("cdc"),
        "target.customer_id = cdc.customer_id"
    )
    .whenMatchedDelete(condition = "cdc.operation = 'DELETE'")
    .whenMatchedUpdate(
        condition = "cdc.operation = 'UPDATE'",
        set = {
            "name": "cdc.name",
            "email": "cdc.email",
            "updated_at": "cdc.timestamp"
        }
    )
    .whenNotMatchedInsert(
        condition = "cdc.operation = 'INSERT'",
        values = {
            "customer_id": "cdc.customer_id",
            "name": "cdc.name",
            "email": "cdc.email",
            "created_at": "cdc.timestamp"
        }
    )
    .execute()
)
```

#### CDC with Sequence Numbers

```python
# Process CDC events in order
cdc_ordered = (
    cdc_events
    .orderBy("sequence_number")  # Ensure correct order
)

# Apply changes
(target.alias("t")
    .merge(cdc_ordered.alias("c"), "t.id = c.id")
    .whenMatchedUpdate(
        condition = "c.operation = 'UPDATE' AND c.sequence_number > t.sequence_number",
        set = {
            "value": "c.value",
            "sequence_number": "c.sequence_number",
            "updated_at": "c.timestamp"
        }
    )
    .whenMatchedDelete(condition = "c.operation = 'DELETE'")
    .whenNotMatchedInsert(
        condition = "c.operation = 'INSERT'",
        values = {
            "id": "c.id",
            "value": "c.value",
            "sequence_number": "c.sequence_number",
            "created_at": "c.timestamp"
        }
    )
    .execute()
)
```

### 4. Slowly Changing Dimensions (SCD)

#### SCD Type 1: Overwrite

Simply overwrite old values with new ones (no history).

```python
# SCD Type 1 - No history tracking
(target.alias("t")
    .merge(updates.alias("s"), "t.customer_id = s.customer_id")
    .whenMatchedUpdate(set = {
        "name": "s.name",
        "email": "s.email",
        "address": "s.address",
        "updated_at": current_timestamp()
    })
    .whenNotMatchedInsert(values = {
        "customer_id": "s.customer_id",
        "name": "s.name",
        "email": "s.email",
        "address": "s.address",
        "created_at": current_timestamp()
    })
    .execute()
)
```

**Use Case**: When historical values don't matter (e.g., correcting typos).

#### SCD Type 2: Historical Tracking

Maintain full history of changes with effective dates.

**Table Schema**:
```python
schema = StructType([
    StructField("customer_id", IntegerType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("address", StringType()),
    StructField("effective_date", TimestampType()),
    StructField("end_date", TimestampType()),
    StructField("is_current", BooleanType())
])
```

**SCD Type 2 Implementation**:

```python
from pyspark.sql.functions import *

def apply_scd_type_2(target_table, source_df, key_col, effective_date_col):
    """
    Apply SCD Type 2 logic
    
    Steps:
    1. Close out old records (set end_date, is_current = false)
    2. Insert new records (set is_current = true)
    """
    
    target = DeltaTable.forName(spark, target_table)
    
    # Step 1: Identify changed records
    changed_records = (
        source_df.alias("source")
        .join(
            target.toDF().filter(col("is_current") == True).alias("target"),
            key_col
        )
        .where(
            # Check if any tracked columns changed
            (col("source.name") != col("target.name")) |
            (col("source.email") != col("target.email")) |
            (col("source.address") != col("target.address"))
        )
        .select("source.*")
    )
    
    # Step 2: Close out old records
    (target.alias("target")
        .merge(
            changed_records.alias("updates"),
            f"target.{key_col} = updates.{key_col} AND target.is_current = true"
        )
        .whenMatchedUpdate(set = {
            "end_date": f"updates.{effective_date_col}",
            "is_current": lit(False)
        })
        .execute()
    )
    
    # Step 3: Insert new versions
    new_versions = (
        changed_records
        .withColumn("effective_date", col(effective_date_col))
        .withColumn("end_date", lit(None).cast("timestamp"))
        .withColumn("is_current", lit(True))
    )
    
    new_versions.write.format("delta").mode("append").saveAsTable(target_table)
    
    # Step 4: Insert truly new records
    new_records = (
        source_df.alias("source")
        .join(
            target.toDF().alias("target"),
            key_col,
            "left_anti"
        )
        .withColumn("effective_date", col(effective_date_col))
        .withColumn("end_date", lit(None).cast("timestamp"))
        .withColumn("is_current", lit(True))
    )
    
    new_records.write.format("delta").mode("append").saveAsTable(target_table)
```

**Querying SCD Type 2**:

```sql
-- Get current records only
SELECT * FROM customers WHERE is_current = true;

-- Get historical record at specific date
SELECT * FROM customers 
WHERE customer_id = 123
  AND effective_date <= '2024-01-15'
  AND (end_date > '2024-01-15' OR end_date IS NULL);

-- Get full history for a customer
SELECT * FROM customers 
WHERE customer_id = 123
ORDER BY effective_date;
```

### 5. CDC File Formats

#### JSON CDC Format

```json
{
  "operation": "UPDATE",
  "table": "customers",
  "key": {"customer_id": 123},
  "before": {
    "name": "John Doe",
    "email": "john@old.com"
  },
  "after": {
    "name": "John Doe",
    "email": "john@new.com"
  },
  "timestamp": "2024-01-15T10:00:00Z",
  "sequence": 12345
}
```

#### Debezium Format

```json
{
  "before": {
    "id": 123,
    "name": "John Doe",
    "email": "john@old.com"
  },
  "after": {
    "id": 123,
    "name": "John Doe",
    "email": "john@new.com"
  },
  "source": {
    "version": "1.9.0",
    "connector": "mysql",
    "name": "dbserver1",
    "ts_ms": 1642248000000,
    "snapshot": "false",
    "db": "inventory",
    "table": "customers"
  },
  "op": "u",
  "ts_ms": 1642248000123
}
```

### 6. Streaming CDC Processing

#### Streaming CDC with Auto Loader

```python
# Read CDC events as stream
cdc_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(cdc_schema)
    .load("/cdc/events/")
)

# Process CDC events
def process_cdc_batch(batch_df, batch_id):
    """Process each micro-batch of CDC events"""
    target = DeltaTable.forName(spark, "customers")
    
    (target.alias("t")
        .merge(batch_df.alias("c"), "t.customer_id = c.customer_id")
        .whenMatchedDelete(condition = "c.operation = 'DELETE'")
        .whenMatchedUpdate(
            condition = "c.operation = 'UPDATE'",
            set = {
                "name": "c.name",
                "email": "c.email",
                "updated_at": "c.timestamp"
            }
        )
        .whenNotMatchedInsert(
            condition = "c.operation = 'INSERT'",
            values = {
                "customer_id": "c.customer_id",
                "name": "c.name",
                "email": "c.email",
                "created_at": "c.timestamp"
            }
        )
        .execute()
    )

# Write stream with foreachBatch
(cdc_stream.writeStream
    .foreachBatch(process_cdc_batch)
    .option("checkpointLocation", "/checkpoints/cdc")
    .trigger(processingTime="1 minute")
    .start()
)
```

#### Handling Late CDC Events

```python
# Add watermark for late data handling
cdc_with_watermark = (
    cdc_stream
    .withWatermark("timestamp", "1 hour")  # Wait 1 hour for late events
)

# Process with watermark
def process_with_watermark(batch_df, batch_id):
    # Deduplicate by sequence number (keep latest)
    from pyspark.sql.window import Window
    
    window_spec = Window.partitionBy("customer_id").orderBy(col("sequence_number").desc())
    
    deduplicated = (
        batch_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    
    # Apply CDC
    target = DeltaTable.forName(spark, "customers")
    # ... merge logic
```

### 7. CDC Best Practices

#### 1. Idempotency

Ensure CDC processing can be run multiple times safely.

```python
# Use sequence numbers or timestamps
(target.alias("t")
    .merge(cdc.alias("c"), "t.id = c.id")
    .whenMatchedUpdate(
        condition = "c.sequence_number > t.sequence_number",  # Only if newer
        set = {"value": "c.value", "sequence_number": "c.sequence_number"}
    )
    .whenNotMatchedInsert(values = {
        "id": "c.id",
        "value": "c.value",
        "sequence_number": "c.sequence_number"
    })
    .execute()
)
```

#### 2. Ordering

Process CDC events in the correct order.

```python
# Order by sequence number or timestamp
cdc_ordered = cdc_events.orderBy("sequence_number", "timestamp")
```

#### 3. Deduplication

Handle duplicate CDC events.

```python
# Deduplicate by keeping latest event
from pyspark.sql.window import Window

window_spec = Window.partitionBy("customer_id", "operation").orderBy(col("sequence_number").desc())

deduplicated_cdc = (
    cdc_events
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)
```

#### 4. Error Handling

Handle malformed or invalid CDC events.

```python
# Validate CDC events
valid_cdc = cdc_events.filter(
    col("operation").isin("INSERT", "UPDATE", "DELETE") &
    col("customer_id").isNotNull() &
    col("timestamp").isNotNull()
)

# Log invalid events
invalid_cdc = cdc_events.subtract(valid_cdc)
invalid_cdc.write.format("delta").mode("append").saveAsTable("cdc_errors")
```

### 8. Complete CDC Pipeline Example

```python
class CDCProcessor:
    """Complete CDC processing pipeline"""
    
    def __init__(self, target_table, cdc_source):
        self.target_table = target_table
        self.cdc_source = cdc_source
        
    def validate_cdc_events(self, cdc_df):
        """Validate CDC events"""
        return cdc_df.filter(
            col("operation").isin("INSERT", "UPDATE", "DELETE") &
            col("customer_id").isNotNull() &
            col("timestamp").isNotNull()
        )
    
    def deduplicate_events(self, cdc_df):
        """Deduplicate CDC events by keeping latest"""
        from pyspark.sql.window import Window
        
        window_spec = Window.partitionBy("customer_id").orderBy(
            col("sequence_number").desc(),
            col("timestamp").desc()
        )
        
        return (
            cdc_df
            .withColumn("row_num", row_number().over(window_spec))
            .filter(col("row_num") == 1)
            .drop("row_num")
        )
    
    def apply_cdc(self, cdc_df):
        """Apply CDC events to target table"""
        target = DeltaTable.forName(spark, self.target_table)
        
        (target.alias("t")
            .merge(cdc_df.alias("c"), "t.customer_id = c.customer_id")
            .whenMatchedDelete(condition = "c.operation = 'DELETE'")
            .whenMatchedUpdate(
                condition = "c.operation = 'UPDATE' AND c.sequence_number > t.sequence_number",
                set = {
                    "name": "c.name",
                    "email": "c.email",
                    "address": "c.address",
                    "sequence_number": "c.sequence_number",
                    "updated_at": "c.timestamp"
                }
            )
            .whenNotMatchedInsert(
                condition = "c.operation = 'INSERT'",
                values = {
                    "customer_id": "c.customer_id",
                    "name": "c.name",
                    "email": "c.email",
                    "address": "c.address",
                    "sequence_number": "c.sequence_number",
                    "created_at": "c.timestamp"
                }
            )
            .execute()
        )
    
    def process_batch(self, batch_df, batch_id):
        """Process a batch of CDC events"""
        print(f"Processing batch {batch_id}...")
        
        # Validate
        valid_events = self.validate_cdc_events(batch_df)
        
        # Deduplicate
        clean_events = self.deduplicate_events(valid_events)
        
        # Apply CDC
        self.apply_cdc(clean_events)
        
        print(f"Batch {batch_id} processed: {clean_events.count()} events")
    
    def start_streaming(self):
        """Start streaming CDC processing"""
        cdc_stream = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(cdc_schema)
            .load(self.cdc_source)
        )
        
        query = (
            cdc_stream.writeStream
            .foreachBatch(self.process_batch)
            .option("checkpointLocation", f"/checkpoints/{self.target_table}")
            .trigger(processingTime="1 minute")
            .start()
        )
        
        return query

# Usage
processor = CDCProcessor("customers", "/cdc/events/")
query = processor.start_streaming()
```

## Key Takeaways

1. **CDC** captures INSERT, UPDATE, DELETE operations from source systems
2. **MERGE** is the primary tool for applying CDC events
3. **SCD Type 1** overwrites old values (no history)
4. **SCD Type 2** maintains full history with effective dates
5. **Sequence numbers** ensure correct event ordering
6. **Deduplication** handles duplicate CDC events
7. **Idempotency** allows safe reprocessing
8. **Streaming CDC** uses foreachBatch with MERGE
9. **Validation** catches malformed events
10. **Watermarking** handles late-arriving events

## Exam Tips

1. Know the difference between SCD Type 1 and Type 2
2. Understand how to process CDC with MERGE
3. Know CDC event types (INSERT, UPDATE, DELETE)
4. Understand sequence number importance
5. Know how to implement SCD Type 2 with effective dates
6. Understand streaming CDC with foreachBatch
7. Know how to query SCD Type 2 tables
8. Understand idempotency in CDC processing
9. Know how to handle late CDC events
10. Understand CDC validation and error handling

## Additional Resources

- [Delta Lake CDC](https://docs.delta.io/latest/delta-change-data-feed.html)
- [SCD Implementation](https://docs.databricks.com/delta/delta-batch.html#slowly-changing-data-scd-type-2-operation-into-delta-tables)
- [Streaming CDC](https://docs.databricks.com/structured-streaming/delta-lake.html)

---

**Next**: Day 20 - Multi-Hop Architecture (Bronze → Silver → Gold)
