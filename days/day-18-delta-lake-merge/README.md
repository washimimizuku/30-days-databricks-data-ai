# Day 18: Delta Lake MERGE (UPSERT)

## Learning Objectives

By the end of today, you will:
- Understand MERGE operations and when to use them
- Implement UPSERT (Update + Insert) logic
- Handle DELETE operations with MERGE
- Use conditional updates and inserts
- Optimize MERGE performance
- Handle schema evolution in MERGE
- Implement slowly changing dimensions (SCD Type 1)
- Apply MERGE in incremental data processing
- Understand MERGE internals and best practices

## Topics Covered

### 1. What is MERGE?

MERGE is a SQL operation that combines INSERT, UPDATE, and DELETE in a single atomic transaction.

**Use Cases**:
- Upserting data (update if exists, insert if not)
- Synchronizing tables
- Applying incremental updates
- Implementing slowly changing dimensions
- Processing change data capture (CDC) events

**Syntax Overview**:
```sql
MERGE INTO target_table
USING source_table
ON merge_condition
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

### 2. Basic MERGE Syntax

#### Simple UPSERT

```sql
-- Update existing records, insert new ones
MERGE INTO customers AS target
USING updates AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
  UPDATE SET
    target.name = source.name,
    target.email = source.email,
    target.updated_at = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, email, created_at)
  VALUES (source.customer_id, source.name, source.email, current_timestamp())
```

#### MERGE with DELETE

```sql
-- Update, insert, or delete
MERGE INTO inventory AS target
USING inventory_updates AS source
ON target.product_id = source.product_id
WHEN MATCHED AND source.quantity = 0 THEN
  DELETE
WHEN MATCHED THEN
  UPDATE SET target.quantity = source.quantity
WHEN NOT MATCHED THEN
  INSERT (product_id, quantity)
  VALUES (source.product_id, source.quantity)
```

### 3. MERGE Components

#### Target Table
The table being modified (must be a Delta table).

```sql
MERGE INTO target_table  -- Must be Delta
```

#### Source
Can be:
- Another table
- A view
- A subquery
- A DataFrame (in Python)

```sql
-- Table
USING source_table

-- Subquery
USING (SELECT * FROM staging WHERE date = current_date()) AS source

-- View
USING latest_updates_view AS source
```

#### Merge Condition
Defines how to match records between source and target.

```sql
ON target.id = source.id  -- Simple key match

ON target.id = source.id AND target.region = source.region  -- Composite key

ON target.email = source.email  -- Natural key
```

#### WHEN Clauses

**WHEN MATCHED**: Record exists in both target and source
```sql
WHEN MATCHED THEN UPDATE SET ...
WHEN MATCHED AND condition THEN UPDATE SET ...
WHEN MATCHED AND condition THEN DELETE
```

**WHEN NOT MATCHED**: Record exists in source but not target
```sql
WHEN NOT MATCHED THEN INSERT ...
WHEN NOT MATCHED AND condition THEN INSERT ...
```

**WHEN NOT MATCHED BY SOURCE**: Record exists in target but not source
```sql
WHEN NOT MATCHED BY SOURCE THEN DELETE
WHEN NOT MATCHED BY SOURCE AND condition THEN UPDATE SET ...
```

### 4. Python MERGE with Delta

#### Using DeltaTable API

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import *

# Load target Delta table
target = DeltaTable.forName(spark, "customers")

# Source DataFrame
updates = spark.table("customer_updates")

# Perform MERGE
(target.alias("target")
  .merge(
    updates.alias("source"),
    "target.customer_id = source.customer_id"
  )
  .whenMatchedUpdate(set = {
    "name": "source.name",
    "email": "source.email",
    "updated_at": current_timestamp()
  })
  .whenNotMatchedInsert(values = {
    "customer_id": "source.customer_id",
    "name": "source.name",
    "email": "source.email",
    "created_at": current_timestamp()
  })
  .execute()
)
```

#### Conditional MERGE

```python
# Update only if source data is newer
(target.alias("target")
  .merge(
    updates.alias("source"),
    "target.id = source.id"
  )
  .whenMatchedUpdate(
    condition = "source.updated_at > target.updated_at",
    set = {
      "value": "source.value",
      "updated_at": "source.updated_at"
    }
  )
  .whenNotMatchedInsert(values = {
    "id": "source.id",
    "value": "source.value",
    "updated_at": "source.updated_at"
  })
  .execute()
)
```

### 5. Advanced MERGE Patterns

#### Pattern 1: SCD Type 1 (Overwrite)

Simply update all columns with new values.

```sql
MERGE INTO dim_customer AS target
USING customer_updates AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
  UPDATE SET
    target.name = source.name,
    target.address = source.address,
    target.phone = source.phone,
    target.updated_at = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, address, phone, created_at)
  VALUES (source.customer_id, source.name, source.address, source.phone, current_timestamp())
```

#### Pattern 2: Conditional Updates

Update only when certain conditions are met.

```python
(target.alias("t")
  .merge(source.alias("s"), "t.id = s.id")
  .whenMatchedUpdate(
    condition = "s.version > t.version",  # Only if newer
    set = {"value": "s.value", "version": "s.version"}
  )
  .whenMatchedUpdate(
    condition = "s.version = t.version AND s.value != t.value",  # Same version, different value
    set = {"value": "s.value", "conflict": lit(True)}
  )
  .whenNotMatchedInsert(values = {
    "id": "s.id",
    "value": "s.value",
    "version": "s.version"
  })
  .execute()
)
```

#### Pattern 3: Soft Deletes

Mark records as deleted instead of physically removing them.

```sql
MERGE INTO products AS target
USING product_updates AS source
ON target.product_id = source.product_id
WHEN MATCHED AND source.is_deleted = true THEN
  UPDATE SET
    target.is_active = false,
    target.deleted_at = current_timestamp()
WHEN MATCHED AND source.is_deleted = false THEN
  UPDATE SET
    target.name = source.name,
    target.price = source.price,
    target.updated_at = current_timestamp()
WHEN NOT MATCHED AND source.is_deleted = false THEN
  INSERT (product_id, name, price, is_active, created_at)
  VALUES (source.product_id, source.name, source.price, true, current_timestamp())
```

#### Pattern 4: Deduplication with MERGE

```python
from pyspark.sql.window import Window

# Deduplicate source data first
window_spec = Window.partitionBy("id").orderBy(col("timestamp").desc())

deduplicated_source = (
    source
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

# Then merge
(target.alias("t")
  .merge(deduplicated_source.alias("s"), "t.id = s.id")
  .whenMatchedUpdate(set = {"value": "s.value"})
  .whenNotMatchedInsert(values = {"id": "s.id", "value": "s.value"})
  .execute()
)
```

### 6. MERGE with Multiple Conditions

#### Multiple WHEN MATCHED Clauses

```sql
MERGE INTO orders AS target
USING order_updates AS source
ON target.order_id = source.order_id
WHEN MATCHED AND source.status = 'CANCELLED' THEN
  UPDATE SET
    target.status = 'CANCELLED',
    target.cancelled_at = current_timestamp()
WHEN MATCHED AND source.status = 'COMPLETED' THEN
  UPDATE SET
    target.status = 'COMPLETED',
    target.completed_at = current_timestamp(),
    target.total_amount = source.total_amount
WHEN MATCHED THEN
  UPDATE SET
    target.status = source.status,
    target.updated_at = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (order_id, status, created_at)
  VALUES (source.order_id, source.status, current_timestamp())
```

**Order Matters**: First matching condition wins!

#### WHEN NOT MATCHED BY SOURCE

```sql
-- Archive records that no longer exist in source
MERGE INTO active_users AS target
USING current_users AS source
ON target.user_id = source.user_id
WHEN MATCHED THEN
  UPDATE SET target.last_seen = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (user_id, last_seen)
  VALUES (source.user_id, current_timestamp())
WHEN NOT MATCHED BY SOURCE THEN
  UPDATE SET
    target.is_active = false,
    target.archived_at = current_timestamp()
```

### 7. MERGE Performance Optimization

#### 1. Partition Pruning

```python
# Filter source to relevant partitions
source_filtered = source.filter(col("date") >= "2024-01-01")

(target.alias("t")
  .merge(source_filtered.alias("s"), "t.id = s.id AND t.date = s.date")
  .whenMatchedUpdate(set = {"value": "s.value"})
  .whenNotMatchedInsert(values = {"id": "s.id", "date": "s.date", "value": "s.value"})
  .execute()
)
```

#### 2. Predicate Pushdown

```sql
-- Add partition filter to merge condition
MERGE INTO sales AS target
USING sales_updates AS source
ON target.sale_id = source.sale_id
  AND target.date = source.date  -- Partition column
  AND source.date >= '2024-01-01'  -- Prune partitions
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

#### 3. Optimize Before MERGE

```sql
-- Compact small files before large MERGE
OPTIMIZE target_table;

-- Then perform MERGE
MERGE INTO target_table ...
```

#### 4. Repartition Source

```python
# Repartition source to match target partitioning
source_repartitioned = source.repartition("date", "region")

(target.alias("t")
  .merge(source_repartitioned.alias("s"), "t.id = s.id")
  .whenMatchedUpdate(set = {"value": "s.value"})
  .whenNotMatchedInsert(values = {"id": "s.id", "value": "s.value"})
  .execute()
)
```

### 8. MERGE Internals

#### How MERGE Works

1. **Identify Matches**: Join target and source on merge condition
2. **Classify Records**:
   - Matched: In both target and source
   - Not matched: In source only
   - Not matched by source: In target only
3. **Apply Actions**: Execute UPDATE, INSERT, or DELETE
4. **Write New Files**: Create new Parquet files with changes
5. **Update Transaction Log**: Record the operation

#### MERGE Metrics

```python
# Get MERGE metrics
merge_result = (target.alias("t")
  .merge(source.alias("s"), "t.id = s.id")
  .whenMatchedUpdate(set = {"value": "s.value"})
  .whenNotMatchedInsert(values = {"id": "s.id", "value": "s.value"})
  .execute()
)

# View metrics (if available)
print(f"Rows updated: {merge_result.get('num_updated_rows', 'N/A')}")
print(f"Rows inserted: {merge_result.get('num_inserted_rows', 'N/A')}")
```

#### Check MERGE History

```sql
-- View MERGE operations
DESCRIBE HISTORY target_table;

-- Filter for MERGE operations
SELECT *
FROM (DESCRIBE HISTORY target_table)
WHERE operation = 'MERGE';
```

### 9. Schema Evolution with MERGE

#### Automatic Schema Evolution

```python
# Enable schema evolution
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# MERGE will automatically add new columns from source
(target.alias("t")
  .merge(source.alias("s"), "t.id = s.id")
  .whenMatchedUpdate(set = {"*": "*"})  # Update all columns
  .whenNotMatchedInsert(values = {"*": "*"})  # Insert all columns
  .execute()
)
```

#### Manual Schema Evolution

```python
# Add columns to target before MERGE
spark.sql("""
  ALTER TABLE target_table
  ADD COLUMNS (new_column STRING, another_column INT)
""")

# Then perform MERGE
(target.alias("t")
  .merge(source.alias("s"), "t.id = s.id")
  .whenMatchedUpdate(set = {
    "value": "s.value",
    "new_column": "s.new_column"
  })
  .whenNotMatchedInsert(values = {
    "id": "s.id",
    "value": "s.value",
    "new_column": "s.new_column"
  })
  .execute()
)
```

### 10. Common MERGE Patterns

#### Pattern 1: Daily Incremental Load

```python
# Load yesterday's data
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
incremental_data = spark.table("staging").filter(col("date") == yesterday)

# Merge into target
target = DeltaTable.forName(spark, "production.sales")

(target.alias("t")
  .merge(
    incremental_data.alias("s"),
    "t.sale_id = s.sale_id AND t.date = s.date"
  )
  .whenMatchedUpdate(set = {
    "amount": "s.amount",
    "status": "s.status",
    "updated_at": current_timestamp()
  })
  .whenNotMatchedInsert(values = {
    "sale_id": "s.sale_id",
    "date": "s.date",
    "amount": "s.amount",
    "status": "s.status",
    "created_at": current_timestamp()
  })
  .execute()
)
```

#### Pattern 2: Streaming MERGE

```python
def merge_micro_batch(micro_batch_df, batch_id):
    """Merge function for streaming"""
    target = DeltaTable.forName(spark, "target_table")
    
    (target.alias("t")
      .merge(micro_batch_df.alias("s"), "t.id = s.id")
      .whenMatchedUpdate(set = {"value": "s.value"})
      .whenNotMatchedInsert(values = {"id": "s.id", "value": "s.value"})
      .execute()
    )

# Use with foreachBatch
(streaming_df.writeStream
  .foreachBatch(merge_micro_batch)
  .option("checkpointLocation", "/checkpoints/merge")
  .start()
)
```

#### Pattern 3: Multi-Table MERGE

```python
# Merge into multiple tables from single source
source = spark.table("updates")

# Merge into table 1
target1 = DeltaTable.forName(spark, "table1")
(target1.alias("t")
  .merge(source.alias("s"), "t.id = s.id")
  .whenMatchedUpdate(set = {"value": "s.value"})
  .whenNotMatchedInsert(values = {"id": "s.id", "value": "s.value"})
  .execute()
)

# Merge into table 2
target2 = DeltaTable.forName(spark, "table2")
(target2.alias("t")
  .merge(source.alias("s"), "t.id = s.id")
  .whenMatchedUpdate(set = {"count": "t.count + 1"})
  .whenNotMatchedInsert(values = {"id": "s.id", "count": lit(1)})
  .execute()
)
```

## Best Practices

### 1. Always Use Merge Conditions on Keys

```python
# Good - uses primary key
.merge(source, "target.id = source.id")

# Bad - no proper key
.merge(source, "target.name = source.name")  # Names can duplicate!
```

### 2. Deduplicate Source Before MERGE

```python
# Deduplicate first
deduplicated = source.dropDuplicates(["id"])

# Then merge
target.merge(deduplicated, "target.id = source.id")
```

### 3. Use Partition Filters

```python
# Filter to relevant partitions
source_filtered = source.filter(col("date") >= "2024-01-01")

target.merge(
    source_filtered,
    "target.id = source.id AND target.date = source.date"
)
```

### 4. Monitor MERGE Performance

```python
import time

start = time.time()
target.merge(source, "target.id = source.id").whenMatchedUpdate(...).execute()
duration = time.time() - start

print(f"MERGE took {duration:.2f} seconds")
```

### 5. Use OPTIMIZE After Large MERGEs

```sql
-- After large MERGE operation
OPTIMIZE target_table;

-- With Z-ordering
OPTIMIZE target_table ZORDER BY (id, date);
```

## Common Pitfalls

### 1. Duplicate Keys in Source

```python
# Problem: Source has duplicates
source = spark.createDataFrame([
    (1, "A"),
    (1, "B"),  # Duplicate!
    (2, "C")
], ["id", "value"])

# Solution: Deduplicate first
from pyspark.sql.window import Window

window = Window.partitionBy("id").orderBy(col("timestamp").desc())
deduplicated = source.withColumn("rn", row_number().over(window)).filter(col("rn") == 1)
```

### 2. Missing Partition Filters

```python
# Slow: Scans all partitions
target.merge(source, "target.id = source.id")

# Fast: Prunes partitions
target.merge(source, "target.id = source.id AND target.date = source.date")
```

### 3. Not Handling NULLs

```sql
-- Problem: NULL != NULL in SQL
ON target.id = source.id  -- Doesn't match if both are NULL

-- Solution: Handle NULLs explicitly
ON (target.id = source.id OR (target.id IS NULL AND source.id IS NULL))
```

### 4. Schema Mismatch

```python
# Enable auto schema merge
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Or align schemas manually
source_aligned = source.select([col(c) for c in target.toDF().columns])
```

## Key Takeaways

1. **MERGE** combines INSERT, UPDATE, and DELETE in one operation
2. **UPSERT** = Update if exists, Insert if not
3. **Target must be Delta**, source can be anything
4. **Merge condition** defines how records match
5. **Multiple WHEN clauses** supported (order matters!)
6. **WHEN NOT MATCHED BY SOURCE** handles records only in target
7. **Deduplicate source** before MERGE
8. **Use partition filters** for performance
9. **OPTIMIZE after large MERGEs** to compact files
10. **Schema evolution** supported with configuration

## Exam Tips

1. Know MERGE syntax (WHEN MATCHED, WHEN NOT MATCHED)
2. Understand UPSERT pattern
3. Know when to use MERGE vs INSERT/UPDATE
4. Understand WHEN NOT MATCHED BY SOURCE
5. Know that target must be Delta table
6. Understand order of WHEN clauses matters
7. Know how to handle duplicates in source
8. Understand MERGE performance optimization
9. Know schema evolution options
10. Understand MERGE with streaming (foreachBatch)

## Additional Resources

- [Delta Lake MERGE](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge)
- [MERGE Performance](https://docs.databricks.com/delta/merge.html#performance-tuning)
- [Schema Evolution](https://docs.databricks.com/delta/delta-batch.html#automatic-schema-evolution)

---

**Next**: Day 19 - Change Data Capture (CDC)
