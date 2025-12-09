# Day 22: Week 3 Review & Incremental Project

## Overview

Congratulations on completing Week 3! Today is dedicated to reviewing and consolidating everything you've learned about Incremental Data Processing. This review day includes:

- Comprehensive review of Days 16-21
- 50-question quiz covering all Week 3 topics
- Hands-on incremental processing project integrating multiple concepts
- Exam preparation tips

## Learning Objectives

By the end of today, you will:
- Consolidate knowledge from Days 16-21
- Identify areas needing additional review
- Build a complete incremental data processing pipeline
- Practice exam-style questions
- Build confidence for Week 4

## Week 3 Topics Recap

### Day 16: Structured Streaming Basics
**Key Concepts**:
- readStream and writeStream
- Output modes (append, complete, update)
- Checkpointing for fault tolerance
- Trigger types (once, processingTime, continuous, availableNow)
- Streaming query lifecycle
- awaitTermination() and stop()

**Critical for Exam**:
- Know the three output modes and when to use each
- Understand checkpoint location importance
- Know trigger types and their use cases
- Recognize streaming query syntax

**Common Patterns**:
```python
# Basic streaming query
df = spark.readStream.format("delta").table("source_table")
query = df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .table("target_table")
```

**Output Modes**:
- **append**: Only new rows (default, most common)
- **complete**: Entire result table (requires aggregation)
- **update**: Only changed rows (for aggregations)

### Day 17: Streaming Transformations
**Key Concepts**:
- Watermarking for late data handling
- Window operations (tumbling, sliding, session)
- withWatermark() syntax
- dropDuplicates() in streaming
- Stateful operations
- Stream-stream joins

**Critical for Exam**:
- Know watermark syntax: `withWatermark("timestamp_col", "10 minutes")`
- Understand window types and syntax
- Know when watermarking is required
- Recognize stateful operation patterns

**Common Patterns**:
```python
# Watermarking with windowing
df.withWatermark("event_time", "10 minutes") \
  .groupBy(window("event_time", "5 minutes"), "user_id") \
  .count()

# Streaming deduplication
df.withWatermark("timestamp", "1 hour") \
  .dropDuplicates(["id"])
```

**Window Types**:
- **Tumbling**: Fixed, non-overlapping (e.g., every 5 minutes)
- **Sliding**: Overlapping (e.g., 10-minute window, slide every 5 minutes)
- **Session**: Gap-based (e.g., 30-minute inactivity gap)

### Day 18: Delta Lake MERGE (UPSERT)
**Key Concepts**:
- MERGE syntax (MERGE INTO ... USING ... ON)
- WHEN MATCHED clauses (UPDATE, DELETE)
- WHEN NOT MATCHED clauses (INSERT)
- Conditional MERGE operations
- MERGE performance optimization
- Schema evolution with MERGE
- SCD Type 1 implementation

**Critical for Exam**:
- Know complete MERGE syntax
- Understand MATCHED vs NOT MATCHED clauses
- Know how to handle duplicates before MERGE
- Recognize MERGE optimization techniques

**Common Patterns**:
```sql
-- Basic UPSERT
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- Conditional update/delete
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED AND s.is_deleted = true THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**MERGE Best Practices**:
- Always deduplicate source before MERGE
- Use specific column updates instead of SET * when possible
- Add predicates to WHEN clauses for conditional logic
- Consider partitioning for large tables

### Day 19: Change Data Capture (CDC)
**Key Concepts**:
- CDC event types (INSERT, UPDATE, DELETE)
- Processing CDC with MERGE
- SCD Type 1 (overwrite)
- SCD Type 2 (historical tracking)
- Handling out-of-order events
- Streaming CDC processing
- CDC with multiple operations per key

**Critical for Exam**:
- Know CDC event type handling
- Understand SCD Type 1 vs Type 2 differences
- Know how to implement SCD Type 2 with effective dates
- Recognize CDC processing patterns

**Common Patterns**:
```python
# SCD Type 2 implementation
def apply_scd_type2(target_table, cdc_df):
    # Close old records
    updates = cdc_df.alias("updates") \
        .join(target_table.alias("target"), "id") \
        .where("target.is_current = true") \
        .select(
            col("target.id"),
            col("target.effective_start_date"),
            col("updates.timestamp").alias("effective_end_date"),
            lit(False).alias("is_current")
        )
    
    # Insert new records
    inserts = cdc_df.select(
        col("id"),
        col("timestamp").alias("effective_start_date"),
        lit(None).alias("effective_end_date"),
        lit(True).alias("is_current"),
        col("*")
    )
```

**SCD Types**:
- **Type 1**: Overwrite (no history) - Simple UPDATE
- **Type 2**: Historical tracking - Close old + Insert new with dates
- **Type 3**: Limited history - Add previous value columns (rare)

### Day 20: Multi-Hop Architecture
**Key Concepts**:
- Medallion Architecture (Bronze → Silver → Gold)
- Bronze layer: Raw data ingestion
- Silver layer: Cleaned and conformed data
- Gold layer: Business-level aggregates
- Incremental processing patterns
- Data quality checks at each layer
- Layer-specific transformations

**Critical for Exam**:
- Know the purpose of each layer
- Understand data flow through layers
- Recognize incremental processing patterns
- Know quality checks for each layer

**Common Patterns**:
```python
# Bronze: Raw ingestion
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .load("/source/path") \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation", "/bronze/checkpoint") \
    .table("bronze.raw_events")

# Silver: Cleaning
spark.readStream \
    .table("bronze.raw_events") \
    .filter(col("event_type").isNotNull()) \
    .withColumn("processed_timestamp", current_timestamp()) \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation", "/silver/checkpoint") \
    .table("silver.clean_events")

# Gold: Aggregation
spark.readStream \
    .table("silver.clean_events") \
    .groupBy("date", "category") \
    .agg(count("*").alias("event_count")) \
    .writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "/gold/checkpoint") \
    .table("gold.daily_summary")
```

**Layer Characteristics**:
- **Bronze**: Raw, append-only, minimal transformation
- **Silver**: Cleaned, validated, conformed schema
- **Gold**: Aggregated, business-ready, optimized for queries

### Day 21: Optimization Techniques
**Key Concepts**:
- Partitioning strategies
- Z-ordering for data skipping
- OPTIMIZE command for file compaction
- VACUUM for old file cleanup
- Data skipping statistics
- File size optimization (target: 1GB)
- Caching strategies
- Query optimization techniques

**Critical for Exam**:
- Know when to partition (high cardinality columns)
- Understand Z-order limitations (max 4 columns)
- Know OPTIMIZE and VACUUM syntax
- Understand VACUUM retention period (default 7 days)
- Know data skipping benefits

**Common Patterns**:
```sql
-- Optimize with Z-order
OPTIMIZE table_name ZORDER BY (col1, col2, col3)

-- Vacuum old files
VACUUM table_name RETAIN 168 HOURS  -- 7 days

-- Create partitioned table
CREATE TABLE partitioned_table (
    id INT,
    name STRING,
    date DATE
)
USING DELTA
PARTITIONED BY (date)

-- Analyze table for statistics
ANALYZE TABLE table_name COMPUTE STATISTICS
```

**Optimization Decision Tree**:
1. **Small files problem?** → Use OPTIMIZE
2. **Frequent filter on columns?** → Use Z-ORDER
3. **Large table with time dimension?** → Use PARTITIONING
4. **Old files accumulating?** → Use VACUUM
5. **Slow queries?** → Check data skipping stats

## Week 3 Integration Concepts

### Streaming + MERGE Pattern
Combine structured streaming with MERGE for real-time UPSERT:
```python
def upsert_to_delta(batch_df, batch_id):
    batch_df.createOrReplaceTempView("updates")
    spark.sql("""
        MERGE INTO target t
        USING updates u
        ON t.id = u.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

query = stream_df.writeStream \
    .foreachBatch(upsert_to_delta) \
    .option("checkpointLocation", "/checkpoint") \
    .start()
```

### CDC + Multi-Hop Pattern
Process CDC events through medallion architecture:
```python
# Bronze: Ingest CDC events
cdc_stream = spark.readStream.table("bronze.cdc_events")

# Silver: Apply CDC logic
def process_cdc(batch_df, batch_id):
    # Group by key, get latest operation
    latest = batch_df.groupBy("id").agg(
        max(struct("timestamp", "operation", "*")).alias("latest")
    ).select("latest.*")
    
    # Apply MERGE
    latest.createOrReplaceTempView("cdc_batch")
    spark.sql("""
        MERGE INTO silver.customers t
        USING cdc_batch s
        ON t.id = s.id
        WHEN MATCHED AND s.operation = 'DELETE' THEN DELETE
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED AND s.operation != 'DELETE' THEN INSERT *
    """)

cdc_stream.writeStream \
    .foreachBatch(process_cdc) \
    .option("checkpointLocation", "/silver/checkpoint") \
    .start()

# Gold: Aggregate from Silver
spark.readStream \
    .table("silver.customers") \
    .groupBy("region", "status") \
    .count() \
    .writeStream \
    .format("delta") \
    .outputMode("complete") \
    .table("gold.customer_summary")
```

### Optimization + Multi-Hop Pattern
Optimize each layer appropriately:
```python
# Bronze: Partition by ingestion date
CREATE TABLE bronze.raw_events (
    event_id STRING,
    event_data STRING,
    ingestion_date DATE
)
USING DELTA
PARTITIONED BY (ingestion_date)

# Silver: Z-order by frequently filtered columns
OPTIMIZE silver.clean_events ZORDER BY (user_id, event_type)

# Gold: Optimize aggregated tables
OPTIMIZE gold.daily_summary

# Vacuum old files from all layers
VACUUM bronze.raw_events RETAIN 168 HOURS
VACUUM silver.clean_events RETAIN 168 HOURS
VACUUM gold.daily_summary RETAIN 168 HOURS
```

## Common Exam Patterns for Week 3

### Pattern 1: Choose the Right Output Mode
**Question Type**: "Which output mode should be used for..."

**Decision Tree**:
- Aggregation with full results needed? → **complete**
- Aggregation with only changes needed? → **update**
- No aggregation or append-only? → **append**

### Pattern 2: MERGE Syntax Completion
**Question Type**: "Complete the MERGE statement to..."

**Key Points**:
- Always starts with `MERGE INTO target USING source ON condition`
- WHEN MATCHED comes before WHEN NOT MATCHED
- Can have multiple WHEN clauses with predicates
- DELETE only works in WHEN MATCHED

### Pattern 3: Identify CDC Processing Issues
**Question Type**: "What is wrong with this CDC processing code?"

**Common Issues**:
- Not handling all operation types (INSERT, UPDATE, DELETE)
- Not deduplicating before MERGE
- Not handling out-of-order events
- Missing timestamp ordering

### Pattern 4: Medallion Architecture Design
**Question Type**: "Which layer should this transformation be in?"

**Guidelines**:
- Raw ingestion → Bronze
- Data cleaning, type conversion → Silver
- Business aggregations → Gold
- Joins across sources → Silver or Gold

### Pattern 5: Optimization Technique Selection
**Question Type**: "What is the best way to optimize..."

**Decision Matrix**:
- Many small files → OPTIMIZE
- Frequent filters on specific columns → Z-ORDER
- Time-based queries on large table → PARTITION
- Storage costs high → VACUUM
- Query performance poor → Check all of the above

## Hands-On Review Exercises

### Exercise 1: End-to-End Streaming Pipeline
Build a complete streaming pipeline with:
1. Bronze layer ingestion
2. Silver layer cleaning with deduplication
3. Gold layer aggregation
4. Proper checkpointing
5. Watermarking for late data

### Exercise 2: CDC Processing with SCD Type 2
Implement a CDC processor that:
1. Reads CDC events from a stream
2. Handles INSERT, UPDATE, DELETE operations
3. Maintains SCD Type 2 history
4. Handles out-of-order events
5. Optimizes the target table

### Exercise 3: Multi-Hop Architecture
Create a medallion architecture for e-commerce data:
1. Bronze: Ingest orders, customers, products
2. Silver: Clean and join data
3. Gold: Create business metrics
4. Apply appropriate optimizations
5. Implement incremental processing

### Exercise 4: Performance Optimization
Given an underperforming pipeline:
1. Identify bottlenecks
2. Apply partitioning strategy
3. Use Z-ordering appropriately
4. Optimize file sizes
5. Measure performance improvements

## Key Formulas and Syntax

### Streaming Query Template
```python
spark.readStream \
    .format("delta") \
    .table("source") \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window("timestamp", "5 minutes"), "key") \
    .agg(count("*").alias("count")) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/checkpoint") \
    .trigger(processingTime="1 minute") \
    .table("target")
```

### MERGE Template
```sql
MERGE INTO target t
USING (
    SELECT * FROM source
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp DESC) = 1
) s
ON t.id = s.id
WHEN MATCHED AND s.operation = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED AND s.operation != 'DELETE' THEN INSERT *
```

### SCD Type 2 Template
```python
# Close old records
UPDATE target
SET is_current = false, end_date = current_timestamp()
WHERE id IN (SELECT id FROM updates) AND is_current = true

# Insert new records
INSERT INTO target
SELECT *, current_timestamp() as start_date, null as end_date, true as is_current
FROM updates
```

### Optimization Template
```sql
-- Analyze current state
DESCRIBE DETAIL table_name;
DESCRIBE HISTORY table_name;

-- Optimize
OPTIMIZE table_name ZORDER BY (col1, col2);

-- Clean up
VACUUM table_name RETAIN 168 HOURS;

-- Verify
DESCRIBE DETAIL table_name;
```

## Exam Preparation Tips

### Week 3 Specific Tips

1. **Memorize Output Modes**
   - append: New rows only (most common)
   - complete: Full result (requires aggregation)
   - update: Changed rows only (for aggregations)

2. **Know MERGE Syntax Cold**
   - Practice writing MERGE statements from scratch
   - Understand all clause combinations
   - Know conditional MERGE patterns

3. **Understand Medallion Architecture**
   - Know what goes in each layer
   - Understand incremental processing
   - Recognize appropriate transformations

4. **Master Optimization Techniques**
   - Know when to use each technique
   - Understand trade-offs
   - Recognize optimization opportunities

5. **Practice CDC Patterns**
   - Know how to handle each operation type
   - Understand SCD Type 1 vs Type 2
   - Practice out-of-order event handling

### Common Mistakes to Avoid

1. **Streaming**:
   - Forgetting checkpoint location
   - Using wrong output mode
   - Not handling late data with watermarks

2. **MERGE**:
   - Not deduplicating source data
   - Wrong order of WHEN clauses
   - Forgetting conditional predicates

3. **CDC**:
   - Not handling DELETE operations
   - Ignoring out-of-order events
   - Missing timestamp ordering

4. **Optimization**:
   - Over-partitioning (too many small partitions)
   - Z-ordering on too many columns (max 4)
   - VACUUM without retention period

5. **Architecture**:
   - Putting business logic in Bronze
   - Not using incremental processing
   - Missing quality checks

## Study Checklist

### Concepts to Master
- [ ] All three streaming output modes
- [ ] Complete MERGE syntax with all clauses
- [ ] SCD Type 1 and Type 2 implementation
- [ ] Medallion architecture layers and purposes
- [ ] All optimization techniques and when to use them
- [ ] Watermarking syntax and use cases
- [ ] Window operations in streaming
- [ ] CDC event processing patterns
- [ ] Incremental processing strategies
- [ ] Data skipping and statistics

### Syntax to Memorize
- [ ] readStream and writeStream
- [ ] withWatermark()
- [ ] window() function
- [ ] MERGE INTO ... USING ... ON
- [ ] WHEN MATCHED / WHEN NOT MATCHED
- [ ] OPTIMIZE ... ZORDER BY
- [ ] VACUUM ... RETAIN
- [ ] foreachBatch pattern
- [ ] Checkpoint location option
- [ ] Trigger types

### Patterns to Practice
- [ ] Basic streaming query
- [ ] Streaming with watermark and window
- [ ] MERGE for UPSERT
- [ ] MERGE for CDC processing
- [ ] SCD Type 2 implementation
- [ ] Bronze → Silver → Gold pipeline
- [ ] Optimization workflow
- [ ] Streaming deduplication
- [ ] Stream-to-batch MERGE
- [ ] Multi-layer incremental processing

## Time Management

- **Quiz**: 30 minutes (50 questions)
- **Review weak areas**: 30 minutes
- **Hands-on project**: 45 minutes
- **Break**: 15 minutes

**Total**: 2 hours

## Next Steps

After completing this review:
1. Take the 50-question quiz
2. Review any questions you got wrong
3. Complete the hands-on project
4. Identify weak areas for additional study
5. Move on to Week 4: Production Pipelines

## Additional Resources

- Databricks Documentation: Structured Streaming
- Databricks Documentation: Delta Lake MERGE
- Databricks Documentation: Optimization
- Practice writing MERGE statements
- Build sample medallion architectures

---

**Remember**: Week 3 covers 25% of the exam (Incremental Data Processing). Master these concepts!

Good luck! 🚀
