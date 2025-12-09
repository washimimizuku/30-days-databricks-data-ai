# Day 20: Multi-Hop Architecture

## Learning Objectives

By the end of today, you will:
- Understand the Medallion Architecture (Bronze, Silver, Gold)
- Implement Bronze layer (raw data ingestion)
- Build Silver layer (cleaned and conformed data)
- Create Gold layer (business-level aggregates)
- Design multi-hop data pipelines
- Apply incremental processing across layers
- Implement data quality checks at each layer
- Understand layer responsibilities and best practices
- Build end-to-end lakehouse architecture

## Topics Covered

### 1. What is Multi-Hop Architecture?

Multi-hop architecture (also called Medallion Architecture) organizes data into layers based on data quality and refinement level.

**Three Layers**:
- **Bronze**: Raw data (as-is from source)
- **Silver**: Cleaned, conformed, enriched data
- **Gold**: Business-level aggregates and features

**Benefits**:
- Clear data quality progression
- Incremental processing at each layer
- Easy troubleshooting and debugging
- Reusable transformations
- Supports both batch and streaming

### 2. Bronze Layer (Raw Data)

The Bronze layer stores raw data exactly as received from source systems.

**Characteristics**:
- Minimal transformation
- Preserves original data
- Append-only (immutable)
- Includes metadata (ingestion time, source file, etc.)
- Schema-on-read approach

**Bronze Layer Example**:

```python
# Read raw JSON files
bronze_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/schemas/bronze")
    .load("/raw/events/")
    .withColumn("ingestion_time", current_timestamp())
    .withColumn("source_file", input_file_name())
)

# Write to Bronze table
(bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/bronze")
    .option("mergeSchema", "true")
    .table("bronze.events")
)
```

**Bronze Best Practices**:
- Store raw data without modification
- Add audit columns (ingestion_time, source_file)
- Enable schema evolution
- Partition by ingestion date
- Keep data for compliance/audit

### 3. Silver Layer (Cleaned Data)

The Silver layer contains cleaned, validated, and conformed data.

**Characteristics**:
- Data quality checks applied
- Standardized formats
- Deduplicated
- Type conversions
- Business rules applied
- Enriched with lookups

**Silver Layer Example**:

```python
# Read from Bronze
bronze_stream = spark.readStream.table("bronze.events")

# Clean and transform
silver_df = (
    bronze_stream
    # Parse JSON if needed
    .withColumn("parsed", from_json(col("value"), schema))
    .select("parsed.*", "ingestion_time", "source_file")
    
    # Data quality checks
    .filter(col("event_id").isNotNull())
    .filter(col("timestamp").isNotNull())
    
    # Type conversions
    .withColumn("timestamp", col("timestamp").cast("timestamp"))
    .withColumn("amount", col("amount").cast("double"))
    
    # Standardization
    .withColumn("event_type", upper(col("event_type")))
    
    # Deduplication
    .dropDuplicates(["event_id"])
    
    # Add processing metadata
    .withColumn("processed_time", current_timestamp())
)

# Write to Silver table
(silver_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/silver")
    .outputMode("append")
    .table("silver.events")
)
```

**Silver Best Practices**:
- Validate data quality
- Apply business rules
- Standardize formats
- Deduplicate records
- Add derived columns
- Document transformations

### 4. Gold Layer (Business Aggregates)

The Gold layer contains business-level aggregates, features, and metrics.

**Characteristics**:
- Aggregated data
- Business metrics
- Denormalized for performance
- Optimized for analytics
- Ready for BI tools

**Gold Layer Example**:

```python
# Read from Silver
silver_stream = spark.readStream.table("silver.events")

# Aggregate to Gold
gold_df = (
    silver_stream
    .withWatermark("timestamp", "10 minutes")
    .groupBy(
        window(col("timestamp"), "1 hour"),
        col("event_type"),
        col("user_id")
    )
    .agg(
        count("*").alias("event_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        countDistinct("event_id").alias("unique_events")
    )
    .select(
        col("window.start").alias("hour_start"),
        col("window.end").alias("hour_end"),
        col("event_type"),
        col("user_id"),
        col("event_count"),
        col("total_amount"),
        col("avg_amount"),
        col("unique_events")
    )
)

# Write to Gold table
(gold_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/gold")
    .outputMode("append")
    .table("gold.hourly_metrics")
)
```

**Gold Best Practices**:
- Create business-focused views
- Denormalize for performance
- Pre-aggregate common queries
- Optimize for BI tools
- Document business logic

### 5. Complete Multi-Hop Pipeline

```python
class MedallionPipeline:
    """Complete Bronze → Silver → Gold pipeline"""
    
    def __init__(self, source_path, checkpoint_base):
        self.source_path = source_path
        self.checkpoint_base = checkpoint_base
        
    def bronze_layer(self):
        """Ingest raw data to Bronze"""
        bronze_df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{self.checkpoint_base}/bronze_schema")
            .load(self.source_path)
            .withColumn("ingestion_time", current_timestamp())
            .withColumn("source_file", input_file_name())
        )
        
        query = (
            bronze_df.writeStream
            .format("delta")
            .option("checkpointLocation", f"{self.checkpoint_base}/bronze")
            .option("mergeSchema", "true")
            .table("bronze.events")
        )
        
        return query
    
    def silver_layer(self):
        """Clean and validate data to Silver"""
        bronze_stream = spark.readStream.table("bronze.events")
        
        silver_df = (
            bronze_stream
            # Parse and validate
            .filter(col("event_id").isNotNull())
            .filter(col("user_id").isNotNull())
            
            # Type conversions
            .withColumn("timestamp", col("timestamp").cast("timestamp"))
            .withColumn("amount", col("amount").cast("double"))
            
            # Standardization
            .withColumn("event_type", upper(trim(col("event_type"))))
            
            # Deduplication
            .dropDuplicates(["event_id"])
            
            # Metadata
            .withColumn("processed_time", current_timestamp())
        )
        
        query = (
            silver_df.writeStream
            .format("delta")
            .option("checkpointLocation", f"{self.checkpoint_base}/silver")
            .outputMode("append")
            .table("silver.events")
        )
        
        return query
    
    def gold_layer(self):
        """Aggregate to Gold"""
        silver_stream = spark.readStream.table("silver.events")
        
        gold_df = (
            silver_stream
            .withWatermark("timestamp", "10 minutes")
            .groupBy(
                window(col("timestamp"), "1 hour"),
                col("event_type")
            )
            .agg(
                count("*").alias("event_count"),
                sum("amount").alias("total_amount"),
                countDistinct("user_id").alias("unique_users")
            )
        )
        
        query = (
            gold_df.writeStream
            .format("delta")
            .option("checkpointLocation", f"{self.checkpoint_base}/gold")
            .outputMode("append")
            .table("gold.hourly_metrics")
        )
        
        return query
    
    def start_all(self):
        """Start all layers"""
        bronze_query = self.bronze_layer()
        silver_query = self.silver_layer()
        gold_query = self.gold_layer()
        
        return [bronze_query, silver_query, gold_query]

# Usage
pipeline = MedallionPipeline("/raw/events/", "/checkpoints/medallion")
queries = pipeline.start_all()
```

### 6. Incremental Processing Across Layers

**Bronze → Silver (Incremental)**:

```python
# Process only new Bronze records
bronze_df = spark.read.table("bronze.events")

# Get max processed timestamp from Silver
max_processed = spark.sql("""
    SELECT COALESCE(MAX(ingestion_time), '1970-01-01') as max_time
    FROM silver.events
""").first()["max_time"]

# Process only new records
new_records = bronze_df.filter(col("ingestion_time") > max_processed)

# Transform and write to Silver
silver_df = transform_to_silver(new_records)
silver_df.write.format("delta").mode("append").saveAsTable("silver.events")
```

**Silver → Gold (Incremental with MERGE)**:

```python
from delta.tables import DeltaTable

# Read new Silver records
new_silver = spark.read.table("silver.events").filter(
    col("processed_time") > last_processed_time
)

# Aggregate
new_aggregates = (
    new_silver
    .groupBy("date", "event_type")
    .agg(
        count("*").alias("event_count"),
        sum("amount").alias("total_amount")
    )
)

# MERGE into Gold
gold_table = DeltaTable.forName(spark, "gold.daily_metrics")

(gold_table.alias("target")
    .merge(new_aggregates.alias("source"), 
           "target.date = source.date AND target.event_type = source.event_type")
    .whenMatchedUpdate(set = {
        "event_count": "target.event_count + source.event_count",
        "total_amount": "target.total_amount + source.total_amount"
    })
    .whenNotMatchedInsert(values = {
        "date": "source.date",
        "event_type": "source.event_type",
        "event_count": "source.event_count",
        "total_amount": "source.total_amount"
    })
    .execute()
)
```

### 7. Data Quality at Each Layer

**Bronze Quality Checks**:
```python
# Minimal checks - just ensure data arrived
bronze_quality = (
    spark.table("bronze.events")
    .agg(
        count("*").alias("total_records"),
        countDistinct("source_file").alias("unique_files"),
        min("ingestion_time").alias("first_ingestion"),
        max("ingestion_time").alias("last_ingestion")
    )
)
```

**Silver Quality Checks**:
```python
# Comprehensive validation
silver_quality = spark.sql("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT event_id) as unique_events,
        SUM(CASE WHEN event_id IS NULL THEN 1 ELSE 0 END) as null_event_ids,
        SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) as negative_amounts,
        SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) as null_timestamps,
        COUNT(*) - COUNT(DISTINCT event_id) as duplicate_count
    FROM silver.events
""")
```

**Gold Quality Checks**:
```python
# Business logic validation
gold_quality = spark.sql("""
    SELECT 
        date,
        SUM(event_count) as total_events,
        SUM(total_amount) as total_revenue,
        COUNT(DISTINCT event_type) as event_types
    FROM gold.daily_metrics
    GROUP BY date
    HAVING SUM(event_count) = 0  -- Alert if no events
""")
```

### 8. Layer Comparison

| Aspect | Bronze | Silver | Gold |
|--------|--------|--------|------|
| **Data State** | Raw | Cleaned | Aggregated |
| **Schema** | Flexible | Enforced | Optimized |
| **Quality** | None | Validated | Business rules |
| **Deduplication** | No | Yes | N/A |
| **Transformations** | Minimal | Extensive | Aggregations |
| **Users** | Data engineers | Data engineers | Analysts, BI |
| **Update Pattern** | Append | Append/Merge | Merge |
| **Optimization** | Minimal | Moderate | Heavy |

### 9. Common Patterns

**Pattern 1: Batch Multi-Hop**:
```python
# Bronze
bronze_df = spark.read.json("/raw/data/")
bronze_df.write.format("delta").mode("append").saveAsTable("bronze.data")

# Silver
silver_df = spark.read.table("bronze.data").transform(clean_and_validate)
silver_df.write.format("delta").mode("append").saveAsTable("silver.data")

# Gold
gold_df = spark.read.table("silver.data").transform(aggregate)
gold_df.write.format("delta").mode("overwrite").saveAsTable("gold.metrics")
```

**Pattern 2: Streaming Multi-Hop**:
```python
# All layers streaming
bronze_query = ingest_to_bronze()
silver_query = bronze_to_silver()
gold_query = silver_to_gold()

# Wait for all
spark.streams.awaitAnyTermination()
```

**Pattern 3: Mixed (Streaming Bronze/Silver, Batch Gold)**:
```python
# Streaming ingestion and cleaning
bronze_query = ingest_to_bronze()  # Streaming
silver_query = bronze_to_silver()  # Streaming

# Batch aggregation (scheduled)
def aggregate_to_gold():
    silver_df = spark.read.table("silver.events")
    gold_df = silver_df.groupBy("date").agg(...)
    gold_df.write.format("delta").mode("overwrite").saveAsTable("gold.daily")

# Run gold aggregation on schedule (e.g., hourly)
```

### 10. Best Practices

1. **Keep Bronze Raw**: Don't transform in Bronze, preserve original data
2. **Validate in Silver**: Apply all data quality checks in Silver layer
3. **Aggregate in Gold**: Create business-focused views in Gold
4. **Use Incremental Processing**: Process only new data at each layer
5. **Add Metadata**: Track lineage with timestamps and source info
6. **Partition Appropriately**: Bronze by date, Silver by business key, Gold by query pattern
7. **Document Transformations**: Clear documentation at each layer
8. **Monitor Quality**: Implement quality checks at each layer
9. **Optimize Gold**: Heavy optimization for query performance
10. **Version Control**: Track schema changes across layers

## Key Takeaways

1. **Medallion Architecture** has three layers: Bronze, Silver, Gold
2. **Bronze** stores raw data as-is from source
3. **Silver** contains cleaned, validated, conformed data
4. **Gold** has business-level aggregates and metrics
5. **Incremental processing** at each layer improves efficiency
6. **Data quality** improves from Bronze → Silver → Gold
7. **Each layer** has specific responsibilities and users
8. **Streaming or batch** can be used at any layer
9. **MERGE** is common for Gold layer updates
10. **Metadata** tracking is important for lineage

## Exam Tips

1. Know the three layers and their purposes
2. Understand Bronze = raw, Silver = cleaned, Gold = aggregated
3. Know when to use each layer
4. Understand incremental processing patterns
5. Know data quality checks at each layer
6. Understand streaming vs batch at each layer
7. Know how to implement multi-hop pipelines
8. Understand MERGE usage in Gold layer
9. Know partitioning strategies per layer
10. Understand metadata and lineage tracking

## Additional Resources

- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Multi-Hop Pipelines](https://docs.databricks.com/lakehouse/medallion.html)
- [Delta Lake Best Practices](https://docs.delta.io/latest/best-practices.html)

---

**Next**: Day 21 - Optimization Techniques
