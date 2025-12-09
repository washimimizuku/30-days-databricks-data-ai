# Day 14: ELT Best Practices

## Learning Objectives

By the end of today, you will:
- Understand the Medallion Architecture (Bronze, Silver, Gold)
- Implement incremental data processing patterns
- Apply data quality checks and validation
- Design idempotent and reusable pipelines
- Implement error handling and logging
- Optimize pipeline performance
- Follow naming conventions and documentation standards
- Build production-ready ELT workflows

## Topics Covered

### 1. Medallion Architecture

The Medallion Architecture is a data design pattern used to logically organize data in a lakehouse, with the goal of incrementally improving the structure and quality of data as it flows through each layer.

#### Architecture Layers

```
Raw Data → Bronze (Raw) → Silver (Cleaned) → Gold (Business-Level)
```

**Bronze Layer (Raw/Landing)**
- Stores raw data in its original format
- Minimal transformations (schema enforcement)
- Preserves complete history
- Append-only operations
- Includes metadata (ingestion timestamp, source file)

**Silver Layer (Cleaned/Conformed)**
- Cleaned and validated data
- Standardized formats and types
- Deduplicated records
- Enriched with business logic
- Ready for analytics

**Gold Layer (Business/Aggregated)**
- Business-level aggregations
- Denormalized for performance
- Optimized for specific use cases
- Serves BI tools and reports
- May include ML features

#### Bronze Layer Implementation

```python
from pyspark.sql.functions import current_timestamp, input_file_name

# Bronze: Ingest raw data with metadata
bronze_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/schemas/bronze")
    .load("/mnt/raw/orders/")
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
)

# Write to Bronze table
(bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/bronze_orders")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .table("bronze.orders")
)
```

#### Silver Layer Implementation

```python
from pyspark.sql.functions import col, to_timestamp, regexp_replace

# Silver: Clean and validate Bronze data
silver_df = (
    spark.readStream
    .table("bronze.orders")
    .filter(col("order_id").isNotNull())  # Remove nulls
    .dropDuplicates(["order_id"])  # Deduplicate
    .withColumn("order_date", to_timestamp(col("order_date")))  # Type conversion
    .withColumn("amount", col("amount").cast("decimal(10,2)"))  # Standardize
    .withColumn("email", regexp_replace(col("email"), " ", ""))  # Clean
    .filter(col("amount") > 0)  # Business rule validation
    .withColumn("processed_timestamp", current_timestamp())
)

# Write to Silver table
(silver_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/silver_orders")
    .outputMode("append")
    .trigger(availableNow=True)
    .table("silver.orders")
)
```

#### Gold Layer Implementation

```python
from pyspark.sql.functions import sum, count, avg, date_trunc

# Gold: Business-level aggregations
gold_df = (
    spark.readStream
    .table("silver.orders")
    .withWatermark("order_date", "1 day")
    .groupBy(
        date_trunc("day", col("order_date")).alias("order_day"),
        col("customer_id")
    )
    .agg(
        count("order_id").alias("total_orders"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_order_value")
    )
    .withColumn("aggregation_timestamp", current_timestamp())
)

# Write to Gold table
(gold_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/gold_daily_summary")
    .outputMode("complete")
    .trigger(availableNow=True)
    .table("gold.daily_customer_summary")
)
```

### 2. Incremental Processing Patterns

#### Pattern 1: Append-Only (Bronze)

```python
# Best for: Raw data ingestion, immutable logs
def ingest_to_bronze(source_path, table_name):
    """Append-only ingestion to Bronze layer"""
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(source_path)
        .withColumn("_ingestion_time", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )
    
    return (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", f"/checkpoints/{table_name}")
        .trigger(availableNow=True)
        .table(f"bronze.{table_name}")
    )
```

#### Pattern 2: Merge/Upsert (Silver)

```python
from delta.tables import DeltaTable

# Best for: Slowly changing dimensions, updates
def upsert_to_silver(source_table, target_table, key_columns):
    """Merge updates into Silver layer"""
    
    # Read new data
    updates_df = spark.read.table(f"bronze.{source_table}")
    
    # Get target table
    target = DeltaTable.forName(spark, f"silver.{target_table}")
    
    # Build merge condition
    merge_condition = " AND ".join([
        f"target.{col} = updates.{col}" for col in key_columns
    ])
    
    # Perform merge
    (target.alias("target")
        .merge(
            updates_df.alias("updates"),
            merge_condition
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
```

#### Pattern 3: Aggregation (Gold)

```python
# Best for: Business metrics, reporting tables
def aggregate_to_gold(source_table, target_table, group_cols, agg_exprs):
    """Aggregate data for Gold layer"""
    
    df = (
        spark.read
        .table(f"silver.{source_table}")
        .groupBy(*group_cols)
        .agg(*agg_exprs)
    )
    
    # Overwrite partition or full table
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"gold.{target_table}")
    )
```

### 3. Data Quality and Validation

#### Schema Validation

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Define expected schema
expected_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("amount", DoubleType(), False),
    StructField("order_date", StringType(), False)
])

# Validate schema
def validate_schema(df, expected_schema):
    """Validate DataFrame schema matches expected"""
    if df.schema != expected_schema:
        missing = set(expected_schema.fieldNames()) - set(df.columns)
        extra = set(df.columns) - set(expected_schema.fieldNames())
        raise ValueError(f"Schema mismatch. Missing: {missing}, Extra: {extra}")
    return df
```

#### Data Quality Checks

```python
from pyspark.sql.functions import col, count, when

def run_quality_checks(df, table_name):
    """Run comprehensive data quality checks"""
    
    total_rows = df.count()
    
    # Null checks
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c) 
        for c in df.columns
    ]).collect()[0].asDict()
    
    # Duplicate checks
    duplicate_count = df.count() - df.dropDuplicates().count()
    
    # Value range checks
    invalid_amounts = df.filter(col("amount") < 0).count()
    
    # Log results
    quality_report = {
        "table": table_name,
        "total_rows": total_rows,
        "null_counts": null_counts,
        "duplicates": duplicate_count,
        "invalid_amounts": invalid_amounts,
        "quality_score": calculate_quality_score(
            total_rows, null_counts, duplicate_count, invalid_amounts
        )
    }
    
    # Fail if quality is too low
    if quality_report["quality_score"] < 0.95:
        raise ValueError(f"Data quality below threshold: {quality_report}")
    
    return quality_report

def calculate_quality_score(total, nulls, dupes, invalid):
    """Calculate overall quality score (0-1)"""
    if total == 0:
        return 0
    
    null_penalty = sum(nulls.values()) / (total * len(nulls))
    dupe_penalty = dupes / total
    invalid_penalty = invalid / total
    
    return 1 - (null_penalty + dupe_penalty + invalid_penalty)
```

#### Constraint Validation

```python
# Add constraints to Delta tables
spark.sql("""
    ALTER TABLE silver.orders 
    ADD CONSTRAINT valid_amount CHECK (amount > 0)
""")

spark.sql("""
    ALTER TABLE silver.orders 
    ADD CONSTRAINT valid_email CHECK (email LIKE '%@%.%')
""")

# Check constraints
spark.sql("DESCRIBE DETAIL silver.orders").select("properties").show(truncate=False)
```

### 4. Idempotency and Reusability

#### Idempotent Pipeline Design

```python
from datetime import datetime

def process_daily_batch(processing_date):
    """Idempotent daily batch processing"""
    
    # Use date-based filtering for idempotency
    df = (
        spark.read
        .table("bronze.orders")
        .filter(col("ingestion_date") == processing_date)
    )
    
    # Process data
    processed_df = transform_data(df)
    
    # Overwrite specific partition (idempotent)
    (processed_df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"processing_date = '{processing_date}'")
        .saveAsTable("silver.orders")
    )
    
    return f"Processed {processed_df.count()} records for {processing_date}"
```

#### Reusable Transformation Functions

```python
# Create reusable transformation library
class DataTransformations:
    """Reusable data transformation functions"""
    
    @staticmethod
    def standardize_phone(df, col_name):
        """Standardize phone numbers to (XXX) XXX-XXXX format"""
        from pyspark.sql.functions import regexp_replace
        
        return df.withColumn(
            col_name,
            regexp_replace(
                regexp_replace(col(col_name), r"[^\d]", ""),
                r"(\d{3})(\d{3})(\d{4})",
                "($1) $2-$3"
            )
        )
    
    @staticmethod
    def standardize_email(df, col_name):
        """Standardize email addresses"""
        from pyspark.sql.functions import lower, trim
        
        return df.withColumn(
            col_name,
            lower(trim(col(col_name)))
        )
    
    @staticmethod
    def calculate_age(df, birthdate_col):
        """Calculate age from birthdate"""
        from pyspark.sql.functions import datediff, current_date
        
        return df.withColumn(
            "age",
            (datediff(current_date(), col(birthdate_col)) / 365).cast("int")
        )
    
    @staticmethod
    def add_audit_columns(df):
        """Add standard audit columns"""
        from pyspark.sql.functions import current_timestamp, current_user
        
        return (df
            .withColumn("created_at", current_timestamp())
            .withColumn("created_by", current_user())
        )

# Usage
transforms = DataTransformations()
df = transforms.standardize_phone(df, "phone")
df = transforms.standardize_email(df, "email")
df = transforms.add_audit_columns(df)
```

### 5. Error Handling and Logging

#### Comprehensive Error Handling

```python
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def safe_pipeline_execution(pipeline_func, *args, **kwargs):
    """Execute pipeline with error handling and logging"""
    
    pipeline_name = pipeline_func.__name__
    start_time = datetime.now()
    
    try:
        logger.info(f"Starting pipeline: {pipeline_name}")
        logger.info(f"Arguments: {args}, {kwargs}")
        
        # Execute pipeline
        result = pipeline_func(*args, **kwargs)
        
        # Log success
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Pipeline {pipeline_name} completed successfully")
        logger.info(f"Duration: {duration:.2f} seconds")
        
        # Write success metrics
        log_pipeline_metrics(pipeline_name, "SUCCESS", duration, result)
        
        return result
        
    except Exception as e:
        # Log failure
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Pipeline {pipeline_name} failed: {str(e)}")
        logger.error(f"Duration before failure: {duration:.2f} seconds")
        
        # Write failure metrics
        log_pipeline_metrics(pipeline_name, "FAILED", duration, str(e))
        
        # Re-raise exception
        raise

def log_pipeline_metrics(pipeline_name, status, duration, details):
    """Log pipeline execution metrics to Delta table"""
    
    metrics_df = spark.createDataFrame([{
        "pipeline_name": pipeline_name,
        "status": status,
        "duration_seconds": duration,
        "details": str(details),
        "execution_timestamp": datetime.now()
    }])
    
    (metrics_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("monitoring.pipeline_metrics")
    )
```

#### Dead Letter Queue Pattern

```python
def process_with_dlq(df, processing_func, dlq_table):
    """Process data with dead letter queue for failures"""
    
    from pyspark.sql.functions import struct, to_json
    
    # Add row identifier
    df_with_id = df.withColumn("_row_id", monotonically_increasing_id())
    
    # Try to process
    try:
        processed_df = processing_func(df_with_id)
        return processed_df.drop("_row_id")
        
    except Exception as e:
        # On failure, write problematic records to DLQ
        logger.error(f"Processing failed: {str(e)}")
        
        (df_with_id
            .withColumn("error_message", lit(str(e)))
            .withColumn("error_timestamp", current_timestamp())
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(dlq_table)
        )
        
        raise
```

### 6. Performance Optimization

#### Partitioning Strategy

```python
# Partition by date for time-series data
(df.write
    .format("delta")
    .partitionBy("order_date")
    .mode("overwrite")
    .saveAsTable("silver.orders")
)

# Multi-level partitioning
(df.write
    .format("delta")
    .partitionBy("year", "month", "day")
    .mode("overwrite")
    .saveAsTable("silver.orders_partitioned")
)
```

#### Z-Ordering

```python
# Optimize for common query patterns
spark.sql("""
    OPTIMIZE silver.orders
    ZORDER BY (customer_id, order_date)
""")
```

#### Caching Strategy

```python
# Cache frequently accessed data
df_customers = spark.read.table("silver.customers").cache()

# Use cached data multiple times
high_value = df_customers.filter(col("lifetime_value") > 10000)
recent_orders = df_customers.filter(col("last_order_date") > "2024-01-01")

# Unpersist when done
df_customers.unpersist()
```

#### Broadcast Joins

```python
from pyspark.sql.functions import broadcast

# Broadcast small dimension tables
df_orders = spark.read.table("silver.orders")
df_products = spark.read.table("silver.products")  # Small table

# Broadcast join
df_enriched = df_orders.join(
    broadcast(df_products),
    "product_id"
)
```

### 7. Naming Conventions and Documentation

#### Table Naming Standards

```python
# Layer.Domain.Entity_Type
# Examples:
# bronze.sales.orders_raw
# silver.sales.orders_cleaned
# gold.sales.daily_revenue
# gold.sales.customer_360

# Naming convention function
def get_table_name(layer, domain, entity, suffix=""):
    """Generate standardized table name"""
    parts = [layer, domain, entity]
    if suffix:
        parts.append(suffix)
    return ".".join(parts)

# Usage
table_name = get_table_name("silver", "sales", "orders", "cleaned")
# Returns: "silver.sales.orders_cleaned"
```

#### Column Naming Standards

```python
# Use snake_case for column names
# Prefix metadata columns with underscore
# Use descriptive names

def standardize_column_names(df):
    """Standardize DataFrame column names"""
    import re
    
    for col_name in df.columns:
        # Convert to snake_case
        new_name = re.sub(r'(?<!^)(?=[A-Z])', '_', col_name).lower()
        new_name = re.sub(r'[^a-z0-9_]', '_', new_name)
        new_name = re.sub(r'_+', '_', new_name)
        
        if col_name != new_name:
            df = df.withColumnRenamed(col_name, new_name)
    
    return df
```

#### Documentation Standards

```python
# Add table comments
spark.sql("""
    COMMENT ON TABLE silver.orders IS 
    'Cleaned and validated orders from bronze layer. 
    Updated daily via incremental processing.
    Owner: data-engineering@company.com'
""")

# Add column comments
spark.sql("""
    ALTER TABLE silver.orders 
    ALTER COLUMN order_id 
    COMMENT 'Unique order identifier from source system'
""")

# View table documentation
spark.sql("DESCRIBE EXTENDED silver.orders").show(truncate=False)
```

### 8. Complete ELT Pipeline Example

```python
from pyspark.sql.functions import *
from delta.tables import DeltaTable
import logging

class MedallionPipeline:
    """Complete Medallion Architecture ELT Pipeline"""
    
    def __init__(self, spark, source_path, checkpoint_base):
        self.spark = spark
        self.source_path = source_path
        self.checkpoint_base = checkpoint_base
        self.logger = logging.getLogger(__name__)
    
    def run_bronze_ingestion(self):
        """Ingest raw data to Bronze layer"""
        self.logger.info("Starting Bronze ingestion")
        
        df = (
            self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{self.checkpoint_base}/schema")
            .load(self.source_path)
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source_file", input_file_name())
        )
        
        query = (
            df.writeStream
            .format("delta")
            .option("checkpointLocation", f"{self.checkpoint_base}/bronze")
            .trigger(availableNow=True)
            .table("bronze.orders")
        )
        
        query.awaitTermination()
        self.logger.info("Bronze ingestion completed")
    
    def run_silver_transformation(self):
        """Transform Bronze to Silver layer"""
        self.logger.info("Starting Silver transformation")
        
        # Read from Bronze
        bronze_df = self.spark.read.table("bronze.orders")
        
        # Apply transformations
        silver_df = (
            bronze_df
            .filter(col("order_id").isNotNull())
            .dropDuplicates(["order_id"])
            .withColumn("order_date", to_timestamp(col("order_date")))
            .withColumn("amount", col("amount").cast("decimal(10,2)"))
            .filter(col("amount") > 0)
            .withColumn("_processed_timestamp", current_timestamp())
        )
        
        # Run quality checks
        self.validate_silver_data(silver_df)
        
        # Write to Silver
        (silver_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable("silver.orders")
        )
        
        self.logger.info(f"Silver transformation completed: {silver_df.count()} records")
    
    def run_gold_aggregation(self):
        """Aggregate Silver to Gold layer"""
        self.logger.info("Starting Gold aggregation")
        
        # Read from Silver
        silver_df = self.spark.read.table("silver.orders")
        
        # Create business-level aggregations
        gold_df = (
            silver_df
            .groupBy(
                date_trunc("day", col("order_date")).alias("order_day"),
                col("customer_id")
            )
            .agg(
                count("order_id").alias("total_orders"),
                sum("amount").alias("total_revenue"),
                avg("amount").alias("avg_order_value"),
                max("order_date").alias("last_order_date")
            )
            .withColumn("_aggregation_timestamp", current_timestamp())
        )
        
        # Write to Gold
        (gold_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable("gold.daily_customer_summary")
        )
        
        self.logger.info(f"Gold aggregation completed: {gold_df.count()} records")
    
    def validate_silver_data(self, df):
        """Validate Silver layer data quality"""
        total_rows = df.count()
        null_orders = df.filter(col("order_id").isNull()).count()
        invalid_amounts = df.filter(col("amount") <= 0).count()
        
        if null_orders > 0:
            raise ValueError(f"Found {null_orders} null order_ids")
        
        if invalid_amounts > 0:
            raise ValueError(f"Found {invalid_amounts} invalid amounts")
        
        self.logger.info(f"Data validation passed: {total_rows} valid records")
    
    def run_full_pipeline(self):
        """Execute complete pipeline"""
        try:
            self.run_bronze_ingestion()
            self.run_silver_transformation()
            self.run_gold_aggregation()
            self.logger.info("Full pipeline completed successfully")
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise

# Usage
pipeline = MedallionPipeline(
    spark=spark,
    source_path="/mnt/raw/orders/",
    checkpoint_base="/mnt/checkpoints/orders"
)

pipeline.run_full_pipeline()
```

## Best Practices Summary

### Design Principles
1. **Separation of Concerns**: Each layer has a specific purpose
2. **Idempotency**: Pipelines can be re-run safely
3. **Incremental Processing**: Process only new/changed data
4. **Data Quality**: Validate at every layer
5. **Error Handling**: Graceful failure and recovery
6. **Monitoring**: Track pipeline health and performance

### Performance Guidelines
1. Partition large tables by date or frequently filtered columns
2. Use Z-ordering for columns in WHERE clauses
3. Broadcast small dimension tables in joins
4. Cache intermediate results used multiple times
5. Optimize file sizes (target 128MB-1GB per file)
6. Run VACUUM regularly to remove old files

### Operational Guidelines
1. Use consistent naming conventions
2. Document tables and columns
3. Implement comprehensive logging
4. Set up alerting for failures
5. Version control all pipeline code
6. Test pipelines with sample data first

### Security Guidelines
1. Use Unity Catalog for access control
2. Encrypt data at rest and in transit
3. Mask sensitive data in lower environments
4. Audit data access patterns
5. Implement row-level security where needed

## Key Takeaways

1. **Medallion Architecture** provides a structured approach to data organization
2. **Bronze → Silver → Gold** represents increasing data quality and business value
3. **Idempotency** ensures pipelines can be safely re-run
4. **Data Quality** checks prevent bad data from propagating
5. **Error Handling** and logging are critical for production pipelines
6. **Performance optimization** through partitioning, Z-ordering, and caching
7. **Naming conventions** and documentation improve maintainability
8. **Reusable functions** reduce code duplication and errors

## Exam Tips

1. Know the purpose of each Medallion layer
2. Understand when to use append vs. merge vs. overwrite
3. Be familiar with data quality validation techniques
4. Know how to implement idempotent pipelines
5. Understand partitioning and optimization strategies
6. Be able to identify best practices in code examples
7. Know common error handling patterns
8. Understand the trade-offs between different approaches

## Additional Resources

- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Delta Lake Best Practices](https://docs.databricks.com/delta/best-practices.html)
- [Data Quality Patterns](https://www.databricks.com/blog/2020/07/06/data-quality-patterns.html)
- [Production Pipeline Patterns](https://www.databricks.com/blog/2021/05/19/production-pipeline-patterns.html)

---

**Next**: Day 15 - Week 2 Review & ELT Project
