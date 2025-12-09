# Day 14: ELT Best Practices - Solutions

"""
Complete solutions for Day 14 exercises demonstrating production-ready
ELT pipelines using the Medallion Architecture pattern.
"""

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# PART 1: BRONZE LAYER - RAW DATA INGESTION
# ============================================================================

# Exercise 1: Create Bronze table from raw JSON files
bronze_df = spark.read.json("/tmp/raw_data/orders/")
print(f"Loaded {bronze_df.count()} records from raw data")

# Exercise 2: Add source file tracking
bronze_df = bronze_df.withColumn("_source_file", input_file_name())

# Exercise 3: Add ingestion timestamp
bronze_df = bronze_df.withColumn("_ingestion_timestamp", current_timestamp())

# Exercise 4: Write to Bronze table
(bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("bronze.orders")
)
print("✓ Bronze table created")

# Exercise 5: Verify Bronze table
bronze_count = spark.table("bronze.orders").count()
print(f"Bronze table contains {bronze_count} records")
spark.table("bronze.orders").show(5)

# Exercise 6: Check Bronze table properties
spark.sql("DESCRIBE EXTENDED bronze.orders").show(truncate=False)

# Exercise 7: Implement incremental Bronze ingestion
def ingest_bronze_streaming():
    """Incremental ingestion using Auto Loader"""
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/tmp/checkpoints/schema_location")
        .load("/tmp/raw_data/orders/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )
    
    query = (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", "/tmp/checkpoints/bronze_orders")
        .trigger(availableNow=True)
        .table("bronze.orders")
    )
    
    query.awaitTermination()
    return query

# Exercise 8: Add schema evolution to Bronze
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
print("✓ Schema evolution enabled")

# Exercise 9: Create Bronze ingestion function
def ingest_to_bronze(source_path, table_name, checkpoint_path):
    """Reusable Bronze layer ingestion function"""
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .load(source_path)
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )
    
    query = (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .table(f"bronze.{table_name}")
    )
    
    query.awaitTermination()
    logger.info(f"Bronze ingestion completed for {table_name}")
    return query

# Exercise 10: Test Bronze ingestion with error handling
def safe_bronze_ingestion(source_path, table_name, checkpoint_path):
    """Bronze ingestion with error handling"""
    try:
        logger.info(f"Starting Bronze ingestion for {table_name}")
        start_time = datetime.now()
        
        query = ingest_to_bronze(source_path, table_name, checkpoint_path)
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Bronze ingestion completed in {duration:.2f} seconds")
        
        return {"status": "SUCCESS", "duration": duration}
        
    except Exception as e:
        logger.error(f"Bronze ingestion failed: {str(e)}")
        return {"status": "FAILED", "error": str(e)}

print("✓ Bronze layer functions created")

# ============================================================================
# PART 2: SILVER LAYER - DATA CLEANING
# ============================================================================

# Exercise 11: Read from Bronze table
bronze_df = spark.table("bronze.orders")
print(f"Read {bronze_df.count()} records from Bronze")

# Exercise 12: Remove null order_ids
silver_df = bronze_df.filter(col("order_id").isNotNull())
print(f"After removing null order_ids: {silver_df.count()} records")

# Exercise 13: Remove null customer_ids
silver_df = silver_df.filter(col("customer_id").isNotNull())
print(f"After removing null customer_ids: {silver_df.count()} records")

# Exercise 14: Remove duplicate orders
silver_df = silver_df.dropDuplicates(["order_id"])
print(f"After deduplication: {silver_df.count()} records")

# Exercise 15: Standardize date format
silver_df = silver_df.withColumn(
    "order_date",
    coalesce(
        to_timestamp(col("order_date"), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col("order_date"), "MM/dd/yyyy"),
        to_timestamp(col("order_date"), "yyyy-MM-dd")
    )
)
print("✓ Date format standardized")

# Exercise 16: Validate and fix amounts
silver_df = silver_df.filter(col("amount") > 0)
print(f"After removing invalid amounts: {silver_df.count()} records")

# Exercise 17: Standardize email addresses
silver_df = silver_df.withColumn(
    "email",
    lower(trim(col("email")))
)

# Exercise 18: Validate email format
silver_df = silver_df.filter(
    col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
)
print(f"After email validation: {silver_df.count()} records")

# Exercise 19: Standardize status values
silver_df = silver_df.withColumn(
    "status",
    lower(trim(col("status")))
)

# Exercise 20: Add audit columns
silver_df = (silver_df
    .withColumn("_created_at", current_timestamp())
    .withColumn("_created_by", current_user())
)

# Exercise 21: Calculate derived columns
silver_df = silver_df.withColumn(
    "total_amount",
    col("quantity") * col("amount")
)

# Exercise 22: Write to Silver table
(silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.orders")
)
print(f"✓ Silver table created with {silver_df.count()} records")

# Exercise 23: Implement Silver transformation function
def transform_to_silver(bronze_table, silver_table):
    """Transform Bronze data to Silver layer"""
    
    # Read Bronze
    df = spark.table(bronze_table)
    
    # Apply transformations
    silver_df = (df
        # Remove nulls
        .filter(col("order_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        
        # Deduplicate
        .dropDuplicates(["order_id"])
        
        # Standardize date
        .withColumn("order_date", coalesce(
            to_timestamp(col("order_date"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("order_date"), "MM/dd/yyyy")
        ))
        
        # Validate amounts
        .filter(col("amount") > 0)
        
        # Standardize strings
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("status", lower(trim(col("status"))))
        
        # Validate email
        .filter(col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"))
        
        # Add derived columns
        .withColumn("total_amount", col("quantity") * col("amount"))
        
        # Add audit columns
        .withColumn("_created_at", current_timestamp())
        .withColumn("_created_by", current_user())
    )
    
    # Write to Silver
    (silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_table)
    )
    
    logger.info(f"Silver transformation completed: {silver_df.count()} records")
    return silver_df

# Exercise 24: Add data quality checks to Silver
def check_silver_quality():
    """Run quality checks on Silver layer"""
    df = spark.table("silver.orders")
    total = df.count()
    
    checks = [
        ("no_null_order_ids", df.filter(col("order_id").isNotNull()).count() == total),
        ("no_null_customer_ids", df.filter(col("customer_id").isNotNull()).count() == total),
        ("valid_amounts", df.filter(col("amount") > 0).count() == total),
        ("valid_emails", df.filter(col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")).count() == total),
        ("no_duplicates", df.count() == df.dropDuplicates(["order_id"]).count())
    ]
    
    results = []
    for check_name, passed in checks:
        result = {
            "table_name": "silver.orders",
            "check_name": check_name,
            "check_result": "PASS" if passed else "FAIL",
            "metric_value": 1.0 if passed else 0.0,
            "check_timestamp": datetime.now()
        }
        results.append(result)
        print(f"{check_name}: {'✓ PASS' if passed else '✗ FAIL'}")
    
    # Log to monitoring table
    results_df = spark.createDataFrame(results)
    (results_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("monitoring.data_quality_metrics")
    )
    
    return results

quality_results = check_silver_quality()

# Exercise 25: Implement MERGE for Silver updates
def merge_to_silver(updates_df):
    """MERGE updates into Silver layer"""
    
    # Get target table
    target = DeltaTable.forName(spark, "silver.orders")
    
    # Perform merge
    (target.alias("target")
        .merge(
            updates_df.alias("updates"),
            "target.order_id = updates.order_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    
    logger.info("Silver MERGE completed")

print("✓ Silver layer functions created")

# ============================================================================
# PART 3: GOLD LAYER - BUSINESS AGGREGATIONS
# ============================================================================

# Exercise 26: Create daily order summary
daily_summary = (
    spark.table("silver.orders")
    .groupBy(date_trunc("day", col("order_date")).alias("order_day"))
    .agg(
        count("order_id").alias("total_orders"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value"),
        sum("quantity").alias("total_quantity")
    )
)
daily_summary.show()

# Exercise 27: Create customer summary
customer_summary = (
    spark.table("silver.orders")
    .groupBy("customer_id")
    .agg(
        count("order_id").alias("total_orders"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value"),
        max("order_date").alias("last_order_date"),
        min("order_date").alias("first_order_date")
    )
)
customer_summary.show()

# Exercise 28: Create product summary
product_summary = (
    spark.table("silver.orders")
    .groupBy("product_id")
    .agg(
        count("order_id").alias("total_orders"),
        sum("quantity").alias("total_quantity_sold"),
        sum("amount").alias("total_revenue")
    )
)
product_summary.show()

# Exercise 29: Create daily customer summary
daily_customer_summary = (
    spark.table("silver.orders")
    .groupBy(
        date_trunc("day", col("order_date")).alias("order_day"),
        col("customer_id")
    )
    .agg(
        count("order_id").alias("total_orders"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value")
    )
    .withColumn("_aggregation_timestamp", current_timestamp())
)

# Exercise 30: Join with customer dimension
enriched_customer = (
    daily_customer_summary
    .join(
        spark.table("silver.customers"),
        "customer_id",
        "left"
    )
    .select(
        "order_day",
        "customer_id",
        "customer_name",
        "segment",
        "total_orders",
        "total_revenue",
        "avg_order_value"
    )
)
enriched_customer.show()

# Exercise 31: Join with product dimension
enriched_product = (
    spark.table("silver.orders")
    .join(
        spark.table("silver.products"),
        "product_id",
        "left"
    )
    .select(
        "order_id",
        "product_id",
        "product_name",
        "category",
        "quantity",
        "amount"
    )
)
enriched_product.show()

# Exercise 32: Create customer 360 view
customer_360 = (
    spark.table("silver.customers")
    .join(
        customer_summary,
        "customer_id",
        "left"
    )
    .select(
        "customer_id",
        "customer_name",
        "email",
        "segment",
        "lifetime_value",
        "registration_date",
        "total_orders",
        "total_revenue",
        "avg_order_value",
        "last_order_date",
        "first_order_date"
    )
)

# Exercise 33: Write to Gold table
(daily_customer_summary.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold.daily_customer_summary")
)
print(f"✓ Gold table created with {daily_customer_summary.count()} records")

# Exercise 34: Create Gold aggregation function
def aggregate_to_gold(silver_table, gold_table, group_cols, agg_exprs):
    """Aggregate Silver data to Gold layer"""
    
    df = (
        spark.table(silver_table)
        .groupBy(*group_cols)
        .agg(*agg_exprs)
        .withColumn("_aggregation_timestamp", current_timestamp())
    )
    
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(gold_table)
    )
    
    logger.info(f"Gold aggregation completed: {df.count()} records")
    return df

# Example usage
gold_df = aggregate_to_gold(
    "silver.orders",
    "gold.daily_summary",
    [date_trunc("day", col("order_date")).alias("order_day")],
    [
        count("order_id").alias("total_orders"),
        sum("amount").alias("total_revenue")
    ]
)

# Exercise 35: Optimize Gold table
spark.sql("OPTIMIZE gold.daily_customer_summary")
spark.sql("OPTIMIZE gold.daily_customer_summary ZORDER BY (customer_id, order_day)")
print("✓ Gold table optimized")

# ============================================================================
# PART 4: DATA QUALITY AND VALIDATION
# ============================================================================

# Exercise 36: Create schema validation function
def validate_schema(df, expected_schema):
    """Validate DataFrame schema matches expected"""
    if df.schema != expected_schema:
        missing = set(expected_schema.fieldNames()) - set(df.columns)
        extra = set(df.columns) - set(expected_schema.fieldNames())
        raise ValueError(f"Schema mismatch. Missing: {missing}, Extra: {extra}")
    return True

# Exercise 37: Implement null check
def check_nulls(df):
    """Count null values in each column"""
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c) 
        for c in df.columns
    ]).collect()[0].asDict()
    
    return null_counts

nulls = check_nulls(spark.table("silver.orders"))
print(f"Null counts: {nulls}")

# Exercise 38: Implement duplicate check
def check_duplicates(df, key_columns):
    """Count duplicate records"""
    total = df.count()
    unique = df.dropDuplicates(key_columns).count()
    duplicates = total - unique
    
    return {
        "total_records": total,
        "unique_records": unique,
        "duplicate_count": duplicates
    }

dupes = check_duplicates(spark.table("silver.orders"), ["order_id"])
print(f"Duplicate check: {dupes}")

# Exercise 39: Implement value range check
def check_value_ranges(df, column, min_val, max_val):
    """Check if values are within expected range"""
    total = df.count()
    valid = df.filter((col(column) >= min_val) & (col(column) <= max_val)).count()
    invalid = total - valid
    
    return {
        "column": column,
        "total": total,
        "valid": valid,
        "invalid": invalid,
        "pass_rate": valid / total if total > 0 else 0
    }

range_check = check_value_ranges(spark.table("silver.orders"), "amount", 0, 10000)
print(f"Range check: {range_check}")

# Exercise 40: Implement referential integrity check
def check_referential_integrity(fact_df, dim_df, join_key):
    """Verify all foreign keys exist in dimension"""
    orphans = (
        fact_df
        .select(join_key)
        .distinct()
        .join(dim_df.select(join_key), join_key, "left_anti")
        .count()
    )
    
    return {
        "join_key": join_key,
        "orphan_count": orphans,
        "is_valid": orphans == 0
    }

ref_check = check_referential_integrity(
    spark.table("silver.orders"),
    spark.table("silver.customers"),
    "customer_id"
)
print(f"Referential integrity: {ref_check}")

# Exercise 41: Calculate data quality score
def calculate_quality_score(df, checks):
    """Calculate overall quality score (0-1)"""
    total_checks = len(checks)
    passed_checks = sum(1 for check in checks if check["passed"])
    
    return passed_checks / total_checks if total_checks > 0 else 0

# Exercise 42: Log quality metrics
def log_quality_metrics(table_name, check_name, check_result, metric_value):
    """Write quality metrics to monitoring table"""
    metrics_df = spark.createDataFrame([{
        "table_name": table_name,
        "check_name": check_name,
        "check_result": check_result,
        "metric_value": metric_value,
        "check_timestamp": datetime.now()
    }])
    
    (metrics_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("monitoring.data_quality_metrics")
    )

# Exercise 43: Implement comprehensive quality check function
def run_quality_checks(df, table_name, checks):
    """Run comprehensive data quality checks"""
    results = []
    
    for check_name, check_expr in checks:
        passing = df.filter(check_expr).count()
        total = df.count()
        pass_rate = passing / total if total > 0 else 0
        
        result = {
            "table_name": table_name,
            "check_name": check_name,
            "check_result": "PASS" if pass_rate >= 0.95 else "FAIL",
            "metric_value": pass_rate,
            "check_timestamp": datetime.now(),
            "passed": pass_rate >= 0.95
        }
        
        results.append(result)
        print(f"{check_name}: {pass_rate:.2%} ({'PASS' if pass_rate >= 0.95 else 'FAIL'})")
    
    # Log results
    results_df = spark.createDataFrame(results)
    (results_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("monitoring.data_quality_metrics")
    )
    
    return results

# Run quality checks on Silver
silver_checks = [
    ("no_null_order_ids", col("order_id").isNotNull()),
    ("no_null_customer_ids", col("customer_id").isNotNull()),
    ("valid_amounts", col("amount") > 0),
    ("valid_emails", col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"))
]

quality_results = run_quality_checks(
    spark.table("silver.orders"),
    "silver.orders",
    silver_checks
)

# Exercise 44: Add table constraints
spark.sql("""
    ALTER TABLE silver.orders 
    ADD CONSTRAINT valid_amount CHECK (amount > 0)
""")

spark.sql("""
    ALTER TABLE silver.orders 
    ADD CONSTRAINT valid_quantity CHECK (quantity > 0)
""")

print("✓ Constraints added")

# Exercise 45: Verify constraints
constraints = spark.sql("DESCRIBE DETAIL silver.orders").select("properties").collect()
print(f"Table constraints: {constraints}")

print("✓ Data quality functions created")

# ============================================================================
# PART 5: PIPELINE ORCHESTRATION
# ============================================================================

# Exercise 46: Create pipeline execution logger
def log_pipeline_execution(pipeline_name, status, duration, details=""):
    """Log pipeline execution metrics"""
    metrics_df = spark.createDataFrame([{
        "pipeline_name": pipeline_name,
        "status": status,
        "duration_seconds": duration,
        "details": details,
        "execution_timestamp": datetime.now()
    }])
    
    (metrics_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("monitoring.pipeline_metrics")
    )
    
    logger.info(f"Pipeline {pipeline_name}: {status} ({duration:.2f}s)")

# Exercise 47: Implement error handling wrapper
def safe_pipeline_execution(pipeline_func, *args, **kwargs):
    """Execute pipeline with error handling and logging"""
    
    pipeline_name = pipeline_func.__name__
    start_time = datetime.now()
    
    try:
        logger.info(f"Starting pipeline: {pipeline_name}")
        
        # Execute pipeline
        result = pipeline_func(*args, **kwargs)
        
        # Log success
        duration = (datetime.now() - start_time).total_seconds()
        log_pipeline_execution(pipeline_name, "SUCCESS", duration, str(result))
        
        return result
        
    except Exception as e:
        # Log failure
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Pipeline {pipeline_name} failed: {str(e)}")
        log_pipeline_execution(pipeline_name, "FAILED", duration, str(e))
        
        raise

# Exercise 48: Create dead letter queue handler
def write_to_dlq(failed_records, source_table, error_message):
    """Write failed records to dead letter queue"""
    
    dlq_df = failed_records.withColumn(
        "source_table", lit(source_table)
    ).withColumn(
        "error_message", lit(error_message)
    ).withColumn(
        "error_timestamp", current_timestamp()
    ).withColumn(
        "failed_record", to_json(struct("*"))
    )
    
    (dlq_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("monitoring.dead_letter_queue")
    )
    
    logger.warning(f"Wrote {failed_records.count()} records to DLQ")

# Exercise 49: Implement retry logic
def retry_pipeline(pipeline_func, max_retries=3, delay=5):
    """Execute pipeline with retry logic"""
    import time
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt {attempt + 1} of {max_retries}")
            result = pipeline_func()
            logger.info("Pipeline succeeded")
            return result
            
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {str(e)}")
            
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error("Max retries exceeded")
                raise

# Exercise 50: Create complete pipeline class
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
        count = self.spark.table("bronze.orders").count()
        self.logger.info(f"Bronze ingestion completed: {count} records")
        return count
    
    def run_silver_transformation(self):
        """Transform Bronze to Silver layer"""
        self.logger.info("Starting Silver transformation")
        
        # Read from Bronze
        bronze_df = self.spark.table("bronze.orders")
        
        # Apply transformations
        silver_df = (
            bronze_df
            .filter(col("order_id").isNotNull())
            .filter(col("customer_id").isNotNull())
            .dropDuplicates(["order_id"])
            .withColumn("order_date", coalesce(
                to_timestamp(col("order_date"), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(col("order_date"), "MM/dd/yyyy")
            ))
            .filter(col("amount") > 0)
            .withColumn("email", lower(trim(col("email"))))
            .withColumn("status", lower(trim(col("status"))))
            .filter(col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"))
            .withColumn("total_amount", col("quantity") * col("amount"))
            .withColumn("_created_at", current_timestamp())
            .withColumn("_created_by", current_user())
        )
        
        # Write to Silver
        (silver_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable("silver.orders")
        )
        
        count = silver_df.count()
        self.logger.info(f"Silver transformation completed: {count} records")
        return count
    
    def run_gold_aggregation(self):
        """Aggregate Silver to Gold layer"""
        self.logger.info("Starting Gold aggregation")
        
        # Read from Silver
        silver_df = self.spark.table("silver.orders")
        
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
        
        count = gold_df.count()
        self.logger.info(f"Gold aggregation completed: {count} records")
        return count
    
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
            bronze_count = self.run_bronze_ingestion()
            silver_count = self.run_silver_transformation()
            gold_count = self.run_gold_aggregation()
            
            self.logger.info("Full pipeline completed successfully")
            return {
                "bronze_records": bronze_count,
                "silver_records": silver_count,
                "gold_records": gold_count
            }
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise

# Create and run pipeline
pipeline = MedallionPipeline(
    spark=spark,
    source_path="/tmp/raw_data/orders/",
    checkpoint_base="/tmp/checkpoints/orders"
)

# Exercise 51: Add pipeline monitoring
@safe_pipeline_execution
def monitored_pipeline():
    """Pipeline with monitoring"""
    return pipeline.run_full_pipeline()

# Run monitored pipeline
# result = monitored_pipeline()
# print(f"Pipeline result: {result}")

# Exercise 52: Implement idempotent daily batch
def process_daily_batch(processing_date):
    """Idempotent daily batch processing"""
    
    # Read data for specific date
    df = (
        spark.table("bronze.orders")
        .filter(col("_ingestion_timestamp").cast("date") == processing_date)
    )
    
    # Process data
    processed_df = transform_to_silver("bronze.orders", "silver.orders_temp")
    
    # Overwrite specific partition (idempotent)
    (processed_df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"order_date >= '{processing_date}' AND order_date < '{processing_date}' + INTERVAL 1 DAY")
        .saveAsTable("silver.orders")
    )
    
    logger.info(f"Processed batch for {processing_date}")
    return processed_df.count()

# Exercise 53: Add checkpoint management
def list_checkpoints(checkpoint_base):
    """List all checkpoint directories"""
    try:
        checkpoints = dbutils.fs.ls(checkpoint_base)
        return [cp.path for cp in checkpoints]
    except:
        return []

def clear_checkpoints(checkpoint_path):
    """Clear checkpoint directory"""
    try:
        dbutils.fs.rm(checkpoint_path, True)
        logger.info(f"Cleared checkpoint: {checkpoint_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to clear checkpoint: {str(e)}")
        return False

# Exercise 54: Implement pipeline validation
def validate_pipeline_config(config):
    """Validate pipeline configuration"""
    required_keys = ["source_path", "checkpoint_base", "bronze_table", "silver_table", "gold_table"]
    
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required config key: {key}")
    
    # Validate paths exist
    try:
        dbutils.fs.ls(config["source_path"])
    except:
        raise ValueError(f"Source path does not exist: {config['source_path']}")
    
    logger.info("Pipeline configuration validated")
    return True

# Exercise 55: Create pipeline status dashboard
def get_pipeline_status():
    """Query pipeline health metrics"""
    
    # Recent pipeline executions
    recent_runs = spark.sql("""
        SELECT 
            pipeline_name,
            status,
            COUNT(*) as run_count,
            AVG(duration_seconds) as avg_duration,
            MAX(execution_timestamp) as last_run
        FROM monitoring.pipeline_metrics
        WHERE execution_timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
        GROUP BY pipeline_name, status
        ORDER BY last_run DESC
    """)
    
    # Data quality summary
    quality_summary = spark.sql("""
        SELECT 
            table_name,
            check_name,
            check_result,
            AVG(metric_value) as avg_pass_rate,
            MAX(check_timestamp) as last_check
        FROM monitoring.data_quality_metrics
        WHERE check_timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
        GROUP BY table_name, check_name, check_result
        ORDER BY last_check DESC
    """)
    
    # Dead letter queue count
    dlq_count = spark.sql("""
        SELECT 
            source_table,
            COUNT(*) as failed_record_count,
            MAX(error_timestamp) as last_failure
        FROM monitoring.dead_letter_queue
        WHERE error_timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
        GROUP BY source_table
    """)
    
    print("=== Pipeline Status Dashboard ===\n")
    print("Recent Pipeline Runs:")
    recent_runs.show()
    
    print("\nData Quality Summary:")
    quality_summary.show()
    
    print("\nDead Letter Queue:")
    dlq_count.show()
    
    return {
        "recent_runs": recent_runs,
        "quality_summary": quality_summary,
        "dlq_count": dlq_count
    }

# Get pipeline status
status = get_pipeline_status()

print("✓ Pipeline orchestration functions created")

# ============================================================================
# PART 6: PERFORMANCE OPTIMIZATION
# ============================================================================

# Exercise 56: Partition Silver table by date
silver_df = spark.table("silver.orders")
(silver_df.write
    .format("delta")
    .partitionBy("order_date")
    .mode("overwrite")
    .saveAsTable("silver.orders_partitioned")
)
print("✓ Table partitioned by date")

# Exercise 57: Apply Z-ordering
spark.sql("OPTIMIZE silver.orders ZORDER BY (customer_id, order_date)")
print("✓ Z-ordering applied")

# Exercise 58: Optimize file sizes
spark.sql("OPTIMIZE silver.orders")
print("✓ File sizes optimized")

# Exercise 59: Implement broadcast join
from pyspark.sql.functions import broadcast

orders_df = spark.table("silver.orders")
products_df = spark.table("silver.products")

# Broadcast small dimension table
enriched_df = orders_df.join(
    broadcast(products_df),
    "product_id"
)
print(f"✓ Broadcast join completed: {enriched_df.count()} records")

# Exercise 60: Cache frequently used data
customers_df = spark.table("silver.customers").cache()

# Use cached data multiple times
high_value = customers_df.filter(col("lifetime_value") > 10000)
premium_segment = customers_df.filter(col("segment") == "Premium")

print(f"High value customers: {high_value.count()}")
print(f"Premium customers: {premium_segment.count()}")

# Unpersist when done
customers_df.unpersist()
print("✓ Cache management completed")

# Exercise 61: Analyze table statistics
spark.sql("ANALYZE TABLE silver.orders COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE silver.orders COMPUTE STATISTICS FOR ALL COLUMNS")
print("✓ Table statistics updated")

# Exercise 62: Check file sizes
file_stats = spark.sql("""
    DESCRIBE DETAIL silver.orders
""").select("numFiles", "sizeInBytes")

file_stats.show()
print("✓ File statistics retrieved")

# Exercise 63: Implement incremental aggregation
def incremental_gold_update(start_date, end_date):
    """Update Gold table incrementally"""
    
    # Read only new data from Silver
    new_data = (
        spark.table("silver.orders")
        .filter(col("order_date").between(start_date, end_date))
    )
    
    # Aggregate new data
    new_aggregates = (
        new_data
        .groupBy(
            date_trunc("day", col("order_date")).alias("order_day"),
            col("customer_id")
        )
        .agg(
            count("order_id").alias("total_orders"),
            sum("amount").alias("total_revenue")
        )
    )
    
    # Merge into Gold table
    target = DeltaTable.forName(spark, "gold.daily_customer_summary")
    
    (target.alias("target")
        .merge(
            new_aggregates.alias("updates"),
            "target.order_day = updates.order_day AND target.customer_id = updates.customer_id"
        )
        .whenMatchedUpdate(set={
            "total_orders": col("target.total_orders") + col("updates.total_orders"),
            "total_revenue": col("target.total_revenue") + col("updates.total_revenue")
        })
        .whenNotMatchedInsertAll()
        .execute()
    )
    
    logger.info(f"Incremental update completed for {start_date} to {end_date}")

# Exercise 64: Add partition pruning
# Query with partition filter
pruned_query = spark.sql("""
    SELECT *
    FROM silver.orders_partitioned
    WHERE order_date >= '2024-01-01' AND order_date < '2024-02-01'
""")

print(f"✓ Partition pruning query: {pruned_query.count()} records")

# Exercise 65: Measure query performance
import time

# Before optimization
start = time.time()
result1 = spark.table("silver.orders").filter(col("customer_id") == 100).count()
duration_before = time.time() - start

# After optimization (with Z-ordering)
spark.sql("OPTIMIZE silver.orders ZORDER BY (customer_id)")

start = time.time()
result2 = spark.table("silver.orders").filter(col("customer_id") == 100).count()
duration_after = time.time() - start

print(f"Query performance:")
print(f"  Before optimization: {duration_before:.3f}s")
print(f"  After optimization: {duration_after:.3f}s")
print(f"  Improvement: {((duration_before - duration_after) / duration_before * 100):.1f}%")

print("✓ Performance optimization completed")

# ============================================================================
# PART 7: NAMING AND DOCUMENTATION
# ============================================================================

# Exercise 66: Standardize column names
def standardize_column_names(df):
    """Convert all column names to snake_case"""
    import re
    
    for col_name in df.columns:
        # Convert to snake_case
        new_name = re.sub(r'(?<!^)(?=[A-Z])', '_', col_name).lower()
        new_name = re.sub(r'[^a-z0-9_]', '_', new_name)
        new_name = re.sub(r'_+', '_', new_name)
        
        if col_name != new_name:
            df = df.withColumnRenamed(col_name, new_name)
    
    return df

# Apply to a DataFrame
standardized_df = standardize_column_names(spark.table("silver.orders"))
print("✓ Column names standardized")

# Exercise 67: Add table comments
spark.sql("""
    COMMENT ON TABLE bronze.orders IS 
    'Raw orders data from source system. Append-only with ingestion metadata.'
""")

spark.sql("""
    COMMENT ON TABLE silver.orders IS 
    'Cleaned and validated orders. Updated daily via incremental processing.'
""")

spark.sql("""
    COMMENT ON TABLE gold.daily_customer_summary IS 
    'Daily customer order aggregations for reporting and analytics.'
""")

print("✓ Table comments added")

# Exercise 68: Add column comments
spark.sql("""
    ALTER TABLE silver.orders 
    ALTER COLUMN order_id COMMENT 'Unique order identifier from source system'
""")

spark.sql("""
    ALTER TABLE silver.orders 
    ALTER COLUMN customer_id COMMENT 'Foreign key to customers dimension'
""")

spark.sql("""
    ALTER TABLE silver.orders 
    ALTER COLUMN amount COMMENT 'Order amount in USD'
""")

print("✓ Column comments added")

# Exercise 69: Create data dictionary
def generate_data_dictionary(table_name):
    """Generate data dictionary from table metadata"""
    
    # Get table description
    desc = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()
    
    dictionary = []
    for row in desc:
        if row.col_name and not row.col_name.startswith("#"):
            dictionary.append({
                "table_name": table_name,
                "column_name": row.col_name,
                "data_type": row.data_type,
                "comment": row.comment if hasattr(row, 'comment') else ""
            })
    
    dict_df = spark.createDataFrame(dictionary)
    return dict_df

# Generate dictionaries
bronze_dict = generate_data_dictionary("bronze.orders")
silver_dict = generate_data_dictionary("silver.orders")
gold_dict = generate_data_dictionary("gold.daily_customer_summary")

print("✓ Data dictionaries generated")
bronze_dict.show(truncate=False)

# Exercise 70: Document pipeline lineage
pipeline_lineage = """
# Data Pipeline Lineage

## Bronze Layer
- **Source**: /tmp/raw_data/orders/ (JSON files)
- **Table**: bronze.orders
- **Process**: Auto Loader incremental ingestion
- **Frequency**: Continuous (triggered)
- **Transformations**: Add metadata columns only

## Silver Layer
- **Source**: bronze.orders
- **Table**: silver.orders
- **Process**: Data cleaning and validation
- **Frequency**: Daily batch
- **Transformations**:
  - Remove nulls and duplicates
  - Standardize formats
  - Validate data quality
  - Add derived columns

## Gold Layer
- **Source**: silver.orders
- **Tables**: 
  - gold.daily_customer_summary
  - gold.customer_360
- **Process**: Business aggregations
- **Frequency**: Daily batch
- **Transformations**:
  - Group by date and customer
  - Calculate metrics
  - Join with dimensions
"""

print(pipeline_lineage)
print("✓ Pipeline lineage documented")

# ============================================================================
# BONUS CHALLENGES
# ============================================================================

# Bonus 1: Implement SCD Type 2
def implement_scd_type2(updates_df, target_table, key_columns):
    """Implement Slowly Changing Dimension Type 2"""
    
    target = DeltaTable.forName(spark, target_table)
    
    # Add SCD columns to updates
    updates_with_scd = (
        updates_df
        .withColumn("effective_date", current_date())
        .withColumn("end_date", lit(None).cast("date"))
        .withColumn("is_current", lit(True))
    )
    
    # Build merge condition
    merge_condition = " AND ".join([
        f"target.{col} = updates.{col}" for col in key_columns
    ])
    merge_condition += " AND target.is_current = true"
    
    # Perform SCD Type 2 merge
    (target.alias("target")
        .merge(updates_with_scd.alias("updates"), merge_condition)
        .whenMatchedUpdate(
            condition="target.is_current = true",
            set={
                "end_date": current_date(),
                "is_current": lit(False)
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    
    logger.info("SCD Type 2 update completed")

print("✓ SCD Type 2 implementation created")

# Bonus 2: Create data quality dashboard
def create_quality_dashboard():
    """Build comprehensive quality metrics dashboard"""
    
    dashboard = spark.sql("""
        WITH latest_checks AS (
            SELECT 
                table_name,
                check_name,
                check_result,
                metric_value,
                check_timestamp,
                ROW_NUMBER() OVER (PARTITION BY table_name, check_name ORDER BY check_timestamp DESC) as rn
            FROM monitoring.data_quality_metrics
        )
        SELECT 
            table_name,
            check_name,
            check_result,
            ROUND(metric_value * 100, 2) as pass_rate_pct,
            check_timestamp
        FROM latest_checks
        WHERE rn = 1
        ORDER BY table_name, check_name
    """)
    
    return dashboard

quality_dashboard = create_quality_dashboard()
print("=== Data Quality Dashboard ===")
quality_dashboard.show(truncate=False)

# Bonus 3: Implement data sampling
def create_stratified_sample(df, strata_column, sample_size=1000):
    """Create stratified sample for testing"""
    
    # Calculate sample fraction per stratum
    strata_counts = df.groupBy(strata_column).count()
    total_count = df.count()
    
    # Sample proportionally from each stratum
    sampled_df = df.sampleBy(
        strata_column,
        fractions={row[strata_column]: sample_size / total_count for row in strata_counts.collect()},
        seed=42
    )
    
    return sampled_df

sample_df = create_stratified_sample(spark.table("silver.orders"), "status", 100)
print(f"✓ Stratified sample created: {sample_df.count()} records")

# Bonus 4: Add data masking
def mask_sensitive_data(df, columns_to_mask):
    """Mask sensitive data for lower environments"""
    
    masked_df = df
    
    for col_name in columns_to_mask:
        if col_name == "email":
            masked_df = masked_df.withColumn(
                col_name,
                concat(lit("masked_"), monotonically_increasing_id(), lit("@example.com"))
            )
        elif col_name == "phone":
            masked_df = masked_df.withColumn(
                col_name,
                lit("(XXX) XXX-XXXX")
            )
    
    return masked_df

masked_df = mask_sensitive_data(
    spark.table("silver.customers"),
    ["email", "phone"]
)
print("✓ Sensitive data masked")
masked_df.select("customer_id", "email", "phone").show(5)

# Bonus 5: Create pipeline orchestration DAG
pipeline_dag = {
    "bronze_ingestion": {
        "depends_on": [],
        "function": "run_bronze_ingestion",
        "retry": 3
    },
    "silver_transformation": {
        "depends_on": ["bronze_ingestion"],
        "function": "run_silver_transformation",
        "retry": 2
    },
    "gold_aggregation": {
        "depends_on": ["silver_transformation"],
        "function": "run_gold_aggregation",
        "retry": 2
    },
    "quality_checks": {
        "depends_on": ["silver_transformation"],
        "function": "run_quality_checks",
        "retry": 1
    }
}

print("✓ Pipeline DAG defined")
print(f"Pipeline tasks: {list(pipeline_dag.keys())}")

# ============================================================================
# FINAL VERIFICATION
# ============================================================================

print("\n" + "="*80)
print("FINAL PIPELINE VERIFICATION")
print("="*80)

# Verify all layers
print("\n=== Layer Record Counts ===")
bronze_count = spark.table("bronze.orders").count()
silver_count = spark.table("silver.orders").count()
gold_count = spark.table("gold.daily_customer_summary").count()

print(f"Bronze: {bronze_count:,} records")
print(f"Silver: {silver_count:,} records")
print(f"Gold: {gold_count:,} records")

# Data quality summary
print("\n=== Data Quality Summary ===")
quality_summary = spark.sql("""
    SELECT 
        table_name,
        COUNT(*) as total_checks,
        SUM(CASE WHEN check_result = 'PASS' THEN 1 ELSE 0 END) as passed_checks,
        ROUND(AVG(metric_value) * 100, 2) as avg_pass_rate_pct
    FROM monitoring.data_quality_metrics
    GROUP BY table_name
""")
quality_summary.show()

# Pipeline execution summary
print("\n=== Pipeline Execution Summary ===")
pipeline_summary = spark.sql("""
    SELECT 
        pipeline_name,
        status,
        COUNT(*) as execution_count,
        ROUND(AVG(duration_seconds), 2) as avg_duration_sec,
        MAX(execution_timestamp) as last_execution
    FROM monitoring.pipeline_metrics
    GROUP BY pipeline_name, status
    ORDER BY last_execution DESC
""")
pipeline_summary.show(truncate=False)

print("\n✅ All exercises completed successfully!")
print("="*80)
