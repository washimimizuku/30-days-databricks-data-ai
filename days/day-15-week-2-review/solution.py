# Day 15: Week 2 Review & ELT Project - Solutions

"""
Complete solutions for Week 2 Integration Project
Demonstrates end-to-end ELT pipeline using Medallion Architecture
"""

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# PART 1: BRONZE LAYER - RAW DATA INGESTION
# ============================================================================

# Exercise 1: Ingest orders from JSON to Bronze
bronze_orders = (
    spark.read
    .format("json")
    .load("/tmp/week2_project/raw/orders/")
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

(bronze_orders.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("week2_bronze.orders")
)

print(f"✓ Bronze orders: {bronze_orders.count():,} records")

# Exercise 2: Ingest customers from CSV to Bronze
bronze_customers = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/tmp/week2_project/raw/customers/")
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

(bronze_customers.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("week2_bronze.customers")
)

print(f"✓ Bronze customers: {bronze_customers.count():,} records")

# Exercise 3: Ingest products from Parquet to Bronze
bronze_products = (
    spark.read
    .format("parquet")
    .load("/tmp/week2_project/raw/products/")
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

(bronze_products.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("week2_bronze.products")
)

print(f"✓ Bronze products: {bronze_products.count():,} records")

# Exercise 4: Ingest nested orders using Auto Loader
def ingest_nested_orders_streaming():
    """Ingest nested orders with Auto Loader"""
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/tmp/week2_project/checkpoints/schema_location")
        .load("/tmp/week2_project/raw/nested_orders/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )
    
    query = (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", "/tmp/week2_project/checkpoints/bronze_nested")
        .trigger(availableNow=True)
        .table("week2_bronze.nested_orders")
    )
    
    query.awaitTermination()
    return query

# Run streaming ingestion
# ingest_nested_orders_streaming()

# Exercise 5: Verify Bronze layer record counts
print("\n=== Bronze Layer Record Counts ===")
for table in ["orders", "customers", "products"]:
    count = spark.table(f"week2_bronze.{table}").count()
    print(f"{table}: {count:,}")

# Exercise 6: Check Bronze data quality issues
bronze_orders_df = spark.table("week2_bronze.orders")

null_order_ids = bronze_orders_df.filter(col("order_id").isNull()).count()
null_customer_ids = bronze_orders_df.filter(col("customer_id").isNull()).count()
invalid_amounts = bronze_orders_df.filter(col("amount") <= 0).count()

print(f"\n=== Bronze Data Quality Issues ===")
print(f"Null order_ids: {null_order_ids}")
print(f"Null customer_ids: {null_customer_ids}")
print(f"Invalid amounts: {invalid_amounts}")

# Exercise 7: Create Bronze ingestion function
def ingest_to_bronze(source_path, format_type, table_name):
    """Reusable Bronze ingestion function"""
    start_time = datetime.now()
    
    try:
        # Read data
        df = (
            spark.read
            .format(format_type)
            .option("header", "true" if format_type == "csv" else "false")
            .option("inferSchema", "true")
            .load(source_path)
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source_file", input_file_name())
        )
        
        # Write to Bronze
        (df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(f"week2_bronze.{table_name}")
        )
        
        duration = (datetime.now() - start_time).total_seconds()
        record_count = df.count()
        
        logger.info(f"Bronze ingestion completed: {table_name} ({record_count:,} records in {duration:.2f}s)")
        
        return {"status": "SUCCESS", "records": record_count, "duration": duration}
        
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Bronze ingestion failed: {table_name} - {str(e)}")
        return {"status": "FAILED", "error": str(e), "duration": duration}

# Exercise 8: Log Bronze ingestion metrics
from days.day_15_week_2_review.setup import Week2Helpers
helpers = Week2Helpers()

result = ingest_to_bronze("/tmp/week2_project/raw/orders/", "json", "orders")
helpers.log_pipeline_execution(
    "bronze_ingestion",
    "bronze",
    result["status"],
    result.get("records", 0),
    result["duration"],
    result.get("error")
)

# Exercise 9: Handle Bronze ingestion errors
def safe_bronze_ingestion(source_path, format_type, table_name):
    """Bronze ingestion with comprehensive error handling"""
    try:
        result = ingest_to_bronze(source_path, format_type, table_name)
        
        if result["status"] == "SUCCESS":
            helpers.log_pipeline_execution(
                f"bronze_{table_name}",
                "bronze",
                "SUCCESS",
                result["records"],
                result["duration"]
            )
        
        return result
        
    except Exception as e:
        logger.error(f"Critical error in Bronze ingestion: {str(e)}")
        helpers.log_pipeline_execution(
            f"bronze_{table_name}",
            "bronze",
            "FAILED",
            0,
            0,
            str(e)
        )
        raise

# Exercise 10: Verify Bronze table properties
print("\n=== Bronze Table Properties ===")
spark.sql("DESCRIBE EXTENDED week2_bronze.orders").show(truncate=False)

# ============================================================================
# PART 2: SILVER LAYER - DATA CLEANING
# ============================================================================

# Exercise 11: Clean orders - remove nulls
silver_orders = spark.table("week2_bronze.orders")
silver_orders = silver_orders.filter(
    col("order_id").isNotNull() & col("customer_id").isNotNull()
)
print(f"After removing nulls: {silver_orders.count():,} records")

# Exercise 12: Clean orders - remove duplicates
silver_orders = silver_orders.dropDuplicates(["order_id"])
print(f"After deduplication: {silver_orders.count():,} records")

# Exercise 13: Clean orders - standardize dates
silver_orders = silver_orders.withColumn(
    "order_date",
    coalesce(
        to_timestamp(col("order_date"), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col("order_date"), "MM/dd/yyyy"),
        to_timestamp(col("order_date"), "yyyy-MM-dd")
    )
)
print("✓ Dates standardized")

# Exercise 14: Clean orders - validate amounts
silver_orders = silver_orders.filter(col("amount") > 0)
print(f"After amount validation: {silver_orders.count():,} records")

# Exercise 15: Clean orders - standardize status
silver_orders = silver_orders.withColumn(
    "status",
    lower(trim(col("status")))
)

# Exercise 16: Clean orders - add derived columns
silver_orders = (silver_orders
    .withColumn("total_amount", col("amount") + col("shipping_cost"))
    .withColumn("discount_amount", col("amount") * (col("discount_percent") / 100))
    .withColumn("final_amount", col("total_amount") - col("discount_amount"))
)
print("✓ Derived columns added")

# Exercise 17: Clean customers - validate emails
silver_customers = spark.table("week2_bronze.customers")
silver_customers = silver_customers.filter(
    col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
)
print(f"Valid customers: {silver_customers.count():,}")

# Exercise 18: Clean customers - standardize phone numbers
silver_customers = silver_customers.withColumn(
    "phone",
    regexp_replace(col("phone"), r"[^\d]", "")
).withColumn(
    "phone",
    concat(
        lit("("),
        substring(col("phone"), 1, 3),
        lit(") "),
        substring(col("phone"), 4, 3),
        lit("-"),
        substring(col("phone"), 7, 4)
    )
)

# Exercise 19: Enrich orders with customer data
silver_orders_enriched = silver_orders.join(
    silver_customers.select(
        "customer_id", "customer_name", "email", "segment", 
        "country", "lifetime_value"
    ),
    "customer_id",
    "left"
)
print(f"✓ Orders enriched with customer data: {silver_orders_enriched.count():,}")

# Exercise 20: Enrich orders with product data
silver_products = spark.table("week2_bronze.products")

silver_orders_final = silver_orders_enriched.join(
    silver_products.select(
        "product_id", "product_name", "category", 
        "price", "cost"
    ),
    "product_id",
    "left"
)
print(f"✓ Orders enriched with product data: {silver_orders_final.count():,}")

# Exercise 21: Add audit columns
silver_orders_final = (silver_orders_final
    .withColumn("_created_at", current_timestamp())
    .withColumn("_created_by", current_user())
    .withColumn("_updated_at", current_timestamp())
)

# Exercise 22: Write to Silver orders table
(silver_orders_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("week2_silver.orders")
)
print(f"✓ Silver orders table created: {silver_orders_final.count():,} records")

# Exercise 23: Implement data quality checks
def run_silver_quality_checks():
    """Comprehensive quality checks for Silver layer"""
    df = spark.table("week2_silver.orders")
    total = df.count()
    
    checks = [
        ("no_null_order_ids", df.filter(col("order_id").isNotNull()).count()),
        ("no_null_customer_ids", df.filter(col("customer_id").isNotNull()).count()),
        ("valid_amounts", df.filter(col("amount") > 0).count()),
        ("valid_dates", df.filter(col("order_date").isNotNull()).count()),
        ("no_duplicates", df.dropDuplicates(["order_id"]).count())
    ]
    
    for check_name, passed in checks:
        failed = total - passed if check_name != "no_duplicates" else 0
        helpers.log_quality_check(
            "week2_silver.orders",
            check_name,
            total,
            passed,
            failed
        )
        print(f"{check_name}: {passed}/{total} ({passed/total*100:.1f}%)")

run_silver_quality_checks()

# Exercise 24: Write failed records to DLQ
failed_records = spark.table("week2_bronze.orders").filter(
    col("order_id").isNull() | 
    col("customer_id").isNull() | 
    (col("amount") <= 0)
)

if failed_records.count() > 0:
    helpers.write_to_dlq(
        failed_records,
        "week2_bronze.orders",
        "validation_failure",
        "Failed null check or amount validation"
    )
    print(f"✓ {failed_records.count()} failed records written to DLQ")

# Exercise 25: Create Silver transformation function
def transform_to_silver(bronze_table, silver_table):
    """Reusable Silver transformation function"""
    start_time = datetime.now()
    
    try:
        # Read Bronze
        df = spark.table(bronze_table)
        
        # Apply transformations
        silver_df = (df
            # Remove nulls
            .filter(col("order_id").isNotNull())
            .filter(col("customer_id").isNotNull())
            
            # Deduplicate
            .dropDuplicates(["order_id"])
            
            # Standardize dates
            .withColumn("order_date", coalesce(
                to_timestamp(col("order_date"), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(col("order_date"), "MM/dd/yyyy"),
                to_timestamp(col("order_date"), "yyyy-MM-dd")
            ))
            
            # Validate amounts
            .filter(col("amount") > 0)
            
            # Standardize strings
            .withColumn("status", lower(trim(col("status"))))
            
            # Add derived columns
            .withColumn("total_amount", col("amount") + col("shipping_cost"))
            .withColumn("discount_amount", col("amount") * (col("discount_percent") / 100))
            .withColumn("final_amount", col("total_amount") - col("discount_amount"))
            
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
        
        duration = (datetime.now() - start_time).total_seconds()
        record_count = silver_df.count()
        
        logger.info(f"Silver transformation completed: {silver_table} ({record_count:,} records)")
        
        return {"status": "SUCCESS", "records": record_count, "duration": duration}
        
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Silver transformation failed: {str(e)}")
        return {"status": "FAILED", "error": str(e), "duration": duration}

print("✓ Silver layer transformation complete")

# ============================================================================
# PART 3: WINDOW FUNCTIONS
# ============================================================================

# Exercise 26: Rank customers by total spending
customer_spending = (
    spark.table("week2_silver.orders")
    .groupBy("customer_id", "customer_name")
    .agg(sum("final_amount").alias("total_spent"))
    .withColumn("rank", row_number().over(Window.orderBy(desc("total_spent"))))
)
customer_spending.show(10)

# Exercise 27: Top 3 products per category
product_sales = (
    spark.table("week2_silver.orders")
    .groupBy("category", "product_id", "product_name")
    .agg(sum("final_amount").alias("total_sales"))
)

window_category = Window.partitionBy("category").orderBy(desc("total_sales"))
top_products = (
    product_sales
    .withColumn("rank", dense_rank().over(window_category))
    .filter(col("rank") <= 3)
)
top_products.show(20)

# Exercise 28: Running total of sales
daily_sales = (
    spark.table("week2_silver.orders")
    .groupBy(date_trunc("day", col("order_date")).alias("date"))
    .agg(sum("final_amount").alias("daily_revenue"))
    .orderBy("date")
)

window_running = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
running_total = daily_sales.withColumn(
    "cumulative_revenue",
    sum("daily_revenue").over(window_running)
)
running_total.show()

# Exercise 29: 7-day moving average
window_7day = Window.orderBy("date").rowsBetween(-6, 0)
moving_avg = daily_sales.withColumn(
    "moving_avg_7day",
    avg("daily_revenue").over(window_7day)
)
moving_avg.show()

# Exercise 30: Previous order for each customer
window_customer = Window.partitionBy("customer_id").orderBy("order_date")
orders_with_prev = (
    spark.table("week2_silver.orders")
    .withColumn("previous_order_date", lag("order_date", 1).over(window_customer))
)
orders_with_prev.select("customer_id", "order_date", "previous_order_date").show()

# Exercise 31: Days between orders
orders_with_gap = orders_with_prev.withColumn(
    "days_since_last_order",
    datediff(col("order_date"), col("previous_order_date"))
)
orders_with_gap.select("customer_id", "order_date", "days_since_last_order").show()

# Exercise 32: First-time vs repeat customers
orders_with_order_num = (
    spark.table("week2_silver.orders")
    .withColumn("order_number", row_number().over(window_customer))
    .withColumn("customer_type", when(col("order_number") == 1, "First-time").otherwise("Repeat"))
)
orders_with_order_num.groupBy("customer_type").count().show()

# Exercise 33: Customer lifetime value
customer_ltv = (
    spark.table("week2_silver.orders")
    .withColumn("cumulative_spent", sum("final_amount").over(
        Window.partitionBy("customer_id").orderBy("order_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    ))
)
customer_ltv.select("customer_id", "order_date", "final_amount", "cumulative_spent").show()

# Exercise 34: Percentile ranks (quartiles)
customer_quartiles = (
    customer_spending
    .withColumn("quartile", ntile(4).over(Window.orderBy("total_spent")))
)
customer_quartiles.groupBy("quartile").agg(
    count("*").alias("customer_count"),
    avg("total_spent").alias("avg_spent")
).show()

# Exercise 35: Month-over-month growth
monthly_sales = (
    spark.table("week2_silver.orders")
    .groupBy(date_trunc("month", col("order_date")).alias("month"))
    .agg(sum("final_amount").alias("monthly_revenue"))
    .orderBy("month")
)

window_mom = Window.orderBy("month")
mom_growth = monthly_sales.withColumn(
    "previous_month_revenue",
    lag("monthly_revenue", 1).over(window_mom)
).withColumn(
    "mom_growth_pct",
    ((col("monthly_revenue") - col("previous_month_revenue")) / col("previous_month_revenue") * 100)
)
mom_growth.show()

print("✓ Window functions exercises complete")

# ============================================================================
# PART 4: HIGHER-ORDER FUNCTIONS
# ============================================================================

# Exercise 36: Process nested order items
nested_orders = spark.table("week2_bronze.nested_orders")
exploded_items = nested_orders.select(
    "order_id",
    "customer_id",
    "order_date",
    explode("items").alias("item")
).select(
    "order_id",
    "customer_id",
    "order_date",
    col("item.product_id").alias("product_id"),
    col("item.quantity").alias("quantity"),
    col("item.unit_price").alias("unit_price")
)
exploded_items.show()

# Exercise 37: Transform prices with discount (SQL)
spark.sql("""
    SELECT 
        order_id,
        items,
        TRANSFORM(items, item -> 
            struct(
                item.product_id as product_id,
                item.quantity as quantity,
                item.unit_price * 0.9 as unit_price
            )
        ) as discounted_items
    FROM week2_bronze.nested_orders
""").show(5, truncate=False)

# Exercise 38: Filter high-value items (SQL)
spark.sql("""
    SELECT 
        order_id,
        items,
        FILTER(items, item -> item.unit_price > 100) as high_value_items
    FROM week2_bronze.nested_orders
""").show(5, truncate=False)

# Exercise 39: Calculate total per order (SQL)
spark.sql("""
    SELECT 
        order_id,
        AGGREGATE(
            items,
            0.0,
            (acc, item) -> acc + (item.quantity * item.unit_price)
        ) as order_total
    FROM week2_bronze.nested_orders
""").show()

# Exercise 40: Check for specific products (SQL)
spark.sql("""
    SELECT 
        order_id,
        EXISTS(items, item -> item.product_id = 'PROD0001') as contains_prod0001
    FROM week2_bronze.nested_orders
""").show()

print("✓ Higher-order functions exercises complete")

# ============================================================================
# PART 5: GOLD LAYER - BUSINESS METRICS
# ============================================================================

# Exercise 41: Daily sales summary
gold_daily_sales = (
    spark.table("week2_silver.orders")
    .groupBy(date_trunc("day", col("order_date")).alias("date"))
    .agg(
        count("order_id").alias("total_orders"),
        sum("final_amount").alias("total_revenue"),
        avg("final_amount").alias("avg_order_value"),
        countDistinct("customer_id").alias("unique_customers")
    )
    .withColumn("_aggregation_timestamp", current_timestamp())
)

(gold_daily_sales.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("week2_gold.daily_sales_summary")
)
print(f"✓ Gold daily sales: {gold_daily_sales.count():,} records")

# Exercise 42: Customer summary
gold_customer_summary = (
    spark.table("week2_silver.orders")
    .groupBy("customer_id", "customer_name", "segment")
    .agg(
        count("order_id").alias("total_orders"),
        sum("final_amount").alias("total_spent"),
        avg("final_amount").alias("avg_order_value"),
        max("order_date").alias("last_order_date"),
        min("order_date").alias("first_order_date")
    )
    .withColumn("_aggregation_timestamp", current_timestamp())
)

(gold_customer_summary.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("week2_gold.customer_summary")
)
print(f"✓ Gold customer summary: {gold_customer_summary.count():,} records")

# Exercise 43: Product performance summary
gold_product_performance = (
    spark.table("week2_silver.orders")
    .groupBy("product_id", "product_name", "category")
    .agg(
        sum("quantity").alias("total_quantity_sold"),
        sum("final_amount").alias("total_revenue"),
        avg("amount").alias("avg_price"),
        count("order_id").alias("order_count")
    )
    .withColumn("_aggregation_timestamp", current_timestamp())
)

(gold_product_performance.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("week2_gold.product_performance")
)
print(f"✓ Gold product performance: {gold_product_performance.count():,} records")

# Exercise 44: Category performance summary
gold_category_performance = (
    spark.table("week2_silver.orders")
    .groupBy("category")
    .agg(
        sum("final_amount").alias("total_sales"),
        count("order_id").alias("total_orders"),
        avg("final_amount").alias("avg_order_value"),
        countDistinct("customer_id").alias("unique_customers")
    )
    .withColumn("_aggregation_timestamp", current_timestamp())
)

(gold_category_performance.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("week2_gold.category_performance")
)
print(f"✓ Gold category performance: {gold_category_performance.count():,} records")

# Exercise 45: Customer segment analysis
gold_segment_analysis = (
    spark.table("week2_silver.orders")
    .groupBy("segment")
    .agg(
        countDistinct("customer_id").alias("customer_count"),
        sum("final_amount").alias("total_revenue"),
        avg("lifetime_value").alias("avg_lifetime_value"),
        count("order_id").alias("total_orders")
    )
    .withColumn("_aggregation_timestamp", current_timestamp())
)

(gold_segment_analysis.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("week2_gold.segment_analysis")
)
print(f"✓ Gold segment analysis: {gold_segment_analysis.count():,} records")

# Exercise 46: Monthly revenue trend
gold_monthly_trend = (
    spark.table("week2_silver.orders")
    .groupBy(date_trunc("month", col("order_date")).alias("month"))
    .agg(
        sum("final_amount").alias("total_revenue"),
        count("order_id").alias("order_count"),
        countDistinct("customer_id").alias("unique_customers"),
        avg("final_amount").alias("avg_order_value")
    )
    .orderBy("month")
    .withColumn("_aggregation_timestamp", current_timestamp())
)

(gold_monthly_trend.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("week2_gold.monthly_trend")
)
print(f"✓ Gold monthly trend: {gold_monthly_trend.count():,} records")

# Exercise 47: Top customers report
gold_top_customers = (
    spark.table("week2_gold.customer_summary")
    .orderBy(desc("total_spent"))
    .limit(100)
)

(gold_top_customers.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("week2_gold.top_100_customers")
)
print(f"✓ Gold top 100 customers created")

# Exercise 48: Product recommendations (frequently bought together)
# This is a simplified version - in production, use collaborative filtering
product_pairs = (
    spark.table("week2_silver.orders")
    .select("order_id", "product_id")
    .alias("a")
    .join(
        spark.table("week2_silver.orders")
        .select("order_id", "product_id")
        .alias("b"),
        col("a.order_id") == col("b.order_id")
    )
    .filter(col("a.product_id") < col("b.product_id"))
    .groupBy(col("a.product_id").alias("product_1"), col("b.product_id").alias("product_2"))
    .agg(count("*").alias("times_bought_together"))
    .orderBy(desc("times_bought_together"))
)

(product_pairs.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("week2_gold.product_affinity")
)
print(f"✓ Gold product affinity: {product_pairs.count():,} pairs")

# Exercise 49: Customer 360 view
gold_customer_360 = (
    spark.table("week2_bronze.customers")
    .join(
        spark.table("week2_gold.customer_summary"),
        "customer_id",
        "left"
    )
    .select(
        "customer_id",
        "customer_name",
        "email",
        "phone",
        "segment",
        "country",
        "registration_date",
        "lifetime_value",
        "total_orders",
        "total_spent",
        "avg_order_value",
        "last_order_date",
        "first_order_date"
    )
)

(gold_customer_360.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("week2_gold.customer_360")
)
print(f"✓ Gold customer 360: {gold_customer_360.count():,} records")

# Exercise 50: Verify all Gold tables
print("\n=== Gold Layer Tables ===")
spark.sql("SHOW TABLES IN week2_gold").show()

# Exercise 51: Daily customer summary
gold_daily_customer = (
    spark.table("week2_silver.orders")
    .groupBy(
        date_trunc("day", col("order_date")).alias("date"),
        "customer_id",
        "customer_name"
    )
    .agg(
        count("order_id").alias("orders"),
        sum("final_amount").alias("revenue")
    )
    .withColumn("_aggregation_timestamp", current_timestamp())
)

(gold_daily_customer.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("week2_gold.daily_customer_summary")
)
print(f"✓ Gold daily customer summary: {gold_daily_customer.count():,} records")

# Exercise 52: Cohort analysis
gold_cohort = (
    spark.table("week2_silver.orders")
    .join(
        spark.table("week2_bronze.customers").select("customer_id", "registration_date"),
        "customer_id"
    )
    .withColumn("registration_month", date_trunc("month", col("registration_date")))
    .withColumn("order_month", date_trunc("month", col("order_date")))
    .groupBy("registration_month", "order_month")
    .agg(
        countDistinct("customer_id").alias("active_customers"),
        sum("final_amount").alias("revenue")
    )
)

(gold_cohort.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("week2_gold.cohort_analysis")
)
print(f"✓ Gold cohort analysis: {gold_cohort.count():,} records")

# Exercise 53: RFM analysis
from pyspark.sql.functions import current_date

gold_rfm = (
    spark.table("week2_silver.orders")
    .groupBy("customer_id")
    .agg(
        datediff(current_date(), max("order_date")).alias("recency"),
        count("order_id").alias("frequency"),
        sum("final_amount").alias("monetary")
    )
    .withColumn("r_score", ntile(5).over(Window.orderBy(col("recency"))))
    .withColumn("f_score", ntile(5).over(Window.orderBy(desc("frequency"))))
    .withColumn("m_score", ntile(5).over(Window.orderBy(desc("monetary"))))
    .withColumn("rfm_score", concat(col("r_score"), col("f_score"), col("m_score")))
)

(gold_rfm.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("week2_gold.rfm_analysis")
)
print(f"✓ Gold RFM analysis: {gold_rfm.count():,} records")

# Exercise 54: Product affinity matrix (already done in Exercise 48)
print("✓ Product affinity matrix already created")

# Exercise 55: Optimize Gold tables
gold_tables = ["daily_sales_summary", "customer_summary", "product_performance", 
               "category_performance", "customer_360"]

for table in gold_tables:
    spark.sql(f"OPTIMIZE week2_gold.{table}")
    print(f"✓ Optimized week2_gold.{table}")

# Z-order key tables
spark.sql("OPTIMIZE week2_gold.customer_summary ZORDER BY (customer_id)")
spark.sql("OPTIMIZE week2_gold.daily_customer_summary ZORDER BY (date, customer_id)")
print("✓ Z-ordering applied to key tables")

print("✓ Gold layer complete")

# ============================================================================
# PART 6: DATA QUALITY & MONITORING
# ============================================================================

# Exercise 56: Comprehensive quality checks
def run_comprehensive_quality_checks():
    """Run all quality checks across layers"""
    
    # Silver orders checks
    silver_orders = spark.table("week2_silver.orders")
    total = silver_orders.count()
    
    checks = {
        "no_null_order_ids": silver_orders.filter(col("order_id").isNotNull()).count(),
        "no_null_customer_ids": silver_orders.filter(col("customer_id").isNotNull()).count(),
        "valid_amounts": silver_orders.filter(col("amount") > 0).count(),
        "valid_quantities": silver_orders.filter(col("quantity") > 0).count(),
        "valid_dates": silver_orders.filter(col("order_date").isNotNull()).count(),
        "no_duplicates": silver_orders.dropDuplicates(["order_id"]).count()
    }
    
    for check_name, passed in checks.items():
        failed = total - passed if check_name != "no_duplicates" else 0
        helpers.log_quality_check(
            "week2_silver.orders",
            check_name,
            total,
            passed,
            failed
        )
    
    # Referential integrity checks
    orders_with_invalid_customers = (
        silver_orders
        .join(
            spark.table("week2_bronze.customers").select("customer_id"),
            "customer_id",
            "left_anti"
        )
        .count()
    )
    
    helpers.log_quality_check(
        "week2_silver.orders",
        "referential_integrity_customers",
        total,
        total - orders_with_invalid_customers,
        orders_with_invalid_customers
    )
    
    print("✓ Comprehensive quality checks complete")

run_comprehensive_quality_checks()

# Exercise 57: Quality dashboard query
quality_dashboard = spark.sql("""
    SELECT 
        table_name,
        check_name,
        check_result,
        ROUND(pass_rate * 100, 2) as pass_rate_pct,
        records_checked,
        records_failed,
        check_timestamp
    FROM week2_monitoring.quality_metrics
    ORDER BY check_timestamp DESC
""")
quality_dashboard.show(20, truncate=False)

# Exercise 58: Pipeline metrics dashboard
pipeline_dashboard = spark.sql("""
    SELECT 
        pipeline_name,
        layer,
        status,
        records_processed,
        ROUND(duration_seconds, 2) as duration_sec,
        execution_timestamp
    FROM week2_monitoring.pipeline_metrics
    ORDER BY execution_timestamp DESC
""")
pipeline_dashboard.show(20, truncate=False)

# Exercise 59: Analyze dead letter queue
dlq_analysis = spark.sql("""
    SELECT 
        source_table,
        error_type,
        COUNT(*) as error_count,
        MAX(error_timestamp) as last_error
    FROM week2_monitoring.dead_letter_queue
    GROUP BY source_table, error_type
    ORDER BY error_count DESC
""")
dlq_analysis.show()

# Exercise 60: Calculate data quality score
def calculate_quality_score(table_name):
    """Calculate overall quality score (0-100)"""
    metrics = spark.sql(f"""
        SELECT AVG(pass_rate) as avg_pass_rate
        FROM week2_monitoring.quality_metrics
        WHERE table_name = '{table_name}'
    """).collect()[0][0]
    
    score = metrics * 100 if metrics else 0
    return score

silver_score = calculate_quality_score("week2_silver.orders")
print(f"Silver orders quality score: {silver_score:.1f}/100")

# Exercise 61: Data profiling
def profile_table(table_name):
    """Profile table with key statistics"""
    df = spark.table(table_name)
    
    profile = {
        "table": table_name,
        "row_count": df.count(),
        "column_count": len(df.columns),
        "null_counts": {},
        "distinct_counts": {}
    }
    
    for col_name in df.columns:
        profile["null_counts"][col_name] = df.filter(col(col_name).isNull()).count()
        profile["distinct_counts"][col_name] = df.select(col_name).distinct().count()
    
    return profile

silver_profile = profile_table("week2_silver.orders")
print(f"\n=== Table Profile: week2_silver.orders ===")
print(f"Rows: {silver_profile['row_count']:,}")
print(f"Columns: {silver_profile['column_count']}")

# Exercise 62: Referential integrity check
def check_referential_integrity(fact_table, dim_table, join_key):
    """Check referential integrity"""
    orphans = (
        spark.table(fact_table)
        .select(join_key)
        .distinct()
        .join(
            spark.table(dim_table).select(join_key),
            join_key,
            "left_anti"
        )
        .count()
    )
    
    return orphans == 0, orphans

is_valid, orphan_count = check_referential_integrity(
    "week2_silver.orders",
    "week2_bronze.customers",
    "customer_id"
)
print(f"Referential integrity check: {'PASS' if is_valid else 'FAIL'} ({orphan_count} orphans)")

# Exercise 63: Validate business rules
business_rules = spark.sql("""
    SELECT 
        COUNT(*) as total_records,
        SUM(CASE WHEN amount > 0 THEN 1 ELSE 0 END) as valid_amounts,
        SUM(CASE WHEN quantity > 0 THEN 1 ELSE 0 END) as valid_quantities,
        SUM(CASE WHEN order_date IS NOT NULL THEN 1 ELSE 0 END) as valid_dates
    FROM week2_silver.orders
""")
business_rules.show()

# Exercise 64: Monitor data freshness
data_freshness = spark.sql("""
    SELECT 
        'bronze.orders' as table_name,
        MAX(_ingestion_timestamp) as last_updated
    FROM week2_bronze.orders
    UNION ALL
    SELECT 
        'silver.orders' as table_name,
        MAX(_created_at) as last_updated
    FROM week2_silver.orders
""")
data_freshness.show()

# Exercise 65: Create alerting logic
def check_quality_alerts():
    """Identify tables with quality issues"""
    alerts = spark.sql("""
        SELECT 
            table_name,
            check_name,
            ROUND(pass_rate * 100, 2) as pass_rate_pct
        FROM week2_monitoring.quality_metrics
        WHERE pass_rate < 0.95
        ORDER BY pass_rate
    """)
    
    alert_count = alerts.count()
    
    if alert_count > 0:
        print(f"⚠️  {alert_count} quality alerts found:")
        alerts.show()
    else:
        print("✓ No quality alerts - all checks passing")
    
    return alerts

check_quality_alerts()

print("✓ Data quality and monitoring complete")

# ============================================================================
# PART 7: PERFORMANCE OPTIMIZATION
# ============================================================================

# Exercise 66: Partition Silver orders by date
spark.sql("""
    CREATE OR REPLACE TABLE week2_silver.orders_partitioned
    USING DELTA
    PARTITIONED BY (order_date)
    AS SELECT * FROM week2_silver.orders
""")
print("✓ Silver orders partitioned by date")

# Exercise 67: Z-order Silver orders
spark.sql("OPTIMIZE week2_silver.orders ZORDER BY (customer_id, product_id)")
print("✓ Z-ordering applied to Silver orders")

# Exercise 68: Optimize file sizes
for table in ["orders", "customers", "products"]:
    spark.sql(f"OPTIMIZE week2_bronze.{table}")
    print(f"✓ Optimized week2_bronze.{table}")

# Exercise 69: Broadcast join
from pyspark.sql.functions import broadcast

orders = spark.table("week2_silver.orders")
products = spark.table("week2_bronze.products")

# Broadcast small dimension table
enriched = orders.join(broadcast(products), "product_id")
print(f"✓ Broadcast join completed: {enriched.count():,} records")

# Exercise 70: Cache dimension tables
customers_cached = spark.table("week2_bronze.customers").cache()
products_cached = spark.table("week2_bronze.products").cache()

# Use cached tables
high_value_customers = customers_cached.filter(col("lifetime_value") > 10000)
electronics = products_cached.filter(col("category") == "Electronics")

print(f"High value customers: {high_value_customers.count():,}")
print(f"Electronics products: {electronics.count():,}")

# Unpersist
customers_cached.unpersist()
products_cached.unpersist()
print("✓ Cache management complete")

# Exercise 71: Analyze table statistics
spark.sql("ANALYZE TABLE week2_silver.orders COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE week2_silver.orders COMPUTE STATISTICS FOR ALL COLUMNS")
print("✓ Table statistics updated")

# Exercise 72: Check file size distribution
file_stats = spark.sql("DESCRIBE DETAIL week2_silver.orders")
file_stats.select("numFiles", "sizeInBytes").show()

# Exercise 73: Incremental Gold updates
def incremental_gold_update(start_date, end_date):
    """Update Gold tables incrementally"""
    
    # Get new data
    new_orders = (
        spark.table("week2_silver.orders")
        .filter(col("order_date").between(start_date, end_date))
    )
    
    # Aggregate
    new_daily_sales = (
        new_orders
        .groupBy(date_trunc("day", col("order_date")).alias("date"))
        .agg(
            count("order_id").alias("total_orders"),
            sum("final_amount").alias("total_revenue")
        )
    )
    
    # Merge into Gold
    target = DeltaTable.forName(spark, "week2_gold.daily_sales_summary")
    
    (target.alias("target")
        .merge(
            new_daily_sales.alias("updates"),
            "target.date = updates.date"
        )
        .whenMatchedUpdate(set={
            "total_orders": col("target.total_orders") + col("updates.total_orders"),
            "total_revenue": col("target.total_revenue") + col("updates.total_revenue")
        })
        .whenNotMatchedInsertAll()
        .execute()
    )
    
    print(f"✓ Incremental update completed for {start_date} to {end_date}")

# Exercise 74: Add table constraints
spark.sql("""
    ALTER TABLE week2_silver.orders 
    ADD CONSTRAINT valid_amount CHECK (amount > 0)
""")

spark.sql("""
    ALTER TABLE week2_silver.orders 
    ADD CONSTRAINT valid_quantity CHECK (quantity > 0)
""")
print("✓ Table constraints added")

# Exercise 75: Measure query performance
import time

# Before optimization
start = time.time()
result1 = spark.table("week2_silver.orders").filter(col("customer_id") == 100).count()
duration_before = time.time() - start

# After optimization (with Z-ordering)
spark.sql("OPTIMIZE week2_silver.orders ZORDER BY (customer_id)")

start = time.time()
result2 = spark.table("week2_silver.orders").filter(col("customer_id") == 100).count()
duration_after = time.time() - start

print(f"\nQuery Performance:")
print(f"  Before: {duration_before:.3f}s")
print(f"  After: {duration_after:.3f}s")
print(f"  Improvement: {((duration_before - duration_after) / duration_before * 100):.1f}%")

print("✓ Performance optimization complete")

# ============================================================================
# FINAL VERIFICATION
# ============================================================================

print("\n" + "="*80)
print("WEEK 2 PROJECT - FINAL VERIFICATION")
print("="*80)

# Layer summaries
print("\n=== Bronze Layer ===")
spark.sql("""
    SELECT 'orders' as table, COUNT(*) as records FROM week2_bronze.orders
    UNION ALL
    SELECT 'customers', COUNT(*) FROM week2_bronze.customers
    UNION ALL
    SELECT 'products', COUNT(*) FROM week2_bronze.products
""").show()

print("\n=== Silver Layer ===")
spark.sql("SELECT COUNT(*) as orders FROM week2_silver.orders").show()

print("\n=== Gold Layer ===")
spark.sql("SHOW TABLES IN week2_gold").show()

# Quality summary
print("\n=== Quality Summary ===")
spark.sql("""
    SELECT 
        table_name,
        COUNT(*) as total_checks,
        SUM(CASE WHEN check_result = 'PASS' THEN 1 ELSE 0 END) as passed,
        ROUND(AVG(pass_rate) * 100, 2) as avg_pass_rate_pct
    FROM week2_monitoring.quality_metrics
    GROUP BY table_name
""").show()

# Pipeline summary
print("\n=== Pipeline Summary ===")
spark.sql("""
    SELECT 
        layer,
        status,
        COUNT(*) as execution_count,
        ROUND(AVG(duration_seconds), 2) as avg_duration_sec
    FROM week2_monitoring.pipeline_metrics
    GROUP BY layer, status
""").show()

print("\n" + "="*80)
print("✅ WEEK 2 PROJECT COMPLETE!")
print("="*80)
print("\nAchievements:")
print("  ✓ Built complete Medallion Architecture pipeline")
print("  ✓ Processed 50,000+ orders across Bronze → Silver → Gold")
print("  ✓ Implemented comprehensive data quality checks")
print("  ✓ Applied performance optimizations")
print("  ✓ Created business metrics and dashboards")
print("\nNext Steps:")
print("  1. Review the 50-question quiz")
print("  2. Identify any weak areas")
print("  3. Prepare for Week 3: Incremental Data Processing")
print("="*80)
