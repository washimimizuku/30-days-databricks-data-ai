# Day 26: Data Quality & Testing - Solutions
# Complete solutions with explanations

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

# Load data
customers_df = spark.table("quality_testing.customers_raw")
orders_df = spark.table("quality_testing.orders_raw")
products_df = spark.table("quality_testing.products_raw")

# ============================================================================
# PART 1: COMPLETENESS CHECKS (10 exercises)
# ============================================================================

# Exercise 1: Count null values in each column of customers_raw
null_counts = customers_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in customers_df.columns
])
null_counts.show()
# Shows null count for each column

# Exercise 2: Calculate completeness percentage for each column
total_rows = customers_df.count()
for column in customers_df.columns:
    null_count = customers_df.filter(col(column).isNull()).count()
    completeness = (1 - null_count / total_rows) * 100
    print(f"{column}: {completeness:.2f}% complete")
# Calculates and displays completeness percentage

# Exercise 3: Find rows with any null values
rows_with_nulls = customers_df.filter(
    col("customer_id").isNull() |
    col("name").isNull() |
    col("email").isNull() |
    col("age").isNull() |
    col("state").isNull()
)
rows_with_nulls.show()
# Shows all rows that have at least one null value

# Exercise 4: Find rows with all required fields populated
complete_rows = customers_df.filter(
    col("customer_id").isNotNull() &
    col("name").isNotNull() &
    col("email").isNotNull()
)
complete_rows.show()
# Shows rows where all critical fields are populated

# Exercise 5: Create a completeness report
completeness_data = []
for column in customers_df.columns:
    null_count = customers_df.filter(col(column).isNull()).count()
    completeness_pct = (1 - null_count / total_rows) * 100
    completeness_data.append((column, null_count, completeness_pct))

completeness_report = spark.createDataFrame(
    completeness_data,
    ["column_name", "null_count", "completeness_pct"]
)
completeness_report.show()
# Creates structured report of completeness metrics

# Exercise 6: Identify columns below 90% completeness
low_completeness = completeness_report.filter(col("completeness_pct") < 90)
low_completeness.show()
# Highlights columns that need attention

# Exercise 7: Count rows with missing email
missing_email_count = customers_df.filter(col("email").isNull()).count()
print(f"Customers with missing email: {missing_email_count}")

# Exercise 8: Find products with missing price
products_missing_price = products_df.filter(col("price").isNull())
products_missing_price.show()

# Exercise 9: Calculate overall data completeness
avg_completeness = completeness_report.agg(
    avg("completeness_pct").alias("overall_completeness")
).collect()[0]["overall_completeness"]
print(f"Overall data completeness: {avg_completeness:.2f}%")

# Exercise 10: Create a function to check completeness
def check_completeness(df, column):
    """Calculate completeness percentage for a column"""
    total = df.count()
    null_count = df.filter(col(column).isNull()).count()
    return (1 - null_count / total) * 100

# Test the function
email_completeness = check_completeness(customers_df, "email")
print(f"Email completeness: {email_completeness:.2f}%")

# ============================================================================
# PART 2: ACCURACY CHECKS (10 exercises)
# ============================================================================

# Exercise 11: Find customers with invalid age (< 0 or > 120)
invalid_ages = customers_df.filter(
    (col("age") < 0) | (col("age") > 120)
)
invalid_ages.show()
# Shows customers with biologically impossible ages

# Exercise 12: Find orders with negative amounts
negative_amounts = orders_df.filter(col("amount") < 0)
negative_amounts.show()
# Negative amounts indicate data errors

# Exercise 13: Find orders with zero amounts
zero_amounts = orders_df.filter(col("amount") == 0)
zero_amounts.show()
# Zero amounts may be valid (free orders) or errors

# Exercise 14: Count products with negative stock
negative_stock_count = products_df.filter(col("stock_quantity") < 0).count()
print(f"Products with negative stock: {negative_stock_count}")

# Exercise 15: Find customers with invalid email format
email_pattern = "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
invalid_emails = customers_df.filter(
    ~col("email").rlike(email_pattern) & col("email").isNotNull()
)
invalid_emails.show()
# Shows emails that don't match standard format

# Exercise 16: Validate state codes
valid_states = ["CA", "NY", "TX", "FL"]
invalid_states = customers_df.filter(~col("state").isin(valid_states))
invalid_states.show()
# Shows customers with invalid state codes

# Exercise 17: Check for future dates
from pyspark.sql.functions import current_date, to_date
future_dates = orders_df.filter(
    to_date(col("order_date")) > current_date()
)
future_dates.show()
# Future dates indicate data entry errors

# Exercise 18: Find products with empty names
empty_names = products_df.filter(
    (col("product_name") == "") | col("product_name").isNull()
)
empty_names.show()

# Exercise 19: Calculate percentage of invalid ages
total_customers = customers_df.count()
invalid_age_count = customers_df.filter(
    (col("age") < 0) | (col("age") > 120) | col("age").isNull()
).count()
invalid_age_pct = (invalid_age_count / total_customers) * 100
print(f"Invalid age percentage: {invalid_age_pct:.2f}%")

# Exercise 20: Create accuracy score for customers table
valid_customers = customers_df.filter(
    col("customer_id").isNotNull() &
    col("name").isNotNull() &
    col("email").rlike(email_pattern) &
    col("age").between(0, 120) &
    col("state").isin(valid_states)
)
accuracy_score = (valid_customers.count() / total_customers) * 100
print(f"Customer data accuracy: {accuracy_score:.2f}%")

# ============================================================================
# PART 3: CONSISTENCY CHECKS (10 exercises)
# ============================================================================

# Exercise 21: Find orders without matching customers (referential integrity)
orphaned_orders = orders_df.join(
    customers_df,
    orders_df.customer_id == customers_df.customer_id,
    "left_anti"
)
orphaned_orders.show()
# Left anti join finds orders with no matching customer

# Exercise 22: Count orphaned orders
orphan_count = orphaned_orders.count()
print(f"Orphaned orders: {orphan_count}")

# Exercise 23: Find duplicate customer_ids
duplicate_ids = customers_df.groupBy("customer_id").count().filter(col("count") > 1)
duplicate_ids.show()

# Exercise 24: Check email consistency (case sensitivity)
email_consistency = customers_df.groupBy(lower(col("email")).alias("email_lower")).agg(
    collect_set("email").alias("email_variations"),
    count("*").alias("count")
).filter(size(col("email_variations")) > 1)
email_consistency.show(truncate=False)

# Exercise 25: Validate order status consistency
valid_statuses = ["pending", "completed", "cancelled"]
invalid_status = orders_df.filter(~col("status").isin(valid_statuses))
invalid_status.show()

# Exercise 26: Check product category consistency
category_summary = products_df.groupBy("category").count().orderBy("category")
category_summary.show()

# Exercise 27: Find customers with inconsistent data
inconsistent = customers_df.filter(
    col("name").isNull() & col("email").isNotNull()
)
inconsistent.show()

# Exercise 28: Cross-table consistency check
all_order_customers = orders_df.select("customer_id").distinct()
all_customers = customers_df.select("customer_id").distinct()
missing_customers = all_order_customers.subtract(all_customers)
print(f"Customer IDs in orders but not in customers: {missing_customers.count()}")

# Exercise 29: Check date format consistency
date_pattern = "^\\d{4}-\\d{2}-\\d{2}$"
invalid_dates = orders_df.filter(
    ~col("order_date").rlike(date_pattern) & col("order_date").isNotNull()
)
invalid_dates.show()

# Exercise 30: Validate price consistency
price_inconsistency = products_df.filter(
    col("price").isNull() & (col("stock_quantity") > 0)
)
price_inconsistency.show()

# ============================================================================
# PART 4: UNIQUENESS CHECKS (10 exercises)
# ============================================================================

# Exercise 31: Find duplicate rows in customers_raw
duplicates = customers_df.groupBy(customers_df.columns).count().filter(col("count") > 1)
duplicates.show()

# Exercise 32: Count total duplicates
total_rows = customers_df.count()
unique_rows = customers_df.dropDuplicates().count()
duplicate_count = total_rows - unique_rows
print(f"Total duplicate rows: {duplicate_count}")

# Exercise 33: Find duplicate customer_ids
dup_ids = customers_df.groupBy("customer_id").count().filter(col("count") > 1)
dup_ids.show()

# Exercise 34: Remove duplicates from customers
unique_customers = customers_df.dropDuplicates()
print(f"Unique customers: {unique_customers.count()}")

# Exercise 35: Find duplicate emails
dup_emails = customers_df.groupBy("email").count().filter(col("count") > 1)
dup_emails.show()

# Exercise 36: Check order_id uniqueness
order_uniqueness = orders_df.groupBy("order_id").count().filter(col("count") > 1)
is_unique = order_uniqueness.count() == 0
print(f"Order IDs are unique: {is_unique}")

# Exercise 37: Find duplicate product_ids
dup_products = products_df.groupBy("product_id").count().filter(col("count") > 1)
dup_products.show()

# Exercise 38: Calculate uniqueness percentage
uniqueness_pct = (unique_rows / total_rows) * 100
print(f"Uniqueness: {uniqueness_pct:.2f}%")

# Exercise 39: Identify duplicate keys with different values
dup_with_diff_values = customers_df.groupBy("customer_id").agg(
    count("*").alias("count"),
    collect_set("name").alias("names")
).filter((col("count") > 1) & (size(col("names")) > 1))
dup_with_diff_values.show(truncate=False)

# Exercise 40: Create deduplication strategy
from pyspark.sql.window import Window
window_spec = Window.partitionBy("customer_id").orderBy(col("created_date").desc())
deduped = customers_df.withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")
deduped.show()

# ============================================================================
# PART 5: VALIDITY CHECKS (10 exercises)
# ============================================================================

# Exercise 41: Validate email format using regex
email_pattern = "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
valid_emails = customers_df.filter(col("email").rlike(email_pattern))
invalid_emails = customers_df.filter(~col("email").rlike(email_pattern) & col("email").isNotNull())
print(f"Valid emails: {valid_emails.count()}, Invalid: {invalid_emails.count()}")

# Exercise 42: Validate phone format (example with added column)
# phone_pattern = "\\d{3}-\\d{3}-\\d{4}"
# valid_phones = df.filter(col("phone").rlike(phone_pattern))

# Exercise 43: Validate date format
valid_dates = orders_df.filter(col("order_date").rlike("^\\d{4}-\\d{2}-\\d{2}$"))
print(f"Valid date formats: {valid_dates.count()}")

# Exercise 44: Check value domains
valid_order_statuses = orders_df.filter(col("status").isin(["pending", "completed", "cancelled"]))
print(f"Valid statuses: {valid_order_statuses.count()}")

# Exercise 45: Validate numeric ranges
valid_ages = customers_df.filter(col("age").between(0, 120))
valid_amounts = orders_df.filter(col("amount") > 0)
print(f"Valid ages: {valid_ages.count()}, Valid amounts: {valid_amounts.count()}")

# Exercise 46: Check string length constraints
valid_names = customers_df.filter(
    (length(col("name")) >= 1) & (length(col("name")) <= 100)
)
print(f"Valid name lengths: {valid_names.count()}")

# Exercise 47: Validate required fields
required_fields_present = customers_df.filter(
    col("customer_id").isNotNull() &
    col("name").isNotNull() &
    col("email").isNotNull()
)
print(f"Rows with required fields: {required_fields_present.count()}")

# Exercise 48: Check business rules
completed_orders_valid = orders_df.filter(
    (col("status") == "completed") & (col("amount") > 0)
)
print(f"Valid completed orders: {completed_orders_valid.count()}")

# Exercise 49: Validate foreign keys
valid_customer_refs = orders_df.join(
    customers_df.select("customer_id"),
    "customer_id",
    "inner"
)
print(f"Orders with valid customer references: {valid_customer_refs.count()}")

# Exercise 50: Create comprehensive validity report
validity_checks = [
    ("email_format", invalid_emails.count() == 0),
    ("age_range", customers_df.filter((col("age") < 0) | (col("age") > 120)).count() == 0),
    ("amount_positive", orders_df.filter(col("amount") <= 0).count() == 0),
    ("status_valid", orders_df.filter(~col("status").isin(valid_statuses)).count() == 0),
    ("no_orphans", orphaned_orders.count() == 0)
]
validity_report = spark.createDataFrame(validity_checks, ["check_name", "passed"])
validity_report.show()

# ============================================================================
# PART 6: DATA QUALITY CHECKER CLASS (10 exercises)
# ============================================================================

# Define the DataQualityChecker class
class DataQualityChecker:
    def __init__(self, df):
        self.df = df
        self.checks = []
        
    def check_not_null(self, column):
        null_count = self.df.filter(col(column).isNull()).count()
        self.checks.append({
            "check": f"{column}_not_null",
            "passed": null_count == 0,
            "details": f"Found {null_count} nulls"
        })
        return self
    
    def check_unique(self, column):
        total = self.df.count()
        unique = self.df.select(column).distinct().count()
        self.checks.append({
            "check": f"{column}_unique",
            "passed": total == unique,
            "details": f"Found {total - unique} duplicates"
        })
        return self
    
    def check_range(self, column, min_val, max_val):
        out_of_range = self.df.filter(
            (col(column) < min_val) | (col(column) > max_val)
        ).count()
        self.checks.append({
            "check": f"{column}_range_{min_val}_to_{max_val}",
            "passed": out_of_range == 0,
            "details": f"Found {out_of_range} out of range"
        })
        return self
    
    def check_format(self, column, pattern):
        invalid = self.df.filter(~col(column).rlike(pattern)).count()
        self.checks.append({
            "check": f"{column}_format",
            "passed": invalid == 0,
            "details": f"Found {invalid} invalid formats"
        })
        return self
    
    def check_values_in_list(self, column, valid_values):
        invalid = self.df.filter(~col(column).isin(valid_values)).count()
        self.checks.append({
            "check": f"{column}_in_list",
            "passed": invalid == 0,
            "details": f"Found {invalid} invalid values"
        })
        return self
    
    def get_results(self):
        return self.checks
    
    def assert_all_passed(self):
        failed = [c for c in self.checks if not c["passed"]]
        if failed:
            raise ValueError(f"Quality checks failed: {failed}")

# Exercise 51-60: Using DataQualityChecker
checker = DataQualityChecker(customers_df)
results = (checker
    .check_not_null("customer_id")
    .check_unique("customer_id")
    .check_range("age", 0, 120)
    .check_format("email", email_pattern)
    .check_values_in_list("state", ["CA", "NY", "TX", "FL"])
    .get_results())

for result in results:
    status = "✅" if result["passed"] else "❌"
    print(f"{status} {result['check']}: {result['details']}")

# Convert to DataFrame
results_df = spark.createDataFrame(results)
results_df.show(truncate=False)

# Save to quality metrics table
results_with_timestamp = results_df.withColumn("measured_at", current_timestamp())
# results_with_timestamp.write.mode("append").saveAsTable("quality_testing.quality_metrics")

# ============================================================================
# PART 7: DELTA LAKE CONSTRAINTS (5 exercises)
# ============================================================================

# Exercise 61-65: Delta Lake Constraints (SQL commands)
# Note: These are SQL commands, run them in SQL cells

"""
-- Exercise 61: Add NOT NULL constraint
ALTER TABLE quality_testing.customers_clean 
ALTER COLUMN customer_id SET NOT NULL;

-- Exercise 62: Add CHECK constraint for valid age
ALTER TABLE quality_testing.customers_clean 
ADD CONSTRAINT valid_age CHECK (age >= 0 AND age <= 120);

-- Exercise 63: Add CHECK constraint for valid amount
ALTER TABLE quality_testing.orders_clean 
ADD CONSTRAINT valid_amount CHECK (amount > 0);

-- Exercise 64: Try to insert invalid data (will fail)
INSERT INTO quality_testing.customers_clean VALUES
(999, 'Test User', 'test@email.com', -5, 'CA', '2024-01-01');
-- Error: CHECK constraint valid_age violated

-- Exercise 65: View all constraints
DESCRIBE EXTENDED quality_testing.customers_clean;
"""

# ============================================================================
# PART 8: DATA CLEANING AND QUARANTINE (10 exercises)
# ============================================================================

# Exercise 66: Separate good and bad customer data
good_customers = customers_df.filter(
    col("customer_id").isNotNull() &
    col("name").isNotNull() &
    col("email").isNotNull() &
    col("email").rlike(email_pattern) &
    col("age").between(0, 120) &
    col("state").isin(["CA", "NY", "TX", "FL"])
).dropDuplicates(["customer_id"])

bad_customers = customers_df.subtract(good_customers)

print(f"Good customers: {good_customers.count()}")
print(f"Bad customers: {bad_customers.count()}")

# Exercise 67: Write good data to customers_clean
# good_customers.write.mode("append").saveAsTable("quality_testing.customers_clean")

# Exercise 68: Write bad data to quarantine with reason
bad_with_reason = bad_customers.withColumn(
    "quarantine_reason",
    when(col("customer_id").isNull(), "Missing customer_id")
    .when(col("name").isNull(), "Missing name")
    .when(col("email").isNull(), "Missing email")
    .when(~col("email").rlike(email_pattern), "Invalid email format")
    .when((col("age") < 0) | (col("age") > 120), "Invalid age")
    .when(~col("state").isin(["CA", "NY", "TX", "FL"]), "Invalid state")
    .otherwise("Other quality issue")
).withColumn("quarantined_at", current_timestamp())

# bad_with_reason.write.mode("append").saveAsTable("quality_testing.quarantine_customers")

# Exercise 69: Clean email addresses
cleaned_emails = customers_df.withColumn(
    "email",
    lower(trim(col("email")))
)
cleaned_emails.select("email").show()

# Exercise 70: Standardize state codes
standardized_states = customers_df.withColumn(
    "state",
    upper(trim(col("state")))
)

# Exercise 71: Fix date formats
fixed_dates = orders_df.withColumn(
    "order_date",
    to_date(col("order_date"), "yyyy-MM-dd")
)

# Exercise 72: Handle null values - replace with median
from pyspark.sql.functions import percentile_approx
median_age = customers_df.select(
    percentile_approx("age", 0.5).alias("median")
).collect()[0]["median"]

filled_ages = customers_df.withColumn(
    "age",
    when(col("age").isNull(), median_age).otherwise(col("age"))
)

# Exercise 73: Remove duplicates
deduped_customers = customers_df.dropDuplicates(["customer_id"])

# Exercise 74: Validate and clean orders
clean_orders = orders_df.filter(
    col("order_id").isNotNull() &
    col("amount") > 0 &
    col("status").isin(["pending", "completed", "cancelled"])
).join(
    customers_df.select("customer_id"),
    "customer_id",
    "inner"
)

# Exercise 75: Create cleaning pipeline
def clean_customer_data(df):
    """Complete cleaning pipeline"""
    return (df
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("state", upper(trim(col("state"))))
        .filter(
            col("customer_id").isNotNull() &
            col("name").isNotNull() &
            col("email").rlike(email_pattern) &
            col("age").between(0, 120) &
            col("state").isin(["CA", "NY", "TX", "FL"])
        )
        .dropDuplicates(["customer_id"])
    )

cleaned_data = clean_customer_data(customers_df)
cleaned_data.show()

# ============================================================================
# PART 9: QUALITY METRICS AND MONITORING (10 exercises)
# ============================================================================

# Exercise 76: Calculate completeness metrics for all columns
def calculate_completeness_metrics(df, table_name):
    total_rows = df.count()
    metrics = []
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        completeness = (1 - null_count / total_rows) * 100
        metrics.append((table_name, column, "completeness", completeness, total_rows))
    return spark.createDataFrame(
        metrics,
        ["table_name", "column_name", "metric_name", "metric_value", "row_count"]
    )

completeness_metrics = calculate_completeness_metrics(customers_df, "customers_raw")
completeness_metrics.show()

# Exercise 77: Calculate accuracy metrics
def calculate_accuracy_metrics(df, table_name):
    total_rows = df.count()
    valid_ages = df.filter(col("age").between(0, 120)).count()
    valid_emails = df.filter(col("email").rlike(email_pattern)).count()
    
    metrics = [
        (table_name, "age", "accuracy", (valid_ages / total_rows) * 100, total_rows),
        (table_name, "email", "accuracy", (valid_emails / total_rows) * 100, total_rows)
    ]
    return spark.createDataFrame(
        metrics,
        ["table_name", "column_name", "metric_name", "metric_value", "row_count"]
    )

accuracy_metrics = calculate_accuracy_metrics(customers_df, "customers_raw")
accuracy_metrics.show()

# Exercise 78: Calculate uniqueness metrics
def calculate_uniqueness_metrics(df, table_name, key_column):
    total_rows = df.count()
    unique_rows = df.select(key_column).distinct().count()
    uniqueness = (unique_rows / total_rows) * 100
    
    return spark.createDataFrame(
        [(table_name, key_column, "uniqueness", uniqueness, total_rows)],
        ["table_name", "column_name", "metric_name", "metric_value", "row_count"]
    )

uniqueness_metrics = calculate_uniqueness_metrics(customers_df, "customers_raw", "customer_id")
uniqueness_metrics.show()

# Exercise 79: Store metrics with timestamp
all_metrics = completeness_metrics.union(accuracy_metrics).union(uniqueness_metrics)
all_metrics_with_ts = all_metrics.withColumn("measured_at", current_timestamp())
# all_metrics_with_ts.write.mode("append").saveAsTable("quality_testing.quality_metrics")

# Exercise 80: Create quality dashboard query
"""
SELECT 
    table_name,
    column_name,
    metric_name,
    metric_value,
    measured_at
FROM quality_testing.quality_metrics
WHERE measured_at >= current_date() - 7
ORDER BY table_name, column_name, measured_at DESC;
"""

# Exercise 81: Calculate quality score
def calculate_quality_score(df, table_name):
    """Calculate overall quality score (0-100)"""
    completeness = calculate_completeness_metrics(df, table_name)
    accuracy = calculate_accuracy_metrics(df, table_name)
    uniqueness = calculate_uniqueness_metrics(df, table_name, "customer_id")
    
    all_metrics = completeness.union(accuracy).union(uniqueness)
    avg_score = all_metrics.agg(avg("metric_value")).collect()[0][0]
    return avg_score

quality_score = calculate_quality_score(customers_df, "customers_raw")
print(f"Overall quality score: {quality_score:.2f}/100")

# Exercise 82: Identify quality trends
"""
SELECT 
    table_name,
    DATE(measured_at) as date,
    AVG(metric_value) as avg_quality
FROM quality_testing.quality_metrics
GROUP BY table_name, DATE(measured_at)
ORDER BY table_name, date;
"""

# Exercise 83: Create quality alert function
def check_quality_and_alert(df, table_name, threshold=95.0):
    """Alert if quality below threshold"""
    score = calculate_quality_score(df, table_name)
    if score < threshold:
        print(f"⚠️  ALERT: Quality score for {table_name} is {score:.2f}% (threshold: {threshold}%)")
        return False
    else:
        print(f"✅ Quality score for {table_name} is {score:.2f}%")
        return True

check_quality_and_alert(customers_df, "customers_raw", threshold=90.0)

# Exercise 84: Generate quality report
def generate_quality_report(df, table_name):
    """Generate comprehensive quality report"""
    total_rows = df.count()
    unique_rows = df.dropDuplicates().count()
    
    report = {
        "table_name": table_name,
        "total_rows": total_rows,
        "unique_rows": unique_rows,
        "duplicate_rows": total_rows - unique_rows,
        "quality_score": calculate_quality_score(df, table_name)
    }
    
    return report

report = generate_quality_report(customers_df, "customers_raw")
for key, value in report.items():
    print(f"{key}: {value}")

# Exercise 85: Visualize quality over time (query)
"""
SELECT 
    DATE(measured_at) as date,
    metric_name,
    AVG(metric_value) as avg_value
FROM quality_testing.quality_metrics
WHERE table_name = 'customers_raw'
GROUP BY DATE(measured_at), metric_name
ORDER BY date, metric_name;
"""

# ============================================================================
# PART 10: TESTING DATA TRANSFORMATIONS (5 exercises)
# ============================================================================

# Exercise 86: Unit test for email cleaning function
def clean_email(email):
    """Clean email address"""
    if email:
        return email.lower().strip()
    return None

# Test
test_emails = [("  JOHN@EMAIL.COM  ",), ("jane@EMAIL.com",)]
test_df = spark.createDataFrame(test_emails, ["email"])
result = test_df.withColumn("clean_email", lower(trim(col("email"))))
expected = [("john@email.com",), ("jane@email.com",)]
assert result.select("clean_email").collect() == [row for row in expected]
print("✅ Email cleaning test passed")

# Exercise 87: Test age validation function
def is_valid_age(age):
    """Validate age is between 0 and 120"""
    return age is not None and 0 <= age <= 120

# Test
assert is_valid_age(25) == True
assert is_valid_age(-5) == False
assert is_valid_age(150) == False
assert is_valid_age(None) == False
print("✅ Age validation test passed")

# Exercise 88: Test deduplication logic
test_data = [(1, "John"), (1, "John"), (2, "Jane")]
test_df = spark.createDataFrame(test_data, ["id", "name"])
deduped = test_df.dropDuplicates()
assert deduped.count() == 2
print("✅ Deduplication test passed")

# Exercise 89: Test referential integrity check
customers_test = spark.createDataFrame([(1,), (2,)], ["customer_id"])
orders_test = spark.createDataFrame([(1,), (2,), (3,)], ["customer_id"])
orphans = orders_test.join(customers_test, "customer_id", "left_anti")
assert orphans.count() == 1
print("✅ Referential integrity test passed")

# Exercise 90: Integration test for full pipeline
def test_full_pipeline():
    # Create test data
    test_customers = spark.createDataFrame([
        (1, "John Doe", "john@email.com", 25, "CA"),
        (2, "Jane Smith", "INVALID", 30, "NY")
    ], ["customer_id", "name", "email", "age", "state"])
    
    # Run cleaning pipeline
    cleaned = clean_customer_data(test_customers)
    
    # Verify results
    assert cleaned.count() == 1  # Only valid row
    assert cleaned.filter(col("customer_id") == 1).count() == 1
    print("✅ Full pipeline test passed")

test_full_pipeline()

# ============================================================================
# BONUS CHALLENGES
# ============================================================================

# Bonus 1: Automated quality check pipeline
def run_quality_pipeline(df, table_name):
    """Run complete quality check pipeline"""
    print(f"\n{'='*60}")
    print(f"Quality Report for {table_name}")
    print(f"{'='*60}\n")
    
    # Run checks
    checker = DataQualityChecker(df)
    results = (checker
        .check_not_null("customer_id")
        .check_unique("customer_id")
        .check_range("age", 0, 120)
        .check_format("email", email_pattern)
        .get_results())
    
    # Display results
    for result in results:
        status = "✅" if result["passed"] else "❌"
        print(f"{status} {result['check']}: {result['details']}")
    
    # Calculate score
    score = calculate_quality_score(df, table_name)
    print(f"\nOverall Quality Score: {score:.2f}/100")
    
    # Alert if needed
    check_quality_and_alert(df, table_name, threshold=90.0)
    
    return results

# Run pipeline
pipeline_results = run_quality_pipeline(customers_df, "customers_raw")

# Bonus 2-5: Additional implementations would go here
print("\n✅ All exercises completed!")
print("\nKey Takeaways:")
print("- Data quality has 6 dimensions: completeness, accuracy, consistency, timeliness, validity, uniqueness")
print("- Use assertions and constraints to enforce quality")
print("- Quarantine bad data for investigation")
print("- Monitor quality metrics over time")
print("- Test transformations thoroughly")
