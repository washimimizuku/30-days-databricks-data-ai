# Day 26: Data Quality & Testing

## Overview

Today you'll learn how to implement data quality checks and testing strategies for your data pipelines. Ensuring data quality is critical for reliable analytics and ML models.

**Time**: 1.5 hours  
**Focus**: Data validation, quality checks, testing frameworks, expectations

## Learning Objectives

By the end of today, you will be able to:
- Implement data quality checks in Spark
- Use assertions and constraints
- Validate data schemas and formats
- Test data transformations
- Monitor data quality metrics
- Handle data quality failures
- Use Great Expectations (conceptually)
- Build data quality dashboards

## Why Data Quality Matters

Poor data quality leads to:
- ❌ Incorrect business decisions
- ❌ Failed ML models
- ❌ Lost customer trust
- ❌ Compliance violations
- ❌ Wasted engineering time

Good data quality ensures:
- ✅ Reliable analytics
- ✅ Accurate ML predictions
- ✅ Regulatory compliance
- ✅ Business confidence
- ✅ Reduced debugging time

## Data Quality Dimensions

### 1. Completeness
Data has all required values

```python
# Check for null values
from pyspark.sql.functions import col, count, when

df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df.columns
]).show()

# Completeness percentage
total_rows = df.count()
for column in df.columns:
    null_count = df.filter(col(column).isNull()).count()
    completeness = (1 - null_count / total_rows) * 100
    print(f"{column}: {completeness:.2f}% complete")
```

### 2. Accuracy
Data correctly represents reality

```python
# Check value ranges
df.filter(
    (col("age") < 0) | (col("age") > 120)
).count()  # Should be 0

# Check against reference data
valid_states = ["CA", "NY", "TX", "FL"]
invalid_states = df.filter(
    ~col("state").isin(valid_states)
).count()
```

### 3. Consistency
Data is consistent across sources

```python
# Check referential integrity
orders_without_customers = orders.join(
    customers,
    orders.customer_id == customers.customer_id,
    "left_anti"
)
orphan_count = orders_without_customers.count()
```

### 4. Timeliness
Data is up-to-date

```python
from pyspark.sql.functions import current_timestamp, datediff

# Check data freshness
df.select(
    datediff(current_timestamp(), col("updated_at")).alias("days_old")
).filter(col("days_old") > 7).count()
```

### 5. Validity
Data conforms to business rules

```python
# Email format validation
df.filter(
    ~col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
).count()

# Phone format validation
df.filter(
    ~col("phone").rlike("^\\d{3}-\\d{3}-\\d{4}$")
).count()
```

### 6. Uniqueness
No duplicate records

```python
# Check for duplicates
total_rows = df.count()
unique_rows = df.dropDuplicates().count()
duplicate_count = total_rows - unique_rows

# Find duplicate keys
df.groupBy("customer_id").count().filter(col("count") > 1).show()
```

## Data Quality Checks in Spark

### Basic Assertions

```python
# Assert no nulls in critical columns
assert df.filter(col("customer_id").isNull()).count() == 0, \
    "customer_id cannot be null"

# Assert value ranges
assert df.filter(col("amount") < 0).count() == 0, \
    "amount must be positive"

# Assert row count
expected_count = 1000
actual_count = df.count()
assert actual_count == expected_count, \
    f"Expected {expected_count} rows, got {actual_count}"
```

### Schema Validation

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define expected schema
expected_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), True)
])

# Validate schema
def validate_schema(df, expected_schema):
    if df.schema != expected_schema:
        raise ValueError(f"Schema mismatch: {df.schema} != {expected_schema}")
    return True

validate_schema(df, expected_schema)
```

### Custom Quality Checks

```python
class DataQualityChecker:
    def __init__(self, df):
        self.df = df
        self.checks = []
        
    def check_not_null(self, column):
        """Check column has no nulls"""
        null_count = self.df.filter(col(column).isNull()).count()
        self.checks.append({
            "check": f"{column}_not_null",
            "passed": null_count == 0,
            "details": f"Found {null_count} nulls"
        })
        return self
    
    def check_unique(self, column):
        """Check column has unique values"""
        total = self.df.count()
        unique = self.df.select(column).distinct().count()
        self.checks.append({
            "check": f"{column}_unique",
            "passed": total == unique,
            "details": f"Found {total - unique} duplicates"
        })
        return self
    
    def check_range(self, column, min_val, max_val):
        """Check column values in range"""
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
        """Check column matches regex pattern"""
        invalid = self.df.filter(~col(column).rlike(pattern)).count()
        self.checks.append({
            "check": f"{column}_format",
            "passed": invalid == 0,
            "details": f"Found {invalid} invalid formats"
        })
        return self
    
    def get_results(self):
        """Return check results"""
        return self.checks
    
    def assert_all_passed(self):
        """Raise error if any check failed"""
        failed = [c for c in self.checks if not c["passed"]]
        if failed:
            raise ValueError(f"Quality checks failed: {failed}")

# Usage
checker = DataQualityChecker(df)
results = (checker
    .check_not_null("customer_id")
    .check_unique("customer_id")
    .check_range("age", 0, 120)
    .check_format("email", "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
    .get_results())

for result in results:
    print(f"{result['check']}: {'✅' if result['passed'] else '❌'} - {result['details']}")
```

## Delta Lake Constraints

Delta Lake supports table constraints for data quality:

```sql
-- Add NOT NULL constraint
ALTER TABLE customers 
ALTER COLUMN customer_id SET NOT NULL;

-- Add CHECK constraint
ALTER TABLE orders 
ADD CONSTRAINT valid_amount CHECK (amount > 0);

-- Add CHECK constraint with multiple conditions
ALTER TABLE customers 
ADD CONSTRAINT valid_email CHECK (
    email IS NOT NULL AND 
    email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
);

-- View constraints
DESCRIBE EXTENDED customers;

-- Drop constraint
ALTER TABLE orders DROP CONSTRAINT valid_amount;
```

Constraint violations will cause writes to fail:

```python
# This will fail if amount <= 0
df_with_invalid = spark.createDataFrame([
    (1, -100.00)
], ["order_id", "amount"])

df_with_invalid.write.mode("append").saveAsTable("orders")
# Error: CHECK constraint valid_amount violated
```

## Testing Data Transformations

### Unit Testing

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

def clean_customer_data(df):
    """Clean customer data"""
    from pyspark.sql.functions import lower, trim
    return df.select(
        col("customer_id"),
        trim(lower(col("email"))).alias("email"),
        col("name")
    )

def test_clean_customer_data(spark):
    # Arrange
    input_data = [
        (1, "  JOHN@EMAIL.COM  ", "John"),
        (2, "jane@EMAIL.com", "Jane")
    ]
    input_df = spark.createDataFrame(input_data, ["customer_id", "email", "name"])
    
    # Act
    result_df = clean_customer_data(input_df)
    
    # Assert
    expected_data = [
        (1, "john@email.com", "John"),
        (2, "jane@email.com", "Jane")
    ]
    expected_df = spark.createDataFrame(expected_data, ["customer_id", "email", "name"])
    
    assert result_df.collect() == expected_df.collect()
```

### Integration Testing

```python
def test_end_to_end_pipeline(spark):
    # Setup test data
    bronze_df = spark.createDataFrame([
        (1, "John", "john@email.com", 100.00),
        (2, "Jane", "jane@email.com", 200.00)
    ], ["id", "name", "email", "amount"])
    
    # Run pipeline
    silver_df = clean_data(bronze_df)
    gold_df = aggregate_data(silver_df)
    
    # Verify results
    assert gold_df.count() == 2
    assert gold_df.filter(col("amount") < 0).count() == 0
```

## Data Quality Metrics

### Track Quality Over Time

```python
from pyspark.sql.functions import current_timestamp

def calculate_quality_metrics(df, table_name):
    """Calculate quality metrics for a table"""
    total_rows = df.count()
    
    metrics = []
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        completeness = (1 - null_count / total_rows) * 100
        
        metrics.append({
            "table_name": table_name,
            "column_name": column,
            "metric_name": "completeness",
            "metric_value": completeness,
            "row_count": total_rows,
            "measured_at": current_timestamp()
        })
    
    return spark.createDataFrame(metrics)

# Store metrics
quality_metrics = calculate_quality_metrics(df, "customers")
quality_metrics.write.mode("append").saveAsTable("data_quality_metrics")
```

### Quality Dashboard Query

```sql
-- Latest quality metrics
SELECT 
    table_name,
    column_name,
    metric_value as completeness_pct,
    measured_at
FROM data_quality_metrics
WHERE metric_name = 'completeness'
    AND measured_at >= current_date() - 7
ORDER BY table_name, column_name, measured_at DESC;

-- Quality trends
SELECT 
    table_name,
    DATE(measured_at) as date,
    AVG(metric_value) as avg_completeness
FROM data_quality_metrics
WHERE metric_name = 'completeness'
GROUP BY table_name, DATE(measured_at)
ORDER BY table_name, date;
```

## Handling Quality Issues

### Quarantine Bad Data

```python
# Separate good and bad data
good_data = df.filter(
    col("customer_id").isNotNull() &
    (col("amount") > 0) &
    col("email").rlike("^[a-zA-Z0-9._%+-]+@")
)

bad_data = df.subtract(good_data)

# Write to separate tables
good_data.write.mode("append").saveAsTable("silver.customers")
bad_data.write.mode("append").saveAsTable("quarantine.customers")
```

### Data Quality Alerts

```python
def check_and_alert(df, table_name, threshold=0.95):
    """Alert if quality below threshold"""
    total_rows = df.count()
    
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        completeness = (1 - null_count / total_rows)
        
        if completeness < threshold:
            alert_message = f"""
            ALERT: Data Quality Issue
            Table: {table_name}
            Column: {column}
            Completeness: {completeness:.2%}
            Threshold: {threshold:.2%}
            """
            print(alert_message)
            # In production: send to monitoring system
```

## Best Practices

### 1. Define Quality Rules Early

```python
# Document quality rules
QUALITY_RULES = {
    "customers": {
        "customer_id": {"not_null": True, "unique": True},
        "email": {"not_null": True, "format": "email"},
        "age": {"range": (0, 120)},
        "state": {"values": ["CA", "NY", "TX", "FL"]}
    },
    "orders": {
        "order_id": {"not_null": True, "unique": True},
        "amount": {"range": (0, 1000000)},
        "status": {"values": ["pending", "completed", "cancelled"]}
    }
}
```

### 2. Automate Quality Checks

```python
def run_quality_checks(df, table_name):
    """Run all quality checks for a table"""
    rules = QUALITY_RULES.get(table_name, {})
    checker = DataQualityChecker(df)
    
    for column, checks in rules.items():
        if checks.get("not_null"):
            checker.check_not_null(column)
        if checks.get("unique"):
            checker.check_unique(column)
        if "range" in checks:
            min_val, max_val = checks["range"]
            checker.check_range(column, min_val, max_val)
    
    return checker.get_results()
```

### 3. Monitor Quality Trends

Track quality metrics over time to detect degradation.

### 4. Fail Fast

Stop pipeline execution on critical quality failures.

### 5. Document Quality Issues

Log all quality issues for analysis and improvement.

## Exam Tips

### Key Concepts

1. **Data Quality Dimensions**: Completeness, accuracy, consistency, timeliness, validity, uniqueness
2. **Delta Constraints**: CHECK and NOT NULL constraints
3. **Quality Checks**: Assertions, schema validation, custom checks
4. **Testing**: Unit tests for transformations, integration tests for pipelines
5. **Monitoring**: Track quality metrics over time

### Common Exam Questions

**Q: How do you add a constraint to a Delta table?**
A: `ALTER TABLE table_name ADD CONSTRAINT constraint_name CHECK (condition)`

**Q: What happens when a constraint is violated?**
A: The write operation fails with an error

**Q: How do you check for null values in a column?**
A: `df.filter(col("column_name").isNull()).count()`

**Q: What's the difference between unit and integration testing?**
A: Unit tests test individual functions, integration tests test entire pipelines

## Summary

Today you learned:
- ✅ Six dimensions of data quality
- ✅ Implementing quality checks in Spark
- ✅ Using Delta Lake constraints
- ✅ Testing data transformations
- ✅ Monitoring quality metrics
- ✅ Handling quality failures
- ✅ Best practices for data quality

## Next Steps

Tomorrow (Day 27) you'll learn about:
- Production best practices
- Error handling and retry logic
- Monitoring and alerting
- Performance optimization
- Deployment strategies

## Additional Resources

- [Delta Lake Constraints](https://docs.databricks.com/delta/delta-constraints.html)
- [Great Expectations](https://greatexpectations.io/)
- [Data Quality Testing](https://docs.databricks.com/data-governance/data-quality.html)

---

**Estimated Time**: 1.5 hours  
**Difficulty**: Intermediate  
**Prerequisites**: Days 1-25
