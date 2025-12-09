# Day 13: Data Transformation Patterns

## Learning Objectives

By the end of today, you will:
- Master data cleaning techniques (nulls, duplicates, outliers)
- Perform type conversions and casting operations
- Implement string manipulation and parsing
- Handle date and timestamp transformations
- Apply data normalization and standardization
- Use regular expressions for pattern matching
- Build reusable transformation functions
- Validate and ensure data quality

## Topics Covered

### 1. Data Cleaning Fundamentals

Data cleaning is the process of detecting and correcting (or removing) corrupt or inaccurate records.

#### Handling Null Values

```python
from pyspark.sql.functions import col, when, coalesce, isnan, isnull

# Drop rows with nulls
df.dropna()  # Drop rows with any null
df.dropna(how="all")  # Drop rows where all values are null
df.dropna(subset=["col1", "col2"])  # Drop if null in specific columns
df.dropna(thresh=3)  # Drop if less than 3 non-null values

# Fill null values
df.fillna(0)  # Fill all nulls with 0
df.fillna({"age": 0, "name": "Unknown"})  # Column-specific fills
df.na.fill({"salary": 0, "bonus": 0})  # Alternative syntax

# Replace nulls with column values
df.withColumn("amount", coalesce(col("amount"), lit(0)))

# Check for nulls
df.filter(col("email").isNull())
df.filter(col("email").isNotNull())
```

#### Removing Duplicates

```python
# Remove all duplicate rows
df.distinct()

# Remove duplicates based on specific columns
df.dropDuplicates(["email"])
df.dropDuplicates(["customer_id", "order_date"])

# Keep first/last occurrence
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("email").orderBy(col("timestamp").desc())
df_dedup = df.withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")
```

#### Handling Outliers

```python
from pyspark.sql.functions import percentile_approx, stddev, mean

# IQR method
quantiles = df.approxQuantile("amount", [0.25, 0.75], 0.01)
Q1, Q3 = quantiles[0], quantiles[1]
IQR = Q3 - Q1
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

df_no_outliers = df.filter(
    (col("amount") >= lower_bound) & (col("amount") <= upper_bound)
)

# Z-score method
stats = df.select(mean("amount"), stddev("amount")).first()
mean_val, std_val = stats[0], stats[1]

df_no_outliers = df.withColumn(
    "z_score",
    (col("amount") - mean_val) / std_val
).filter((col("z_score") >= -3) & (col("z_score") <= 3))
```

### 2. Type Conversions and Casting

#### Basic Type Casting

```python
from pyspark.sql.types import IntegerType, DoubleType, StringType, DateType

# Cast to different types
df.withColumn("age", col("age").cast(IntegerType()))
df.withColumn("amount", col("amount").cast(DoubleType()))
df.withColumn("id", col("id").cast(StringType()))

# Alternative syntax
df.withColumn("age", col("age").cast("int"))
df.withColumn("amount", col("amount").cast("double"))
df.withColumn("date", col("date").cast("date"))
```

#### Safe Type Conversion

```python
# Handle conversion errors
df.withColumn("age_int", 
    when(col("age").cast("int").isNotNull(), col("age").cast("int"))
    .otherwise(None)
)

# Try-catch pattern with UDF
from pyspark.sql.functions import udf

@udf(IntegerType())
def safe_int_cast(value):
    try:
        return int(value) if value else None
    except:
        return None

df.withColumn("age_safe", safe_int_cast(col("age")))
```

#### String to Numeric

```python
from pyspark.sql.functions import regexp_replace

# Remove currency symbols and convert
df.withColumn("amount_clean", 
    regexp_replace(col("amount"), "[$,]", "").cast("double")
)

# Handle percentages
df.withColumn("rate_decimal",
    regexp_replace(col("rate"), "%", "").cast("double") / 100
)
```

### 3. String Manipulation

#### Basic String Operations

```python
from pyspark.sql.functions import upper, lower, initcap, trim, ltrim, rtrim
from pyspark.sql.functions import concat, concat_ws, substring, length

# Case conversion
df.withColumn("name_upper", upper(col("name")))
df.withColumn("name_lower", lower(col("name")))
df.withColumn("name_title", initcap(col("name")))

# Trimming whitespace
df.withColumn("email_clean", trim(col("email")))
df.withColumn("name_ltrim", ltrim(col("name")))
df.withColumn("name_rtrim", rtrim(col("name")))

# Concatenation
df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
df.withColumn("address", concat_ws(", ", col("street"), col("city"), col("state")))

# Substring and length
df.withColumn("area_code", substring(col("phone"), 1, 3))
df.withColumn("name_length", length(col("name")))
```

#### Advanced String Operations

```python
from pyspark.sql.functions import split, regexp_extract, regexp_replace

# Split strings
df.withColumn("name_parts", split(col("full_name"), " "))
df.withColumn("first_name", split(col("full_name"), " ").getItem(0))

# Regular expressions
df.withColumn("email_domain", regexp_extract(col("email"), "@(.+)", 1))
df.withColumn("phone_clean", regexp_replace(col("phone"), "[^0-9]", ""))

# Pattern matching
df.filter(col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"))
```

### 4. Date and Timestamp Transformations

#### Date Parsing

```python
from pyspark.sql.functions import to_date, to_timestamp, date_format

# Parse dates
df.withColumn("date_parsed", to_date(col("date_string"), "yyyy-MM-dd"))
df.withColumn("date_parsed", to_date(col("date_string"), "MM/dd/yyyy"))

# Parse timestamps
df.withColumn("timestamp_parsed", 
    to_timestamp(col("timestamp_string"), "yyyy-MM-dd HH:mm:ss")
)

# Format dates
df.withColumn("date_formatted", date_format(col("date"), "MM/dd/yyyy"))
```

#### Date Arithmetic

```python
from pyspark.sql.functions import date_add, date_sub, datediff, months_between
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, dayofyear

# Date arithmetic
df.withColumn("next_week", date_add(col("date"), 7))
df.withColumn("last_month", date_sub(col("date"), 30))
df.withColumn("days_diff", datediff(col("end_date"), col("start_date")))
df.withColumn("months_diff", months_between(col("end_date"), col("start_date")))

# Extract date parts
df.withColumn("year", year(col("date")))
df.withColumn("month", month(col("date")))
df.withColumn("day", dayofmonth(col("date")))
df.withColumn("day_of_week", dayofweek(col("date")))
```

#### Current Date/Time

```python
from pyspark.sql.functions import current_date, current_timestamp, now

# Add current date/time
df.withColumn("processed_date", current_date())
df.withColumn("processed_timestamp", current_timestamp())
```

### 5. Data Normalization and Standardization

#### Min-Max Normalization

```python
from pyspark.sql.functions import min as spark_min, max as spark_max

# Calculate min and max
stats = df.select(spark_min("amount"), spark_max("amount")).first()
min_val, max_val = stats[0], stats[1]

# Normalize to 0-1 range
df_normalized = df.withColumn(
    "amount_normalized",
    (col("amount") - min_val) / (max_val - min_val)
)
```

#### Z-Score Standardization

```python
from pyspark.sql.functions import mean, stddev

# Calculate mean and stddev
stats = df.select(mean("amount"), stddev("amount")).first()
mean_val, std_val = stats[0], stats[1]

# Standardize
df_standardized = df.withColumn(
    "amount_standardized",
    (col("amount") - mean_val) / std_val
)
```

#### Categorical Encoding

```python
from pyspark.ml.feature import StringIndexer, OneHotEncoder

# Label encoding
indexer = StringIndexer(inputCol="category", outputCol="category_index")
df_indexed = indexer.fit(df).transform(df)

# One-hot encoding
encoder = OneHotEncoder(inputCol="category_index", outputCol="category_vector")
df_encoded = encoder.fit(df_indexed).transform(df_indexed)
```

### 6. Data Validation

#### Range Validation

```python
# Validate numeric ranges
df.withColumn("is_valid_age", 
    when((col("age") >= 0) & (col("age") <= 120), True).otherwise(False)
)

# Validate date ranges
df.withColumn("is_valid_date",
    when((col("date") >= "2020-01-01") & (col("date") <= current_date()), True)
    .otherwise(False)
)
```

#### Format Validation

```python
# Email validation
df.withColumn("is_valid_email",
    col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
)

# Phone validation
df.withColumn("is_valid_phone",
    col("phone").rlike("^\\d{3}-\\d{3}-\\d{4}$")
)

# ZIP code validation
df.withColumn("is_valid_zip",
    col("zip").rlike("^\\d{5}(-\\d{4})?$")
)
```

#### Business Rule Validation

```python
# Complex validation rules
df.withColumn("is_valid",
    when(
        (col("amount") > 0) &
        (col("quantity") > 0) &
        (col("price") == col("amount") / col("quantity")) &
        (col("status").isin(["pending", "completed", "cancelled"])),
        True
    ).otherwise(False)
)
```

### 7. Conditional Transformations

#### Simple Conditions

```python
from pyspark.sql.functions import when, otherwise

# Single condition
df.withColumn("category",
    when(col("amount") > 1000, "High").otherwise("Low")
)

# Multiple conditions
df.withColumn("tier",
    when(col("amount") > 10000, "Platinum")
    .when(col("amount") > 5000, "Gold")
    .when(col("amount") > 1000, "Silver")
    .otherwise("Bronze")
)
```

#### Complex Conditions

```python
# Multiple column conditions
df.withColumn("discount",
    when((col("amount") > 1000) & (col("customer_type") == "Premium"), 0.20)
    .when((col("amount") > 1000) & (col("customer_type") == "Regular"), 0.10)
    .when(col("amount") > 500, 0.05)
    .otherwise(0.0)
)
```

### 8. Aggregation Transformations

#### Group-wise Transformations

```python
from pyspark.sql.window import Window

# Calculate group statistics
window_spec = Window.partitionBy("category")

df.withColumn("category_avg", avg("amount").over(window_spec)) \
  .withColumn("category_max", max("amount").over(window_spec)) \
  .withColumn("amount_vs_avg", col("amount") - col("category_avg"))
```

#### Ranking and Percentiles

```python
from pyspark.sql.functions import percent_rank, ntile

window_spec = Window.partitionBy("category").orderBy(col("amount").desc())

df.withColumn("percentile", percent_rank().over(window_spec)) \
  .withColumn("quartile", ntile(4).over(window_spec))
```

### 9. Reusable Transformation Functions

#### Creating Transformation Functions

```python
def clean_email(df, email_col="email"):
    """Clean and validate email addresses"""
    return df.withColumn(email_col, lower(trim(col(email_col)))) \
        .withColumn(f"{email_col}_valid",
            col(email_col).rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
        )

def clean_phone(df, phone_col="phone"):
    """Clean and format phone numbers"""
    return df.withColumn(phone_col,
        regexp_replace(col(phone_col), "[^0-9]", "")
    ).withColumn(f"{phone_col}_formatted",
        concat(
            substring(col(phone_col), 1, 3),
            lit("-"),
            substring(col(phone_col), 4, 3),
            lit("-"),
            substring(col(phone_col), 7, 4)
        )
    )

def add_audit_columns(df):
    """Add audit columns to DataFrame"""
    return df.withColumn("processed_at", current_timestamp()) \
        .withColumn("processed_by", lit("etl_pipeline"))
```

## Hands-On Exercises

See `exercise.py` for 60 practical exercises covering:
- Null handling and imputation
- Duplicate removal strategies
- Type conversions and casting
- String cleaning and parsing
- Date/timestamp transformations
- Data normalization
- Validation rules
- Reusable transformation functions

## Key Takeaways

1. Always handle nulls explicitly - don't assume data is clean
2. Use appropriate type casting with error handling
3. Regular expressions are powerful for pattern matching and extraction
4. Date parsing requires correct format specification
5. Normalize/standardize data for consistent analysis
6. Validate data against business rules
7. Build reusable transformation functions for consistency
8. Test transformations with edge cases

## Exam Tips

- Know common string functions (upper, lower, trim, concat, substring)
- Understand date functions (to_date, date_format, datediff)
- Remember type casting syntax: col("x").cast("int")
- Know how to handle nulls (dropna, fillna, coalesce)
- Understand when vs otherwise for conditional logic
- Be familiar with regexp_extract and regexp_replace
- Know how to remove duplicates (distinct, dropDuplicates)
- Understand window functions for group-wise transformations
- Remember that transformations are lazy (not executed until action)
- Know how to validate data formats (email, phone, etc.)

## Common Pitfalls

1. Not handling null values before type conversion
2. Forgetting to escape special characters in regex
3. Using wrong date format in to_date()
4. Not trimming whitespace before comparisons
5. Assuming data types without checking schema
6. Not validating data after transformation
7. Hardcoding values instead of using parameters
8. Not testing with edge cases (nulls, empty strings, etc.)
9. Forgetting that strings are case-sensitive
10. Not documenting transformation logic

## Additional Resources

- [PySpark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [Data Cleaning Best Practices](https://docs.databricks.com/data/best-practices.html)
- [Regular Expressions Guide](https://docs.python.org/3/library/re.html)
- [Date/Time Functions](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)

## Next Steps

Tomorrow: [Day 14 - ELT Best Practices](../day-14-elt-best-practices/README.md)

Master these transformation patterns - they're essential for building robust data pipelines!
