# Day 13: Data Transformation Patterns - Solutions
# =========================================================

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Load sample data
df_messy = spark.read.csv("/FileStore/day13-transformation/messy_data.csv", header=True)
df_nulls = spark.read.csv("/FileStore/day13-transformation/null_data.csv", header=True)
df_dates = spark.read.csv("/FileStore/day13-transformation/date_data.csv", header=True)
df_duplicates = spark.read.csv("/FileStore/day13-transformation/duplicate_data.csv", header=True)

# =========================================================
# PART 1: NULL HANDLING
# =========================================================

# Exercise 1: Drop rows with any null
result = df_nulls.dropna()
display(result)

# Exercise 2: Drop rows with all nulls
result = df_nulls.dropna(how="all")
display(result)

# Exercise 3: Drop nulls in specific columns
result = df_nulls.dropna(subset=["email", "name"])
display(result)

# Exercise 4: Fill nulls with default values
result = df_nulls.fillna({"amount": 0, "category": "Unknown"})
display(result)

# Exercise 5: Fill nulls with column mean
mean_amount = df_nulls.select(mean("amount")).first()[0]
result = df_nulls.fillna({"amount": mean_amount})
display(result)

# Exercise 6: Use coalesce for null handling
result = df_nulls.withColumn("amount", coalesce(col("amount"), lit(0)))
display(result)

# Exercise 7: Identify null patterns
from pyspark.sql.functions import count, when, isnan, col
result = df_nulls.select([count(when(col(c).isNull(), c)).alias(c) for c in df_nulls.columns])
display(result)

# Exercise 8: Create null indicator columns
result = df_nulls.withColumn("amount_was_null", col("amount").isNull())
display(result)

# Exercise 9: Forward fill nulls
window_spec = Window.partitionBy("category").orderBy("id").rowsBetween(Window.unboundedPreceding, 0)
result = df_nulls.withColumn("amount_filled", last("amount", ignorenulls=True).over(window_spec))
display(result)

# Exercise 10: Conditional null filling
result = df_nulls.withColumn("amount",
    when(col("amount").isNull() & (col("category") == "A"), 1000)
    .when(col("amount").isNull() & (col("category") == "B"), 2000)
    .otherwise(col("amount"))
)
display(result)

# =========================================================
# PART 2: DUPLICATE REMOVAL
# =========================================================

# Exercise 11: Remove all duplicates
result = df_duplicates.distinct()
print(f"Original: {df_duplicates.count()}, After dedup: {result.count()}")

# Exercise 12: Remove duplicates by email
result = df_duplicates.dropDuplicates(["email"])
display(result)

# Exercise 13: Remove duplicates keeping latest
window_spec = Window.partitionBy("email").orderBy(col("timestamp").desc())
result = df_duplicates.withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")
display(result)

# Exercise 14: Identify duplicates
window_spec = Window.partitionBy("email")
result = df_duplicates.withColumn("duplicate_count", count("*").over(window_spec)) \
    .withColumn("is_duplicate", col("duplicate_count") > 1)
display(result)

# Exercise 15: Count duplicates per group
result = df_duplicates.groupBy("email").count().orderBy(desc("count"))
display(result)

# =========================================================
# PART 3: TYPE CONVERSIONS
# =========================================================

# Exercise 16: Cast string to integer
result = df_messy.withColumn("id", col("id").cast(IntegerType()))
result.printSchema()

# Exercise 17: Cast string to double
result = df_messy.withColumn("amount", col("amount").cast(DoubleType()))
display(result)

# Exercise 18: Safe type conversion
result = df_messy.withColumn("amount_safe",
    when(col("amount").cast(DoubleType()).isNotNull(), col("amount").cast(DoubleType()))
    .otherwise(None)
)
display(result)

# Exercise 19: Convert currency string to number
result = df_messy.withColumn("amount_clean",
    regexp_replace(col("amount"), "[$,]", "").cast(DoubleType())
)
display(result)

# Exercise 20: Convert percentage to decimal
result = df_messy.withColumn("amount_decimal",
    when(col("amount").contains("%"),
        regexp_replace(col("amount"), "%", "").cast(DoubleType()) / 100
    ).otherwise(col("amount").cast(DoubleType()))
)
display(result)

# Exercise 21: Boolean conversion
result = df_messy.withColumn("is_active",
    lower(trim(col("status"))) == "active"
)
display(result)

# Exercise 22: Parse JSON string (example)
json_data = spark.createDataFrame([('{"name":"Alice","age":30}',)], ["json_str"])
result = json_data.withColumn("parsed", from_json(col("json_str"), "name STRING, age INT"))
display(result)

# Exercise 23: Array to string
array_data = spark.createDataFrame([(["a", "b", "c"],)], ["items"])
result = array_data.withColumn("items_str", concat_ws(",", col("items")))
display(result)

# Exercise 24: String to array
result = df_messy.withColumn("name_parts", split(col("name"), " "))
display(result)

# Exercise 25: Type validation
result = df_messy.withColumn("amount_is_numeric",
    col("amount").cast(DoubleType()).isNotNull()
)
display(result)

# =========================================================
# PART 4: STRING CLEANING
# =========================================================

# Exercise 26: Trim whitespace
result = df_messy.withColumn("name", trim(col("name")))
display(result)

# Exercise 27: Convert to lowercase
result = df_messy.withColumn("email", lower(col("email")))
display(result)

# Exercise 28: Convert to title case
result = df_messy.withColumn("name", initcap(col("name")))
display(result)

# Exercise 29: Remove special characters
result = df_messy.withColumn("phone_clean", regexp_replace(col("phone"), "[^0-9]", ""))
display(result)

# Exercise 30: Extract domain from email
result = df_messy.withColumn("email_domain", regexp_extract(col("email"), "@(.+)", 1))
display(result)

# Exercise 31: Format phone number
result = df_messy.withColumn("phone_clean", regexp_replace(col("phone"), "[^0-9]", "")) \
    .withColumn("phone_formatted",
        concat(
            substring(col("phone_clean"), 1, 3),
            lit("-"),
            substring(col("phone_clean"), 4, 3),
            lit("-"),
            substring(col("phone_clean"), 7, 4)
        )
    )
display(result)

# Exercise 32: Standardize status values
result = df_messy.withColumn("status", lower(trim(col("status"))))
display(result)

# Exercise 33: Replace multiple values
result = df_nulls.replace(["NULL", "NA", "N/A", "null", "None"], None)
display(result)

# Exercise 34: Concatenate columns
result = df_messy.withColumn("full_name", concat(col("name"), lit(" - "), col("email")))
display(result)

# Exercise 35: Extract substring
result = df_messy.withColumn("phone_clean", regexp_replace(col("phone"), "[^0-9]", "")) \
    .withColumn("area_code", substring(col("phone_clean"), 1, 3))
display(result)

# =========================================================
# PART 5: DATE TRANSFORMATIONS
# =========================================================

# Exercise 36: Parse date with format
result = df_dates.withColumn("date1_parsed", to_date(col("date1"), "yyyy-MM-dd"))
display(result)

# Exercise 37: Parse multiple date formats
result = df_dates.withColumn("date_parsed",
    coalesce(
        to_date(col("date1"), "yyyy-MM-dd"),
        to_date(col("date2"), "MM/dd/yyyy"),
        to_date(col("date3"), "dd-MMM-yyyy")
    )
)
display(result)

# Exercise 38: Extract year from date
result = df_dates.withColumn("date_parsed", to_date(col("date1"), "yyyy-MM-dd")) \
    .withColumn("year", year(col("date_parsed")))
display(result)

# Exercise 39: Calculate age from birthdate
birthdate_data = spark.createDataFrame([("1990-01-15",)], ["birthdate"])
result = birthdate_data.withColumn("birthdate_parsed", to_date(col("birthdate"), "yyyy-MM-dd")) \
    .withColumn("age", floor(datediff(current_date(), col("birthdate_parsed")) / 365.25))
display(result)

# Exercise 40: Add days to date
result = df_dates.withColumn("date_parsed", to_date(col("date1"), "yyyy-MM-dd")) \
    .withColumn("date_plus_30", date_add(col("date_parsed"), 30))
display(result)

# Exercise 41: Calculate date difference
result = df_dates.withColumn("date1_parsed", to_date(col("date1"), "yyyy-MM-dd")) \
    .withColumn("date2_parsed", to_date(col("date2"), "MM/dd/yyyy")) \
    .withColumn("days_diff", datediff(col("date2_parsed"), col("date1_parsed")))
display(result)

# Exercise 42: Format date for display
result = df_dates.withColumn("date_parsed", to_date(col("date1"), "yyyy-MM-dd")) \
    .withColumn("date_formatted", date_format(col("date_parsed"), "MM/dd/yyyy"))
display(result)

# Exercise 43: Extract day of week
result = df_dates.withColumn("date_parsed", to_date(col("date1"), "yyyy-MM-dd")) \
    .withColumn("day_of_week", dayofweek(col("date_parsed"))) \
    .withColumn("day_name", date_format(col("date_parsed"), "EEEE"))
display(result)

# Exercise 44: Parse timestamp
result = df_dates.withColumn("timestamp_parsed",
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
)
display(result)

# Exercise 45: Add current timestamp
result = df_dates.withColumn("processed_at", current_timestamp())
display(result)

# =========================================================
# PART 6: DATA VALIDATION
# =========================================================

# Exercise 46: Validate email format
result = df_messy.withColumn("email_valid",
    col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
)
display(result)

# Exercise 47: Validate phone format
result = df_messy.withColumn("phone_clean", regexp_replace(col("phone"), "[^0-9]", "")) \
    .withColumn("phone_valid", length(col("phone_clean")) == 10)
display(result)

# Exercise 48: Validate amount range
result = df_messy.withColumn("amount_num", col("amount").cast(DoubleType())) \
    .withColumn("amount_valid",
        (col("amount_num") >= 0) & (col("amount_num") <= 10000)
    )
display(result)

# Exercise 49: Validate required fields
result = df_messy.withColumn("all_required_present",
    col("id").isNotNull() &
    col("name").isNotNull() &
    col("email").isNotNull()
)
display(result)

# Exercise 50: Complex business rule validation
result = df_messy.withColumn("is_valid",
    col("id").isNotNull() &
    col("name").isNotNull() &
    col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$") &
    (col("amount").cast(DoubleType()) > 0) &
    lower(trim(col("status"))).isin(["active", "pending", "completed", "cancelled"])
)
display(result)

print("All solutions completed!")
