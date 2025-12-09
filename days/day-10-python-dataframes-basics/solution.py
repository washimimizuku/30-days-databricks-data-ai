# Day 10: Python DataFrames Basics - Solutions
# =========================================================

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, lit, when, avg, sum, count, max, min, countDistinct
from pyspark.sql.functions import desc, asc, upper, lower, concat, substring, length, trim
from pyspark.sql.functions import year, month, datediff, current_date, to_date, round as spark_round
from pyspark.sql.functions import abs as spark_abs, array, array_contains, size

# =========================================================
# PART 1: CREATING DATAFRAMES
# =========================================================

# Exercise 1: Create DataFrame from List of Tuples
data = [(1, "Alice", 30), (2, "Bob", 25), (3, "Charlie", 35)]
df1 = spark.createDataFrame(data, ["id", "name", "age"])
display(df1)

# Exercise 2: Create DataFrame from List of Rows
data = [
    Row(id=101, product="Laptop", price=999.99),
    Row(id=102, product="Mouse", price=29.99),
    Row(id=103, product="Keyboard", price=79.99)
]
df2 = spark.createDataFrame(data)
display(df2)

# Exercise 3: Create DataFrame with Explicit Schema
schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("salary", DoubleType(), nullable=True)
])

data = [(1, "Alice", 95000.0), (2, "Bob", 75000.0)]
df3 = spark.createDataFrame(data, schema)
df3.printSchema()
display(df3)

# Exercise 4: Create DataFrame from Dictionary
data_dict = {
    "name": ["Alice", "Bob", "Charlie"],
    "age": [30, 25, 35],
    "city": ["Seattle", "New York", "Los Angeles"]
}
df4 = spark.createDataFrame([(k, v) for k, v in zip(data_dict["name"], data_dict["age"], data_dict["city"])], 
                            ["name", "age", "city"])
display(df4)

# Exercise 5: Create Empty DataFrame
schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True)
])
df5 = spark.createDataFrame([], schema)
display(df5)
print(f"Empty DataFrame with {df5.count()} rows")


# =========================================================
# PART 2: READING DATA
# =========================================================

# Exercise 6: Read CSV with Header
df_employees = spark.read.csv(
    "/FileStore/day10-exercises/employees.csv",
    header=True,
    inferSchema=True
)
display(df_employees)

# Exercise 7: Read CSV with Explicit Schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("hire_date", StringType(), True),
    StructField("city", StringType(), True)
])

df_employees_schema = spark.read.csv(
    "/FileStore/day10-exercises/employees.csv",
    header=True,
    schema=schema
)
df_employees_schema.printSchema()

# Exercise 8: Read JSON File
df_products = spark.read.json("/FileStore/day10-exercises/products.json")
display(df_products)

# Exercise 9: Read Multiple CSV Files
df_all_csv = spark.read.csv(
    "/FileStore/day10-exercises/*.csv",
    header=True,
    inferSchema=True
)
print(f"Total rows from all CSV files: {df_all_csv.count()}")

# Exercise 10: Read CSV with Options
df_custom = spark.read.csv(
    "/FileStore/day10-exercises/employees.csv",
    header=True,
    inferSchema=True,
    sep=",",
    quote='"',
    escape="\\",
    nullValue="NULL"
)
display(df_custom)

# =========================================================
# PART 3: INSPECTING DATAFRAMES
# =========================================================

# Exercise 11: Display DataFrame Schema
df_employees.printSchema()

# Exercise 12: Show First Rows
df_employees.show(5)
display(df_employees.limit(5))

# Exercise 13: Get DataFrame Dimensions
row_count = df_employees.count()
col_count = len(df_employees.columns)
print(f"DataFrame dimensions: {row_count} rows x {col_count} columns")

# Exercise 14: Get Column Names and Types
print("Column names:", df_employees.columns)
print("Column types:", df_employees.dtypes)

# Exercise 15: Display Summary Statistics
df_employees.describe().show()
df_employees.summary().show()

# =========================================================
# PART 4: SELECTING COLUMNS
# =========================================================

# Exercise 16: Select Single Column
df_employees.select("name").show()

# Exercise 17: Select Multiple Columns
df_employees.select("name", "department", "salary").show()

# Exercise 18: Select All Columns
df_employees.select("*").show()

# Exercise 19: Select with Column Expressions
df_employees.select(
    col("name"),
    (col("salary") + 1000).alias("new_salary")
).show()

# Exercise 20: Select Distinct Values
df_employees.select("department").distinct().show()

# =========================================================
# PART 5: FILTERING DATA
# =========================================================

# Exercise 21: Filter with Single Condition
df_employees.filter(col("salary") > 80000).show()

# Exercise 22: Filter with Multiple Conditions (AND)
df_employees.filter(
    (col("department") == "Engineering") & (col("salary") > 90000)
).show()

# Exercise 23: Filter with Multiple Conditions (OR)
df_employees.filter(
    (col("department") == "Engineering") | (col("department") == "Sales")
).show()

# Exercise 24: Filter with NOT Condition
df_employees.filter(~(col("department") == "Engineering")).show()
df_employees.filter(col("department") != "Engineering").show()

# Exercise 25: Filter with String Operations
df_employees.filter(col("name").startswith("A")).show()

# Exercise 26: Filter with IN Clause
df_employees.filter(col("city").isin(["Seattle", "New York", "Chicago"])).show()

# Exercise 27: Filter with BETWEEN
df_employees.filter(col("salary").between(75000, 90000)).show()

# Exercise 28: Filter Null Values
df_employees.filter(col("city").isNotNull()).show()

# =========================================================
# PART 6: ADDING AND MODIFYING COLUMNS
# =========================================================

# Exercise 29: Add New Column with Literal Value
df_with_country = df_employees.withColumn("country", lit("USA"))
display(df_with_country)

# Exercise 30: Add Calculated Column
df_with_bonus = df_employees.withColumn("annual_bonus", col("salary") * 0.10)
display(df_with_bonus)

# Exercise 31: Add Conditional Column
df_with_grade = df_employees.withColumn(
    "salary_grade",
    when(col("salary") > 85000, "High").otherwise("Standard")
)
display(df_with_grade)

# Exercise 32: Add Multiple Conditional Column
df_with_level = df_employees.withColumn(
    "experience_level",
    when(col("hire_date") < "2020-01-01", "Senior")
    .when((col("hire_date") >= "2020-01-01") & (col("hire_date") < "2022-01-01"), "Mid")
    .otherwise("Junior")
)
display(df_with_level)

# Exercise 33: Rename Column
df_renamed = df_employees.withColumnRenamed("name", "employee_name")
display(df_renamed)

# Exercise 34: Modify Existing Column
df_salary_increase = df_employees.withColumn("salary", col("salary") * 1.05)
display(df_salary_increase)

# Exercise 35: Drop Columns
df_dropped = df_employees.drop("city", "hire_date")
display(df_dropped)

# =========================================================
# PART 7: AGGREGATIONS
# =========================================================

# Exercise 36: Count Total Rows
total_count = df_employees.count()
print(f"Total employees: {total_count}")

# Exercise 37: Calculate Average
df_employees.select(avg("salary").alias("avg_salary")).show()

# Exercise 38: Find Min and Max
df_employees.select(
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary")
).show()

# Exercise 39: Multiple Aggregations
df_employees.select(
    count("*").alias("total_count"),
    avg("salary").alias("avg_salary"),
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary")
).show()

# Exercise 40: Count Distinct
df_employees.select(countDistinct("department").alias("distinct_departments")).show()

# =========================================================
# PART 8: GROUPING DATA
# =========================================================

# Exercise 41: Group By Single Column
df_employees.groupBy("department").count().show()

# Exercise 42: Group By with Average
df_employees.groupBy("department").avg("salary").show()

# Exercise 43: Group By with Multiple Aggregations
df_employees.groupBy("department").agg(
    count("*").alias("employee_count"),
    avg("salary").alias("avg_salary"),
    max("salary").alias("max_salary")
).show()

# Exercise 44: Group By Multiple Columns
df_employees.groupBy("department", "city").count().show()

# Exercise 45: Group By with Filter
df_dept_avg = df_employees.groupBy("department").agg(
    avg("salary").alias("avg_salary")
)
df_dept_avg.filter(col("avg_salary") > 80000).show()

# =========================================================
# PART 9: SORTING DATA
# =========================================================

# Exercise 46: Sort Ascending
df_employees.orderBy("salary").show()

# Exercise 47: Sort Descending
df_employees.orderBy(col("salary").desc()).show()
df_employees.orderBy(desc("salary")).show()

# Exercise 48: Sort by Multiple Columns
df_employees.orderBy(col("department").asc(), col("salary").desc()).show()

# Exercise 49: Sort with Null Handling
df_employees.orderBy(col("city").asc_nulls_first()).show()

# =========================================================
# PART 10: REMOVING DUPLICATES
# =========================================================

# Exercise 50: Remove All Duplicates
df_unique = df_employees.distinct()
print(f"Unique rows: {df_unique.count()}")

# Exercise 51: Remove Duplicates by Column
df_unique_dept = df_employees.dropDuplicates(["department"])
display(df_unique_dept)

# Exercise 52: Remove Duplicates by Multiple Columns
df_unique_dept_city = df_employees.dropDuplicates(["department", "city"])
display(df_unique_dept_city)

# =========================================================
# PART 11: HANDLING NULL VALUES
# =========================================================

# Exercise 53: Read Customers with Nulls
df_customers = spark.read.csv(
    "/FileStore/day10-exercises/customers.csv",
    header=True,
    inferSchema=True
)
display(df_customers)

# Exercise 54: Drop Rows with Any Null
df_no_nulls = df_customers.dropna()
print(f"Rows after dropping any null: {df_no_nulls.count()}")

# Exercise 55: Drop Rows with All Nulls
df_no_all_nulls = df_customers.dropna(how="all")
print(f"Rows after dropping all nulls: {df_no_all_nulls.count()}")

# Exercise 56: Drop Nulls in Specific Columns
df_no_email_phone_nulls = df_customers.dropna(subset=["email", "phone"])
display(df_no_email_phone_nulls)

# Exercise 57: Fill Nulls with Default Values
df_filled = df_customers.fillna({
    "email": "no-email@company.com",
    "phone": "000-0000"
})
display(df_filled)

# Exercise 58: Fill Nulls with Column-Specific Values
df_filled_all = df_customers.fillna({
    "email": "unknown@email.com",
    "phone": "N/A",
    "city": "Unknown",
    "state": "XX"
})
display(df_filled_all)

# =========================================================
# PART 12: WRITING DATA
# =========================================================

# Exercise 59: Write DataFrame as CSV
df_employees.write.csv(
    "/FileStore/day10-output/employees_output.csv",
    header=True,
    mode="overwrite"
)
print("CSV written successfully!")

# Exercise 60: Write DataFrame as JSON
df_employees.write.json(
    "/FileStore/day10-output/employees_output.json",
    mode="overwrite"
)
print("JSON written successfully!")

# Exercise 61: Write DataFrame as Parquet
df_employees.write.parquet(
    "/FileStore/day10-output/employees_output.parquet",
    mode="overwrite"
)
print("Parquet written successfully!")

# Exercise 62: Write DataFrame as Delta
df_employees.write.format("delta").save(
    "/FileStore/day10-output/employees_delta",
    mode="overwrite"
)
print("Delta table written successfully!")

# Exercise 63: Write with Overwrite Mode
df_employees.write.mode("overwrite").parquet(
    "/FileStore/day10-output/employees_overwrite.parquet"
)

# Exercise 64: Write with Append Mode
df_employees.limit(5).write.mode("append").parquet(
    "/FileStore/day10-output/employees_overwrite.parquet"
)

# Exercise 65: Write with Partitioning
df_employees.write.partitionBy("department").parquet(
    "/FileStore/day10-output/employees_partitioned.parquet",
    mode="overwrite"
)

# Exercise 66: Write as Table
df_employees.write.saveAsTable(
    "day10_db.employees_table",
    mode="overwrite"
)
print("Table created successfully!")

# =========================================================
# PART 13: SQL INTEGRATION
# =========================================================

# Exercise 67: Create Temporary View
df_employees.createOrReplaceTempView("employees_view")
print("Temporary view created!")

# Exercise 68: Query Temporary View
result = spark.sql("""
    SELECT * FROM employees_view
    WHERE department = 'Engineering'
""")
display(result)

# Exercise 69: Create Global Temporary View
df_employees.createOrReplaceGlobalTempView("global_employees")
result = spark.sql("SELECT * FROM global_temp.global_employees")
display(result)

# Exercise 70: Complex SQL Query
result = spark.sql("""
    SELECT 
        department,
        AVG(salary) as avg_salary,
        COUNT(*) as employee_count
    FROM employees_view
    GROUP BY department
    ORDER BY avg_salary DESC
""")
display(result)

# Exercise 71: Join Using SQL
# Create departments data
dept_data = [
    Row(dept_id=1, dept_name="Engineering", location="Seattle"),
    Row(dept_id=2, dept_name="Sales", location="New York"),
    Row(dept_id=3, dept_name="Marketing", location="Los Angeles")
]
df_departments = spark.createDataFrame(dept_data)
df_departments.createOrReplaceTempView("departments_view")

result = spark.sql("""
    SELECT 
        e.name,
        e.department,
        d.location as dept_location
    FROM employees_view e
    LEFT JOIN departments_view d ON e.department = d.dept_name
""")
display(result)

# =========================================================
# PART 14: DATAFRAME TRANSFORMATIONS CHAIN
# =========================================================

# Exercise 72: Chain Multiple Transformations
result = (df_employees
    .filter(col("salary") > 80000)
    .select("name", "salary")
    .orderBy(col("salary").desc())
)
display(result)

# Exercise 73: Complex Transformation Pipeline
result = (df_employees
    .withColumn("bonus", col("salary") * 0.10)
    .filter(col("bonus") > 8000)
    .groupBy("department")
    .agg(avg("bonus").alias("avg_bonus"))
    .orderBy(col("avg_bonus").desc())
)
display(result)

# Exercise 74: Data Cleaning Pipeline
result = (df_customers
    .dropna(subset=["customer_id", "name"])
    .fillna({"email": "unknown@email.com", "phone": "N/A", "city": "Unknown", "state": "XX"})
    .dropDuplicates(["customer_id"])
    .select("customer_id", "name", "email", "city", "state")
    .orderBy("name")
)
display(result)

# =========================================================
# PART 15: WORKING WITH DATES
# =========================================================

# Exercise 75: Parse Date Column
df_with_date = df_employees.withColumn("hire_date_parsed", to_date(col("hire_date"), "yyyy-MM-dd"))
display(df_with_date)

# Exercise 76: Extract Year from Date
df_with_year = df_with_date.withColumn("hire_year", year(col("hire_date_parsed")))
display(df_with_year)

# Exercise 77: Calculate Date Difference
df_with_days = df_with_date.withColumn(
    "days_since_hire",
    datediff(current_date(), col("hire_date_parsed"))
)
display(df_with_days)

# Exercise 78: Filter by Date Range
df_2020_hires = df_with_date.filter(
    (col("hire_date_parsed") >= "2020-01-01") & 
    (col("hire_date_parsed") < "2021-01-01")
)
display(df_2020_hires)

# =========================================================
# PART 16: STRING OPERATIONS
# =========================================================

# Exercise 79: Convert to Uppercase
df_upper = df_employees.withColumn("name_upper", upper(col("name")))
display(df_upper)

# Exercise 80: Concatenate Strings
df_concat = df_employees.withColumn(
    "full_info",
    concat(lit("Name: "), col("name"), lit(", Dept: "), col("department"))
)
display(df_concat)

# Exercise 81: Extract Substring
df_substring = df_employees.withColumn("name_code", substring(col("name"), 1, 3))
display(df_substring)

# Exercise 82: String Length
df_length = df_employees.withColumn("name_length", length(col("name")))
display(df_length)

# Exercise 83: Trim Whitespace
df_trimmed = df_employees.withColumn("name_trimmed", trim(col("name")))
display(df_trimmed)

# =========================================================
# PART 17: NUMERIC OPERATIONS
# =========================================================

# Exercise 84: Round Numbers
df_rounded = df_employees.withColumn("salary_rounded", spark_round(col("salary") / 1000) * 1000)
display(df_rounded)

# Exercise 85: Absolute Value
df_abs = df_employees.withColumn("salary_diff", spark_abs(col("salary") - 85000))
display(df_abs)

# Exercise 86: Mathematical Operations
df_projected = df_employees.withColumn(
    "projected_salary",
    (col("salary") * 1.05) + 1000
)
display(df_projected)

# =========================================================
# PART 18: WORKING WITH ARRAYS AND COLLECTIONS
# =========================================================

# Exercise 87: Create Array Column
df_with_skills = df_employees.withColumn(
    "skills",
    when(col("department") == "Engineering", array(lit("Python"), lit("Spark"), lit("SQL")))
    .when(col("department") == "Sales", array(lit("Communication"), lit("Negotiation")))
    .otherwise(array(lit("General")))
)
display(df_with_skills)

# Exercise 88: Array Contains
df_python_skills = df_with_skills.filter(array_contains(col("skills"), "Python"))
display(df_python_skills)

# Exercise 89: Array Size
df_skill_count = df_with_skills.withColumn("skill_count", size(col("skills")))
display(df_skill_count)

# =========================================================
# PART 19: PERFORMANCE AND OPTIMIZATION
# =========================================================

# Exercise 90: Cache DataFrame
df_employees.cache()
df_employees.count()  # Trigger caching
print(f"DataFrame cached: {df_employees.is_cached}")

# Exercise 91: Unpersist DataFrame
df_employees.unpersist()
print("DataFrame unpersisted")

# Exercise 92: Repartition DataFrame
df_repartitioned = df_employees.repartition(4)
print(f"Number of partitions: {df_repartitioned.rdd.getNumPartitions()}")

# Exercise 93: Coalesce Partitions
df_coalesced = df_employees.coalesce(1)
print(f"Number of partitions after coalesce: {df_coalesced.rdd.getNumPartitions()}")

# =========================================================
# PART 20: ADVANCED SCENARIOS
# =========================================================

# Exercise 94: Pivot Table
df_pivot = df_employees.groupBy("department").pivot("city").count()
display(df_pivot)

# Exercise 95: Unpivot Data (Melt)
# Create sample wide format data
wide_data = [
    Row(id=1, name="Alice", Q1=100, Q2=150, Q3=200, Q4=250),
    Row(id=2, name="Bob", Q1=120, Q2=130, Q3=180, Q4=220)
]
df_wide = spark.createDataFrame(wide_data)

# Unpivot using stack
df_long = df_wide.selectExpr(
    "id", "name",
    "stack(4, 'Q1', Q1, 'Q2', Q2, 'Q3', Q3, 'Q4', Q4) as (quarter, sales)"
)
display(df_long)

# Exercise 96: Sample Data
df_sample = df_employees.sample(fraction=0.5, seed=42)
print(f"Sample size: {df_sample.count()} out of {df_employees.count()}")

# Exercise 97: Union DataFrames
df1 = df_employees.limit(5)
df2 = df_employees.limit(3)
df_union = df1.union(df2)
print(f"Union result: {df_union.count()} rows")

# Exercise 98: Intersect DataFrames
df_intersect = df1.intersect(df2)
print(f"Intersect result: {df_intersect.count()} rows")

# Exercise 99: Except (Difference)
df_except = df1.subtract(df2)
print(f"Except result: {df_except.count()} rows")

# Exercise 100: Complete ETL Pipeline
# Read employees and sales
df_emp = spark.read.csv("/FileStore/day10-exercises/employees.csv", header=True, inferSchema=True)
df_sales = spark.read.csv("/FileStore/day10-exercises/sales.csv", header=True, inferSchema=True)

# Transform employees (add bonus)
df_emp_transformed = df_emp.withColumn("bonus", col("salary") * 0.10)

# Transform sales (add total_amount)
df_sales_transformed = df_sales.withColumn("total_amount", col("quantity") * col("amount"))

# Join (assuming we need to aggregate sales by some logic)
df_sales_agg = df_sales_transformed.groupBy("customer_id").agg(
    sum("total_amount").alias("total_sales"),
    count("*").alias("order_count")
)

# Write to Delta
df_emp_transformed.write.format("delta").mode("overwrite").save(
    "/FileStore/day10-output/etl_employees_delta"
)
df_sales_agg.write.format("delta").mode("overwrite").save(
    "/FileStore/day10-output/etl_sales_agg_delta"
)

print("ETL Pipeline completed successfully!")

# =========================================================
# REFLECTION QUESTIONS - ANSWERS
# =========================================================

"""
1. What is the difference between a transformation and an action?
   - Transformation: Lazy operation that returns a new DataFrame (select, filter, withColumn)
   - Action: Eager operation that triggers computation and returns a result (show, count, collect)
   - Transformations are not executed until an action is called

2. Why is lazy evaluation beneficial in Spark?
   - Allows Spark to optimize the execution plan
   - Combines multiple operations for efficiency
   - Avoids unnecessary computations
   - Enables better resource utilization

3. When should you use inferSchema vs explicit schema?
   - inferSchema: Quick prototyping, small files, unknown structure
   - Explicit schema: Production code, large files, better performance, data validation
   - Explicit schema is recommended for production

4. What are the advantages of Delta Lake over Parquet?
   - ACID transactions
   - Time travel (version history)
   - Schema evolution
   - Better performance (data skipping, Z-ordering)
   - Audit trail
   - Concurrent reads/writes

5. How do you handle null values in DataFrames?
   - dropna(): Remove rows with nulls
   - fillna(): Replace nulls with default values
   - isNull() / isNotNull(): Filter based on null status
   - coalesce(): Return first non-null value

6. What is the difference between filter() and where()?
   - They are aliases - functionally identical
   - filter() is more common in PySpark
   - where() is more SQL-like
   - Use whichever you prefer for consistency

7. Why use col() function instead of string column names?
   - Enables complex expressions
   - Better for chaining operations
   - Type safety
   - IDE autocomplete support
   - Required for arithmetic and logical operations

8. What happens when you call collect() on a large DataFrame?
   - Brings all data to the driver node
   - Can cause out-of-memory errors
   - Should be avoided for large datasets
   - Use show(), take(), or limit() instead

9. What is the difference between cache() and persist()?
   - cache(): Stores in memory using default storage level
   - persist(): Allows custom storage level (memory, disk, both)
   - Both improve performance for repeated operations
   - Use unpersist() to free memory

10. When should you use repartition() vs coalesce()?
    - repartition(): Increases or decreases partitions, full shuffle
    - coalesce(): Only decreases partitions, no full shuffle
    - Use repartition() for better parallelism
    - Use coalesce() before writing to reduce files
"""

# =========================================================
# BONUS CHALLENGES - SOLUTIONS
# =========================================================

# Bonus 1: Create a Data Quality Report
def data_quality_report(df):
    """Generate comprehensive data quality report"""
    print("=" * 80)
    print("DATA QUALITY REPORT")
    print("=" * 80)
    
    # Total rows
    total_rows = df.count()
    print(f"\nTotal Rows: {total_rows}")
    print(f"Total Columns: {len(df.columns)}")
    
    # Null counts per column
    print("\n" + "-" * 80)
    print("NULL VALUE ANALYSIS")
    print("-" * 80)
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c) for c in df.columns
    ]).collect()[0].asDict()
    
    for col_name, null_count in null_counts.items():
        null_pct = (null_count / total_rows) * 100
        print(f"{col_name:20s}: {null_count:5d} nulls ({null_pct:5.2f}%)")
    
    # Distinct counts
    print("\n" + "-" * 80)
    print("DISTINCT VALUE ANALYSIS")
    print("-" * 80)
    for col_name in df.columns:
        distinct_count = df.select(col_name).distinct().count()
        print(f"{col_name:20s}: {distinct_count:5d} distinct values")
    
    # Numeric column statistics
    print("\n" + "-" * 80)
    print("NUMERIC COLUMN STATISTICS")
    print("-" * 80)
    numeric_cols = [f.name for f in df.schema.fields if f.dataType.typeName() in ['integer', 'long', 'double', 'float']]
    
    if numeric_cols:
        df.select(numeric_cols).describe().show()
    else:
        print("No numeric columns found")
    
    print("=" * 80)

# Test
data_quality_report(df_employees)

# Bonus 2: Dynamic Column Selection
def select_columns_by_type(df, data_type):
    """Select columns based on data type"""
    selected_cols = [f.name for f in df.schema.fields if f.dataType.typeName() == data_type]
    if selected_cols:
        return df.select(selected_cols)
    else:
        print(f"No columns of type '{data_type}' found")
        return None

# Test
df_string_cols = select_columns_by_type(df_employees, "string")
if df_string_cols:
    display(df_string_cols)

# Bonus 3: Outlier Detection
def detect_outliers(df, column_name):
    """Detect outliers using IQR method"""
    # Calculate quartiles
    quantiles = df.approxQuantile(column_name, [0.25, 0.75], 0.01)
    Q1, Q3 = quantiles[0], quantiles[1]
    IQR = Q3 - Q1
    
    # Calculate bounds
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    print(f"Outlier Detection for '{column_name}':")
    print(f"Q1: {Q1}, Q3: {Q3}, IQR: {IQR}")
    print(f"Lower Bound: {lower_bound}, Upper Bound: {upper_bound}")
    
    # Find outliers
    outliers = df.filter(
        (col(column_name) < lower_bound) | (col(column_name) > upper_bound)
    )
    
    outlier_count = outliers.count()
    print(f"Outliers found: {outlier_count}")
    
    if outlier_count > 0:
        display(outliers)
    
    return outliers

# Test
detect_outliers(df_employees, "salary")

# Bonus 4: Data Profiling
def profile_dataframe(df):
    """Comprehensive data profiling"""
    print("=" * 80)
    print("DATAFRAME PROFILE")
    print("=" * 80)
    
    # Basic info
    print(f"\nShape: {df.count()} rows x {len(df.columns)} columns")
    
    # Schema
    print("\nSchema:")
    df.printSchema()
    
    # Sample data
    print("\nSample Data (first 5 rows):")
    df.show(5, truncate=False)
    
    # Column analysis
    print("\nColumn Analysis:")
    for col_name in df.columns:
        col_type = df.schema[col_name].dataType.typeName()
        null_count = df.filter(col(col_name).isNull()).count()
        distinct_count = df.select(col_name).distinct().count()
        
        print(f"\n{col_name}:")
        print(f"  Type: {col_type}")
        print(f"  Nulls: {null_count}")
        print(f"  Distinct: {distinct_count}")
        
        # For numeric columns, show stats
        if col_type in ['integer', 'long', 'double', 'float']:
            stats = df.select(
                min(col_name).alias("min"),
                max(col_name).alias("max"),
                avg(col_name).alias("avg")
            ).collect()[0]
            print(f"  Min: {stats['min']}")
            print(f"  Max: {stats['max']}")
            print(f"  Avg: {stats['avg']:.2f}")
    
    print("=" * 80)

# Test
profile_dataframe(df_employees)

# Bonus 5: Custom Aggregation Function
def median_by_group(df, group_col, agg_col):
    """Calculate median by group using percentile_approx"""
    from pyspark.sql.functions import expr
    
    result = df.groupBy(group_col).agg(
        expr(f"percentile_approx({agg_col}, 0.5)").alias(f"median_{agg_col}")
    )
    
    return result

# Test
median_result = median_by_group(df_employees, "department", "salary")
display(median_result)


# =========================================================
# KEY LEARNINGS SUMMARY
# =========================================================

"""
## Day 10 Key Learnings

### DataFrame Fundamentals
1. DataFrames are distributed, immutable collections with named columns
2. Operations are lazily evaluated for optimization
3. Transformations return new DataFrames; actions trigger execution

### Creating DataFrames
1. From lists, tuples, Row objects, dictionaries
2. Reading from files (CSV, JSON, Parquet, Delta)
3. With explicit or inferred schemas

### Basic Operations
1. Select, filter, withColumn, drop
2. Aggregations: count, sum, avg, min, max
3. GroupBy with multiple aggregations
4. OrderBy with asc/desc

### Data Quality
1. Handle nulls with dropna, fillna
2. Remove duplicates with distinct, dropDuplicates
3. Data validation and profiling

### SQL Integration
1. Create temporary views
2. Query with spark.sql()
3. Switch between DataFrame API and SQL

### Performance
1. Cache frequently used DataFrames
2. Repartition for parallelism
3. Coalesce before writing
4. Use explicit schemas in production

### Best Practices
1. Use Delta Lake for structured data
2. Chain transformations for readability
3. Avoid collect() on large DataFrames
4. Use column functions for complex expressions
5. Handle nulls explicitly

**Next**: Day 11 - Python DataFrames Advanced (Joins, Window Functions, UDFs)
"""
