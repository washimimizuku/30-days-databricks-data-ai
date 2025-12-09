# Day 11: Python DataFrames Advanced - Solutions
# =========================================================

from pyspark.sql.functions import col, lit, when, broadcast, coalesce
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, ntile, lag, lead
from pyspark.sql.functions import sum, avg, max, min, count
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql.functions import array, array_contains, explode, size, collect_list
from pyspark.sql.functions import struct, create_map, map_keys, map_values
from pyspark.sql.functions import expr, desc
import pandas as pd

# Load datasets
df_employees = spark.read.csv("/FileStore/day11-exercises/employees.csv", header=True, inferSchema=True)
df_departments = spark.read.csv("/FileStore/day11-exercises/departments.csv", header=True, inferSchema=True)
df_sales = spark.read.csv("/FileStore/day11-exercises/sales.csv", header=True, inferSchema=True)
df_products = spark.read.csv("/FileStore/day11-exercises/products.csv", header=True, inferSchema=True)
df_orders = spark.read.csv("/FileStore/day11-exercises/orders.csv", header=True, inferSchema=True)
df_customers = spark.read.csv("/FileStore/day11-exercises/customers.csv", header=True, inferSchema=True)
df_timeseries = spark.read.csv("/FileStore/day11-exercises/timeseries.csv", header=True, inferSchema=True)

# =========================================================
# PART 1: INNER JOINS
# =========================================================

# Exercise 1: Simple Inner Join
result = df_employees.join(df_departments, df_employees.department == df_departments.dept_name, "inner")
display(result)

# Exercise 2: Inner Join with Column Selection
result = df_employees.join(df_departments, df_employees.department == df_departments.dept_name) \
    .select("emp_id", "name", "dept_name", "location")
display(result)

# Exercise 3: Inner Join with Filter
result = df_employees.join(df_departments, df_employees.department == df_departments.dept_name) \
    .filter(col("department") == "Engineering")
display(result)

# Exercise 4: Multiple Column Join
data1 = [(1, "A", "2024-01-01"), (2, "B", "2024-01-02")]
data2 = [(1, "A", "X"), (2, "B", "Y")]
df1 = spark.createDataFrame(data1, ["id", "code", "date"])
df2 = spark.createDataFrame(data2, ["id", "code", "value"])
result = df1.join(df2, ["id", "code"])
display(result)

# Exercise 5: Inner Join with Aggregation
result = df_employees.join(df_departments, df_employees.department == df_departments.dept_name) \
    .groupBy("location").count()
display(result)

# =========================================================
# PART 2: LEFT AND RIGHT JOINS
# =========================================================

# Exercise 6: Left Join
result = df_employees.join(df_departments, df_employees.department == df_departments.dept_name, "left")
display(result)

# Exercise 7: Right Join
result = df_employees.join(df_departments, df_employees.department == df_departments.dept_name, "right")
display(result)

# Exercise 8: Left Join with Null Check
result = df_employees.join(df_departments, df_employees.department == df_departments.dept_name, "left") \
    .filter(col("dept_id").isNull())
display(result)

# Exercise 9: Left Join with Coalesce
result = df_employees.join(df_departments, df_employees.department == df_departments.dept_name, "left") \
    .withColumn("location", coalesce(col("location"), lit("Unknown")))
display(result)

# Exercise 10: Right Join with Aggregation
result = df_employees.join(df_departments, df_employees.department == df_departments.dept_name, "right") \
    .groupBy("dept_name").agg(count("emp_id").alias("employee_count"))
display(result)

# =========================================================
# PART 3: OUTER AND CROSS JOINS
# =========================================================

# Exercise 11: Full Outer Join
result = df_employees.join(df_departments, df_employees.department == df_departments.dept_name, "outer")
display(result)

# Exercise 12: Cross Join
result = df_employees.limit(3).crossJoin(df_products.limit(3))
print(f"Cross join result count: {result.count()}")
display(result)

# Exercise 13: Cross Join with Filter
result = df_employees.limit(5).crossJoin(df_products.limit(5)) \
    .filter(col("salary") > 80000)
display(result)

# =========================================================
# PART 4: SEMI AND ANTI JOINS
# =========================================================

# Exercise 14: Left Semi Join
result = df_employees.join(df_sales, df_employees.emp_id == df_sales.emp_id, "left_semi")
display(result)

# Exercise 15: Left Anti Join
result = df_employees.join(df_sales, df_employees.emp_id == df_sales.emp_id, "left_anti")
display(result)

# Exercise 16: Semi Join with Condition
dept_high_budget = df_departments.filter(col("budget") > 200000)
result = df_employees.join(dept_high_budget, df_employees.department == dept_high_budget.dept_name, "left_semi")
display(result)

# Exercise 17: Anti Join for Data Quality
result = df_orders.join(df_products, df_orders.product_id == df_products.product_id, "left_anti")
display(result)
print(f"Orders with non-existent products: {result.count()}")

# =========================================================
# PART 5: COMPLEX JOINS
# =========================================================

# Exercise 18: Self Join
emp_alias = df_employees.alias("emp")
mgr_alias = df_employees.alias("mgr")
result = emp_alias.join(mgr_alias, col("emp.manager_id") == col("mgr.emp_id"), "left") \
    .select(
        col("emp.emp_id"),
        col("emp.name").alias("employee_name"),
        col("mgr.name").alias("manager_name")
    )
display(result)

# Exercise 19: Multiple Joins
result = df_employees \
    .join(df_departments, df_employees.department == df_departments.dept_name) \
    .join(df_sales, df_employees.emp_id == df_sales.emp_id) \
    .select("emp_id", "name", "dept_name", "location", "amount", "sale_date")
display(result)

# Exercise 20: Join with Different Column Names
result = df_orders.join(df_products, df_orders.product_id == df_products.product_id) \
    .select("order_id", "customer_id", "product_name", "quantity", "price")
display(result)

# Exercise 21: Join with Complex Condition
# Simulate date range join
result = df_employees.alias("e").join(
    df_sales.alias("s"),
    (col("e.emp_id") == col("s.emp_id")) & (col("s.sale_date") >= "2024-01-01"),
    "inner"
).select("e.name", "s.amount", "s.sale_date")
display(result)

# Exercise 22: Broadcast Join
result = df_employees.join(broadcast(df_departments), df_employees.department == df_departments.dept_name)
display(result)

# =========================================================
# PART 6: WINDOW FUNCTIONS - RANKING
# =========================================================

# Exercise 23: Row Number
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())
result = df_employees.withColumn("row_num", row_number().over(window_spec))
display(result)

# Exercise 24: Rank
result = df_employees.withColumn("rank", rank().over(window_spec))
display(result)

# Exercise 25: Dense Rank
result = df_employees.withColumn("dense_rank", dense_rank().over(window_spec))
display(result)

# Exercise 26: Ntile
result = df_employees.withColumn("quartile", ntile(4).over(window_spec))
display(result)

# Exercise 27: Top N per Group
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())
result = df_employees.withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") <= 2)
display(result)

# =========================================================
# PART 7: WINDOW FUNCTIONS - ANALYTIC
# =========================================================

# Exercise 28: Lag Function
window_spec = Window.partitionBy("department").orderBy("salary")
result = df_employees.withColumn("prev_salary", lag("salary", 1).over(window_spec))
display(result)

# Exercise 29: Lead Function
result = df_employees.withColumn("next_salary", lead("salary", 1).over(window_spec))
display(result)

# Exercise 30: Salary Difference
result = df_employees.withColumn("prev_salary", lag("salary", 1).over(window_spec)) \
    .withColumn("salary_diff", col("salary") - col("prev_salary"))
display(result)

# Exercise 31: First and Last
from pyspark.sql.functions import first, last
result = df_employees.withColumn("first_salary", first("salary").over(window_spec)) \
    .withColumn("last_salary", last("salary").over(window_spec))
display(result)

# Exercise 32: Lag with Default
result = df_employees.withColumn("prev_salary", lag("salary", 1, 0).over(window_spec))
display(result)

# =========================================================
# PART 8: WINDOW FUNCTIONS - AGGREGATES
# =========================================================

# Exercise 33: Running Total
window_spec = Window.partitionBy("emp_id").orderBy("sale_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
result = df_sales.withColumn("running_total", sum("amount").over(window_spec))
display(result)

# Exercise 34: Cumulative Average
window_spec = Window.partitionBy("department").orderBy("salary") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
result = df_employees.withColumn("cum_avg", avg("salary").over(window_spec))
display(result)

# Exercise 35: Moving Average
window_spec = Window.partitionBy("product").orderBy("date").rowsBetween(-2, 0)
result = df_timeseries.withColumn("moving_avg_3day", avg("sales").over(window_spec))
display(result)

# Exercise 36: Running Max
window_spec = Window.partitionBy("department").orderBy("salary") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
result = df_employees.withColumn("running_max", max("salary").over(window_spec))
display(result)

# Exercise 37: Percentage of Total
window_spec = Window.partitionBy("department")
result = df_employees.withColumn("dept_total", sum("salary").over(window_spec)) \
    .withColumn("pct_of_total", (col("salary") / col("dept_total")) * 100)
display(result)

# =========================================================
# PART 9: WINDOW FRAMES
# =========================================================

# Exercise 38: Rows Between
window_spec = Window.partitionBy("product").orderBy("date").rowsBetween(-2, 0)
result = df_timeseries.withColumn("sum_3rows", sum("sales").over(window_spec))
display(result)

# Exercise 39: Range Between
window_spec = Window.partitionBy("department").orderBy("salary").rangeBetween(-10000, 10000)
result = df_employees.withColumn("similar_salary_count", count("*").over(window_spec))
display(result)

# Exercise 40: Unbounded Preceding
window_spec = Window.partitionBy("department").orderBy("salary") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
result = df_employees.withColumn("running_total", sum("salary").over(window_spec))
display(result)

# Exercise 41: Unbounded Following
window_spec = Window.partitionBy("department").orderBy("salary") \
    .rowsBetween(Window.currentRow, Window.unboundedFollowing)
result = df_employees.withColumn("remaining_total", sum("salary").over(window_spec))
display(result)

# =========================================================
# PART 10: USER-DEFINED FUNCTIONS (UDFs)
# =========================================================

# Exercise 42: Simple UDF
def categorize_salary(salary):
    if salary < 80000:
        return "Low"
    elif salary <= 100000:
        return "Medium"
    else:
        return "High"

categorize_udf = udf(categorize_salary, StringType())
result = df_employees.withColumn("salary_category", categorize_udf(col("salary")))
display(result)

# Exercise 43: Lambda UDF
bonus_udf = udf(lambda x: x * 0.10, DoubleType())
result = df_employees.withColumn("bonus", bonus_udf(col("salary")))
display(result)

# Exercise 44: UDF with Multiple Inputs
def format_employee(name, department):
    return f"{name} works in {department}"

format_udf = udf(format_employee, StringType())
result = df_employees.withColumn("formatted", format_udf(col("name"), col("department")))
display(result)

# Exercise 45: UDF with Conditional Logic
def experience_level(hire_date):
    if hire_date is None:
        return "Unknown"
    year = int(hire_date.split("-")[0])
    if year < 2020:
        return "Senior"
    elif year < 2022:
        return "Mid"
    else:
        return "Junior"

experience_udf = udf(experience_level, StringType())
result = df_employees.withColumn("experience", experience_udf(col("hire_date")))
display(result)

# Exercise 46: Register UDF for SQL
spark.udf.register("categorize_salary_sql", categorize_salary, StringType())
df_employees.createOrReplaceTempView("employees_view")
result = spark.sql("""
    SELECT name, salary, categorize_salary_sql(salary) as category
    FROM employees_view
""")
display(result)

# =========================================================
# PART 11: PANDAS UDFs
# =========================================================

# Exercise 47: Scalar Pandas UDF
@pandas_udf(DoubleType())
def calculate_tax(salary: pd.Series) -> pd.Series:
    return salary * 0.20

result = df_employees.withColumn("tax", calculate_tax(col("salary")))
display(result)

# Exercise 48: Pandas UDF with Series
@pandas_udf(DoubleType())
def net_salary(salary: pd.Series) -> pd.Series:
    tax = salary * 0.20
    return salary - tax

result = df_employees.withColumn("net_salary", net_salary(col("salary")))
display(result)

# Exercise 49: Grouped Map Pandas UDF (Note: Syntax varies by Spark version)
# For Spark 3.0+, use applyInPandas
def normalize_salaries(pdf):
    mean_salary = pdf['salary'].mean()
    std_salary = pdf['salary'].std()
    pdf['normalized_salary'] = (pdf['salary'] - mean_salary) / std_salary if std_salary > 0 else 0
    return pdf

schema = df_employees.schema.add("normalized_salary", DoubleType())
result = df_employees.groupBy("department").applyInPandas(normalize_salaries, schema)
display(result)

# =========================================================
# PART 12: BROADCAST VARIABLES
# =========================================================

# Exercise 50: Create Broadcast Variable
dept_codes = {"Engineering": "ENG", "Sales": "SLS", "Marketing": "MKT", "HR": "HR", "Finance": "FIN"}
broadcast_codes = spark.sparkContext.broadcast(dept_codes)
print(f"Broadcast variable created: {broadcast_codes.value}")

# Exercise 51: Use Broadcast in UDF
def map_dept_code(department):
    return broadcast_codes.value.get(department, "UNK")

map_code_udf = udf(map_dept_code, StringType())
result = df_employees.withColumn("dept_code", map_code_udf(col("department")))
display(result)

# Exercise 52: Broadcast Join
result = df_employees.join(broadcast(df_departments), df_employees.department == df_departments.dept_name)
display(result)
print("Broadcast join completed")

# =========================================================
# PART 13: ARRAY OPERATIONS
# =========================================================

# Exercise 53: Create Array Column
result = df_employees.withColumn(
    "skills",
    when(col("department") == "Engineering", array(lit("Python"), lit("Spark"), lit("SQL")))
    .when(col("department") == "Sales", array(lit("Communication"), lit("Negotiation")))
    .otherwise(array(lit("General")))
)
display(result)

# Exercise 54: Array Contains
result = df_employees.withColumn(
    "skills",
    when(col("department") == "Engineering", array(lit("Python"), lit("Spark"), lit("SQL")))
    .otherwise(array(lit("General")))
).filter(array_contains(col("skills"), "Python"))
display(result)

# Exercise 55: Array Size
result = df_employees.withColumn(
    "skills",
    when(col("department") == "Engineering", array(lit("Python"), lit("Spark"), lit("SQL")))
    .otherwise(array(lit("General")))
).withColumn("skill_count", size(col("skills")))
display(result)

# Exercise 56: Explode Array
result = df_employees.withColumn(
    "skills",
    when(col("department") == "Engineering", array(lit("Python"), lit("Spark"), lit("SQL")))
    .otherwise(array(lit("General")))
).select("name", "department", explode("skills").alias("skill"))
display(result)

# Exercise 57: Collect List
result = df_employees.groupBy("department").agg(collect_list("name").alias("employees"))
display(result)

# Exercise 58: Array Distinct
from pyspark.sql.functions import array_distinct
data = [(1, ["A", "B", "A", "C"]), (2, ["X", "Y", "X"])]
df = spark.createDataFrame(data, ["id", "items"])
result = df.withColumn("unique_items", array_distinct(col("items")))
display(result)

# =========================================================
# PART 14: STRUCT OPERATIONS
# =========================================================

# Exercise 59: Create Struct
result = df_employees.withColumn(
    "location_info",
    struct(col("city").alias("city"), lit("USA").alias("country"))
)
display(result)

# Exercise 60: Access Struct Fields
result = df_employees.withColumn(
    "location_info",
    struct(col("city").alias("city"), lit("USA").alias("country"))
).select("name", col("location_info.city"), col("location_info.country"))
display(result)

# Exercise 61: Expand Struct
result = df_employees.withColumn(
    "location_info",
    struct(col("city").alias("city"), lit("USA").alias("country"))
).select("name", "location_info.*")
display(result)

# =========================================================
# PART 15: MAP OPERATIONS
# =========================================================

# Exercise 62: Create Map
result = df_employees.withColumn(
    "attributes",
    create_map(
        lit("department"), col("department"),
        lit("city"), col("city")
    )
)
display(result)

# Exercise 63: Map Keys and Values
result = df_employees.withColumn(
    "attributes",
    create_map(lit("department"), col("department"), lit("city"), col("city"))
).withColumn("keys", map_keys(col("attributes"))) \
 .withColumn("values", map_values(col("attributes")))
display(result)

# Exercise 64: Explode Map
result = df_employees.withColumn(
    "attributes",
    create_map(lit("department"), col("department"), lit("city"), col("city"))
).select("name", explode("attributes").alias("key", "value"))
display(result)

# =========================================================
# PART 16: PIVOT AND UNPIVOT
# =========================================================

# Exercise 65: Pivot Table
result = df_sales.groupBy("emp_id").pivot("product").count()
display(result)

# Exercise 66: Pivot with Aggregation
result = df_sales.groupBy("emp_id").pivot("product").sum("amount")
display(result)

# Exercise 67: Unpivot Data
# Create wide format data
wide_data = [(1, "Alice", 100, 150, 200, 250), (2, "Bob", 120, 130, 180, 220)]
df_wide = spark.createDataFrame(wide_data, ["id", "name", "Q1", "Q2", "Q3", "Q4"])

# Unpivot using stack
result = df_wide.selectExpr(
    "id", "name",
    "stack(4, 'Q1', Q1, 'Q2', Q2, 'Q3', Q3, 'Q4', Q4) as (quarter, sales)"
)
display(result)

# =========================================================
# PART 17: CONDITIONAL AGGREGATIONS
# =========================================================

# Exercise 68: Count with Condition
result = df_employees.groupBy("department").agg(
    count(when(col("salary") > 90000, 1)).alias("high_earners"),
    count(when(col("salary") <= 90000, 1)).alias("regular_earners")
)
display(result)

# Exercise 69: Sum with Condition
result = df_employees.groupBy("department").agg(
    sum(when(col("hire_date") > "2020-01-01", col("salary")).otherwise(0)).alias("new_hire_total_salary")
)
display(result)

# Exercise 70: Multiple Conditional Aggregations
result = df_employees.groupBy("department").agg(
    count("*").alias("total_employees"),
    count(when(col("salary") > 90000, 1)).alias("high_earners"),
    avg(when(col("salary") > 90000, col("salary"))).alias("avg_high_salary"),
    sum(when(col("city") == "Seattle", 1).otherwise(0)).alias("seattle_count")
)
display(result)

# =========================================================
# PART 18: PERFORMANCE OPTIMIZATION
# =========================================================

# Exercise 71: Cache DataFrame
df_employees.cache()
df_employees.count()  # Trigger caching
print(f"DataFrame cached: {df_employees.is_cached}")

# Exercise 72: Repartition
result = df_employees.repartition("department")
print(f"Partitions after repartition: {result.rdd.getNumPartitions()}")

# Exercise 73: Coalesce
result = df_employees.coalesce(1)
print(f"Partitions after coalesce: {result.rdd.getNumPartitions()}")

# Exercise 74: Persist with Storage Level
from pyspark import StorageLevel
df_employees.persist(StorageLevel.MEMORY_AND_DISK)
df_employees.count()
print("DataFrame persisted with MEMORY_AND_DISK")

# Unpersist
df_employees.unpersist()
print("DataFrame unpersisted")

# =========================================================
# PART 19: COMPLEX TRANSFORMATIONS
# =========================================================

# Exercise 75: Chained Transformations
result = df_employees \
    .join(df_departments, df_employees.department == df_departments.dept_name) \
    .filter(col("budget") > 200000) \
    .groupBy("dept_name").agg(avg("salary").alias("avg_salary")) \
    .orderBy(desc("avg_salary"))
display(result)

# Exercise 76: Nested Aggregations
dept_avg = df_employees.groupBy("department").agg(avg("salary").alias("dept_avg"))
result = dept_avg.agg(avg("dept_avg").alias("overall_avg"))
display(result)

# Exercise 77: Complex Window Calculation
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())
result = df_employees \
    .withColumn("rank", rank().over(window_spec)) \
    .withColumn("dense_rank", dense_rank().over(window_spec)) \
    .withColumn("pct_rank", expr("percent_rank()").over(window_spec))
display(result)

# Exercise 78: Multi-level Grouping
result = df_employees.groupBy("department", "city").agg(
    count("*").alias("employee_count"),
    avg("salary").alias("avg_salary")
).orderBy("department", "city")
display(result)

# =========================================================
# PART 20: REAL-WORLD SCENARIOS
# =========================================================

# Exercise 79: Customer Lifetime Value
result = df_orders \
    .join(df_products, "product_id") \
    .withColumn("order_value", col("quantity") * col("price")) \
    .groupBy("customer_id") \
    .agg(
        sum("order_value").alias("lifetime_value"),
        count("order_id").alias("total_orders"),
        avg("order_value").alias("avg_order_value")
    ) \
    .orderBy(desc("lifetime_value"))
display(result)

# Exercise 80: Employee Hierarchy
emp = df_employees.alias("emp")
mgr = df_employees.alias("mgr")
result = emp.join(mgr, col("emp.manager_id") == col("mgr.emp_id"), "left") \
    .select(
        col("emp.emp_id"),
        col("emp.name").alias("employee"),
        col("emp.department"),
        col("mgr.name").alias("manager"),
        col("emp.salary").alias("emp_salary"),
        col("mgr.salary").alias("mgr_salary")
    ) \
    .orderBy("department", "manager")
display(result)

print("All exercises completed!")

# =========================================================
# REFLECTION QUESTIONS - ANSWERS
# =========================================================

"""
1. When should you use left semi join vs inner join?
   - Left semi join: When you only need columns from the left DataFrame and just want to filter
     based on existence in right DataFrame (returns left rows that have matches)
   - Inner join: When you need columns from both DataFrames
   - Semi join is more efficient when you don't need right DataFrame columns

2. What is the difference between rank() and dense_rank()?
   - rank(): Assigns ranks with gaps for ties (1, 2, 2, 4, 5)
   - dense_rank(): Assigns ranks without gaps (1, 2, 2, 3, 4)
   - Use rank() for traditional ranking, dense_rank() when you want consecutive numbers

3. Why are Pandas UDFs faster than standard UDFs?
   - Pandas UDFs use Apache Arrow for efficient data transfer
   - Process data in batches (vectorized) instead of row-by-row
   - Reduce serialization/deserialization overhead
   - Can be 10-100x faster than standard UDFs

4. When should you use broadcast joins?
   - When one DataFrame is small (< 10MB by default)
   - To avoid expensive shuffle operations
   - Spark automatically broadcasts small DataFrames
   - Use broadcast() hint to force broadcasting

5. What is the difference between rowsBetween and rangeBetween?
   - rowsBetween: Physical rows (e.g., 3 rows before and after)
   - rangeBetween: Logical range based on values (e.g., values within ±1000)
   - rowsBetween is more common and predictable
   - rangeBetween useful for time-based or value-based windows

6. How do you handle duplicate column names after joins?
   - Use aliases: df1.alias("a").join(df2.alias("b"), ...)
   - Select with alias: col("a.id"), col("b.id")
   - Drop duplicate columns: df.drop(df2.id)
   - Rename before join: df1.withColumnRenamed("id", "id1")

7. What are the performance implications of cross joins?
   - Creates Cartesian product (m × n rows)
   - Very expensive for large DataFrames
   - Can cause out-of-memory errors
   - Usually indicates missing join condition
   - Use only when truly needed (e.g., generating combinations)

8. When should you cache a DataFrame?
   - When DataFrame is used multiple times in your code
   - After expensive transformations
   - Before iterative algorithms
   - When reading from slow sources
   - Don't cache if used only once

9. What is the difference between repartition and coalesce?
   - repartition(): Full shuffle, can increase or decrease partitions
   - coalesce(): No full shuffle, only decreases partitions
   - Use repartition() for better parallelism
   - Use coalesce() before writing to reduce output files

10. How do window functions differ from group by aggregations?
    - Window functions: Keep all rows, add calculated columns
    - Group by: Reduce rows, one row per group
    - Window functions: Can access other rows in partition
    - Group by: Only aggregates within group
    - Window functions: More flexible for analytics
"""

# =========================================================
# BONUS CHALLENGES - SOLUTIONS
# =========================================================

# Bonus 1: Advanced Join Pattern
def complex_join_analysis():
    """Complex multi-table join with filters and aggregations"""
    result = df_employees \
        .join(df_departments, df_employees.department == df_departments.dept_name) \
        .join(df_sales, df_employees.emp_id == df_sales.emp_id, "left") \
        .filter(col("budget") > 200000) \
        .groupBy("dept_name", "location").agg(
            count("sale_id").alias("total_sales"),
            sum("amount").alias("total_revenue"),
            countDistinct("emp_id").alias("active_sellers"),
            avg("salary").alias("avg_salary")
        ) \
        .orderBy(desc("total_revenue"))
    
    display(result)
    return result

complex_join_analysis()

# Bonus 2: Custom Window Function
def custom_ranking():
    """Custom ranking system combining multiple criteria"""
    # Rank by salary within department, then by hire date
    window_spec1 = Window.partitionBy("department").orderBy(col("salary").desc())
    window_spec2 = Window.partitionBy("department").orderBy("hire_date")
    
    result = df_employees \
        .withColumn("salary_rank", rank().over(window_spec1)) \
        .withColumn("seniority_rank", rank().over(window_spec2)) \
        .withColumn("combined_score", col("salary_rank") + col("seniority_rank")) \
        .orderBy("department", "combined_score")
    
    display(result)
    return result

custom_ranking()

# Bonus 3: Performance Comparison
def compare_udf_performance():
    """Compare standard UDF vs Pandas UDF performance"""
    import time
    
    # Standard UDF
    def calculate_bonus_std(salary):
        return salary * 0.15
    
    bonus_std_udf = udf(calculate_bonus_std, DoubleType())
    
    # Pandas UDF
    @pandas_udf(DoubleType())
    def calculate_bonus_pandas(salary: pd.Series) -> pd.Series:
        return salary * 0.15
    
    # Test standard UDF
    start = time.time()
    result1 = df_employees.withColumn("bonus", bonus_std_udf(col("salary")))
    result1.count()
    std_time = time.time() - start
    
    # Test Pandas UDF
    start = time.time()
    result2 = df_employees.withColumn("bonus", calculate_bonus_pandas(col("salary")))
    result2.count()
    pandas_time = time.time() - start
    
    print(f"Standard UDF time: {std_time:.4f}s")
    print(f"Pandas UDF time: {pandas_time:.4f}s")
    print(f"Speedup: {std_time/pandas_time:.2f}x")

compare_udf_performance()

# Bonus 4: Data Quality Check
def data_quality_check():
    """Use anti joins to find data quality issues"""
    print("Data Quality Report")
    print("=" * 60)
    
    # Find orders without valid products
    invalid_products = df_orders.join(df_products, "product_id", "left_anti")
    print(f"Orders with invalid products: {invalid_products.count()}")
    
    # Find employees without departments
    invalid_depts = df_employees.join(df_departments, 
        df_employees.department == df_departments.dept_name, "left_anti")
    print(f"Employees with invalid departments: {invalid_depts.count()}")
    
    # Find sales by non-existent employees
    invalid_sales = df_sales.join(df_employees, "emp_id", "left_anti")
    print(f"Sales by non-existent employees: {invalid_sales.count()}")
    
    print("=" * 60)

data_quality_check()

# Bonus 5: Time Series Analysis
def time_series_analysis():
    """Perform time series analysis using window functions"""
    window_spec = Window.partitionBy("product").orderBy("date")
    
    result = df_timeseries \
        .withColumn("prev_sales", lag("sales", 1).over(window_spec)) \
        .withColumn("sales_change", col("sales") - col("prev_sales")) \
        .withColumn("pct_change", 
            (col("sales") - col("prev_sales")) / col("prev_sales") * 100) \
        .withColumn("moving_avg_3", 
            avg("sales").over(window_spec.rowsBetween(-2, 0))) \
        .withColumn("cumulative_sales", 
            sum("sales").over(window_spec.rowsBetween(Window.unboundedPreceding, 0)))
    
    display(result)
    return result

time_series_analysis()


# =========================================================
# KEY LEARNINGS SUMMARY
# =========================================================

"""
## Day 11 Key Learnings

### Join Operations
1. Master all join types: inner, left, right, outer, semi, anti, cross
2. Use broadcast joins for small DataFrames
3. Handle duplicate column names with aliases
4. Choose appropriate join type for use case

### Window Functions
1. Ranking: row_number, rank, dense_rank, ntile
2. Analytic: lag, lead, first, last
3. Aggregates: sum, avg, max, min with window frames
4. Frame specifications: rowsBetween vs rangeBetween

### UDFs and Performance
1. Prefer built-in functions over UDFs
2. Use Pandas UDFs for 10-100x speedup
3. Handle nulls in UDF logic
4. Register UDFs for SQL usage

### Complex Data Types
1. Arrays: create, explode, collect_list, array_contains
2. Structs: create, access fields, expand
3. Maps: create, explode, map_keys, map_values

### Optimization
1. Cache DataFrames used multiple times
2. Broadcast small DataFrames in joins
3. Repartition for parallelism, coalesce for output
4. Use appropriate storage levels

### Best Practices
1. Use aliases to avoid column name conflicts
2. Filter before joins to reduce data size
3. Choose window functions over self-joins
4. Test UDF performance before production use
5. Monitor partition counts and sizes

**Next**: Day 12 - Data Ingestion Patterns (COPY INTO, Auto Loader)
"""
