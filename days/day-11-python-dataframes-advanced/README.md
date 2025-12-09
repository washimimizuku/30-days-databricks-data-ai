# Day 11: Python DataFrames Advanced

## Learning Objectives

By the end of today, you will:
- Master DataFrame joins (inner, outer, left, right, cross, semi, anti)
- Implement window functions in PySpark for advanced analytics
- Create and use User-Defined Functions (UDFs) and Pandas UDFs
- Perform complex data transformations with broadcast variables
- Optimize DataFrame operations for better performance
- Work with complex data types (arrays, structs, maps)

## Topics Covered

### 1. DataFrame Joins

Joins combine data from multiple DataFrames based on common columns.

#### Join Types

```python
from pyspark.sql.functions import col

# Inner Join (default) - Returns matching rows from both DataFrames
df_result = df1.join(df2, df1.id == df2.id, "inner")
df_result = df1.join(df2, "id")  # Shorthand when column names match

# Left Join (Left Outer) - All rows from left, matching from right
df_result = df1.join(df2, df1.id == df2.id, "left")
df_result = df1.join(df2, df1.id == df2.id, "left_outer")

# Right Join (Right Outer) - All rows from right, matching from left
df_result = df1.join(df2, df1.id == df2.id, "right")

# Full Outer Join - All rows from both DataFrames
df_result = df1.join(df2, df1.id == df2.id, "outer")
df_result = df1.join(df2, df1.id == df2.id, "full")

# Left Semi Join - Returns rows from left that have matches in right
df_result = df1.join(df2, df1.id == df2.id, "left_semi")

# Left Anti Join - Returns rows from left that DON'T have matches in right
df_result = df1.join(df2, df1.id == df2.id, "left_anti")

# Cross Join - Cartesian product (every row from left with every row from right)
df_result = df1.crossJoin(df2)
```

#### Join Conditions

```python
# Single column join
df_result = df1.join(df2, "id")

# Multiple column join
df_result = df1.join(df2, ["id", "date"])

# Complex join conditions
df_result = df1.join(df2, 
    (df1.id == df2.id) & (df1.date >= df2.start_date) & (df1.date <= df2.end_date)
)

# Join with different column names
df_result = df1.join(df2, df1.customer_id == df2.id)

# Handling duplicate column names
df_result = df1.alias("a").join(df2.alias("b"), col("a.id") == col("b.id"))
df_result.select("a.id", "a.name", "b.amount")
```

#### Join Best Practices

1. **Broadcast Joins**: For small DataFrames (< 10MB)
```python
from pyspark.sql.functions import broadcast

df_result = df_large.join(broadcast(df_small), "id")
```

2. **Avoid Shuffle**: Use broadcast for small tables
3. **Filter Before Join**: Reduce data size before joining
4. **Use Appropriate Join Type**: Choose the right join for your use case

### 2. Window Functions

Window functions perform calculations across a set of rows related to the current row.

#### Window Specification

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, ntile
from pyspark.sql.functions import lag, lead, first, last
from pyspark.sql.functions import sum, avg, max, min, count

# Define window specification
window_spec = Window.partitionBy("department").orderBy("salary")

# With frame specification
window_spec = Window.partitionBy("department").orderBy("salary") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
```

#### Ranking Functions

```python
# row_number: Unique sequential number
df.withColumn("row_num", row_number().over(window_spec))

# rank: Rank with gaps for ties
df.withColumn("rank", rank().over(window_spec))

# dense_rank: Rank without gaps
df.withColumn("dense_rank", dense_rank().over(window_spec))

# ntile: Divide rows into n buckets
df.withColumn("quartile", ntile(4).over(window_spec))
```

#### Analytic Functions

```python
# lag: Access previous row value
df.withColumn("prev_salary", lag("salary", 1).over(window_spec))

# lead: Access next row value
df.withColumn("next_salary", lead("salary", 1).over(window_spec))

# first: First value in window
df.withColumn("first_salary", first("salary").over(window_spec))

# last: Last value in window
df.withColumn("last_salary", last("salary").over(window_spec))
```

#### Aggregate Window Functions

```python
# Running total
df.withColumn("running_total", sum("amount").over(window_spec))

# Moving average
window_spec_3 = Window.partitionBy("department").orderBy("date") \
    .rowsBetween(-2, 0)  # Current row and 2 preceding
df.withColumn("moving_avg", avg("amount").over(window_spec_3))

# Cumulative max
df.withColumn("cum_max", max("salary").over(window_spec))
```

#### Frame Specifications

```python
# Rows-based frame
Window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
Window.rowsBetween(-2, 2)  # 2 rows before and after

# Range-based frame (value-based)
Window.rangeBetween(Window.unboundedPreceding, Window.currentRow)
Window.rangeBetween(-1000, 1000)  # Values within range
```

### 3. User-Defined Functions (UDFs)

UDFs allow you to define custom transformation logic.

#### Standard UDFs

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, DoubleType

# Define Python function
def categorize_age(age):
    if age < 18:
        return "Minor"
    elif age < 65:
        return "Adult"
    else:
        return "Senior"

# Register as UDF
categorize_age_udf = udf(categorize_age, StringType())

# Use UDF
df.withColumn("age_category", categorize_age_udf(col("age")))

# Lambda UDF
square_udf = udf(lambda x: x * x, IntegerType())
df.withColumn("age_squared", square_udf(col("age")))
```

#### Pandas UDFs (Vectorized UDFs)

Pandas UDFs are much faster than standard UDFs.

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

# Scalar Pandas UDF
@pandas_udf(DoubleType())
def calculate_bonus(salary: pd.Series) -> pd.Series:
    return salary * 0.10

df.withColumn("bonus", calculate_bonus(col("salary")))

# Grouped Map Pandas UDF
from pyspark.sql.types import StructType, StructField

schema = StructType([
    StructField("department", StringType()),
    StructField("avg_salary", DoubleType())
])

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def calculate_dept_avg(pdf):
    return pd.DataFrame({
        "department": [pdf["department"].iloc[0]],
        "avg_salary": [pdf["salary"].mean()]
    })

df.groupBy("department").apply(calculate_dept_avg)
```

#### UDF Best Practices

1. **Use Built-in Functions First**: They're optimized
2. **Prefer Pandas UDFs**: 10-100x faster than standard UDFs
3. **Avoid UDFs for Simple Logic**: Use when, otherwise instead
4. **Register UDFs for SQL**: Use spark.udf.register()
5. **Handle Nulls**: Check for None in UDF logic

### 4. Broadcast Variables

Broadcast variables efficiently share read-only data across all nodes.

```python
# Create broadcast variable
broadcast_var = spark.sparkContext.broadcast({"key1": "value1", "key2": "value2"})

# Use in UDF
def lookup_value(key):
    return broadcast_var.value.get(key, "Unknown")

lookup_udf = udf(lookup_value, StringType())
df.withColumn("mapped_value", lookup_udf(col("key_column")))

# Broadcast join (automatic for small DataFrames)
from pyspark.sql.functions import broadcast
df_result = df_large.join(broadcast(df_small), "id")
```

### 5. Complex Data Types

#### Working with Arrays

```python
from pyspark.sql.functions import array, array_contains, explode, size, sort_array
from pyspark.sql.functions import array_distinct, array_union, array_intersect

# Create array
df.withColumn("skills", array(lit("Python"), lit("Spark"), lit("SQL")))

# Array operations
df.withColumn("has_python", array_contains(col("skills"), "Python"))
df.withColumn("skill_count", size(col("skills")))
df.withColumn("sorted_skills", sort_array(col("skills")))

# Explode array (one row per element)
df.select("name", explode("skills").alias("skill"))

# Array aggregation
from pyspark.sql.functions import collect_list, collect_set
df.groupBy("department").agg(collect_list("name").alias("employees"))
```

#### Working with Structs

```python
from pyspark.sql.functions import struct, col

# Create struct
df.withColumn("address", struct(
    col("street"),
    col("city"),
    col("state"),
    col("zip")
))

# Access struct fields
df.select("name", col("address.city"), col("address.state"))
df.select("name", "address.*")  # Expand all fields
```

#### Working with Maps

```python
from pyspark.sql.functions import create_map, map_keys, map_values, explode

# Create map
df.withColumn("attributes", create_map(
    lit("color"), col("color"),
    lit("size"), col("size")
))

# Map operations
df.withColumn("keys", map_keys(col("attributes")))
df.withColumn("values", map_values(col("attributes")))

# Explode map
df.select("id", explode("attributes").alias("key", "value"))
```

### 6. Advanced Transformations

#### Pivot and Unpivot

```python
# Pivot (wide format)
df_pivot = df.groupBy("year").pivot("quarter").sum("sales")

# Unpivot (long format)
df_unpivot = df.selectExpr(
    "year",
    "stack(4, 'Q1', Q1, 'Q2', Q2, 'Q3', Q3, 'Q4', Q4) as (quarter, sales)"
)
```

#### Conditional Aggregations

```python
from pyspark.sql.functions import when, sum, count

df.groupBy("department").agg(
    count(when(col("salary") > 80000, 1)).alias("high_earners"),
    count(when(col("salary") <= 80000, 1)).alias("regular_earners"),
    sum(when(col("gender") == "F", col("salary")).otherwise(0)).alias("female_total_salary")
)
```

#### Multiple Aggregations

```python
from pyspark.sql.functions import expr

df.groupBy("department").agg(
    count("*").alias("total_employees"),
    avg("salary").alias("avg_salary"),
    max("salary").alias("max_salary"),
    min("salary").alias("min_salary"),
    expr("percentile_approx(salary, 0.5)").alias("median_salary")
)
```

### 7. Performance Optimization

#### Caching and Persistence

```python
from pyspark import StorageLevel

# Cache in memory
df.cache()
df.persist(StorageLevel.MEMORY_ONLY)

# Cache in memory and disk
df.persist(StorageLevel.MEMORY_AND_DISK)

# Unpersist
df.unpersist()
```

#### Partitioning

```python
# Repartition (full shuffle)
df_repartitioned = df.repartition(10)
df_repartitioned = df.repartition("department")  # By column

# Coalesce (reduce partitions without full shuffle)
df_coalesced = df.coalesce(1)
```

#### Bucketing

```python
# Write with bucketing
df.write.bucketBy(10, "id").sortBy("date").saveAsTable("bucketed_table")

# Read bucketed table
df_bucketed = spark.table("bucketed_table")
```

## Hands-On Exercises

See `exercise.py` for 80 practical exercises covering:
- All join types with real-world scenarios
- Window functions for ranking and analytics
- Creating and using UDFs and Pandas UDFs
- Working with complex data types
- Advanced transformations and aggregations
- Performance optimization techniques

## Key Takeaways

1. Choose the right join type for your use case (inner, left, right, outer, semi, anti)
2. Use broadcast joins for small DataFrames to avoid shuffles
3. Window functions enable powerful analytics without self-joins
4. Prefer Pandas UDFs over standard UDFs for better performance
5. Use built-in functions whenever possible instead of UDFs
6. Complex data types (arrays, structs, maps) enable flexible data modeling
7. Cache DataFrames that are used multiple times
8. Partition data appropriately for better query performance

## Exam Tips

- Know all join types and when to use each
- Understand window function syntax and frame specifications
- Remember that UDFs break Catalyst optimizer benefits
- Know the difference between rowsBetween and rangeBetween
- Understand broadcast joins and when they're applied automatically
- Be familiar with array and struct operations
- Know how to explode arrays and maps
- Understand the performance implications of different operations
- Remember that Pandas UDFs are much faster than standard UDFs
- Know how to handle duplicate column names in joins

## Common Pitfalls

1. Using cross joins unintentionally (missing join condition)
2. Not handling duplicate column names after joins
3. Using UDFs when built-in functions would work
4. Forgetting to specify window ordering for ranking functions
5. Not broadcasting small DataFrames in joins
6. Using collect() on large DataFrames
7. Not caching DataFrames used multiple times
8. Incorrect window frame specifications
9. Not handling nulls in UDFs
10. Over-partitioning or under-partitioning data

## Additional Resources

- [PySpark Join Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html)
- [Window Functions Guide](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html)
- [UDF Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html)
- [Pandas UDF Guide](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html)
- [Performance Tuning Guide](https://spark.apache.org/docs/latest/sql-performance-tuning.html)

## Next Steps

Tomorrow: [Day 12 - Data Ingestion Patterns](../day-12-data-ingestion-patterns/README.md)

Master these advanced DataFrame operations - they're critical for the exam and real-world data engineering!
