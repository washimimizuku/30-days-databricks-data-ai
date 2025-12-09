# Day 10: Python DataFrames Basics

## Learning Objectives

By the end of today, you will:
- Create and manipulate Spark DataFrames using PySpark
- Read and write data in various formats (CSV, JSON, Parquet, Delta)
- Perform basic DataFrame transformations (select, filter, withColumn)
- Understand DataFrame schemas and data types
- Apply common DataFrame operations (groupBy, orderBy, distinct)
- Convert between DataFrames, SQL, and temporary views

## Topics Covered

### 1. Introduction to PySpark DataFrames

DataFrames are the primary abstraction in Spark for working with structured data. They are:
- Distributed collections of data organized into named columns
- Similar to tables in a relational database or pandas DataFrames
- Immutable (transformations create new DataFrames)
- Lazily evaluated (transformations are not executed until an action is called)

Key differences from pandas:
- Spark DataFrames are distributed across a cluster
- Operations are lazily evaluated for optimization
- Better for large-scale data processing

### 2. Creating DataFrames

Multiple ways to create DataFrames:

```python
# From a list of tuples
data = [(1, "Alice", 30), (2, "Bob", 25)]
df = spark.createDataFrame(data, ["id", "name", "age"])

# From a list of Row objects
from pyspark.sql import Row
data = [Row(id=1, name="Alice", age=30), Row(id=2, name="Bob", age=25)]
df = spark.createDataFrame(data)

# From a pandas DataFrame
import pandas as pd
pdf = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
df = spark.createDataFrame(pdf)

# Reading from files
df = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)
df = spark.read.json("path/to/file.json")
df = spark.read.parquet("path/to/file.parquet")
df = spark.read.format("delta").load("path/to/delta/table")
```

### 3. DataFrame Schema

Schema defines the structure of your DataFrame:

```python
# View schema
df.printSchema()
df.schema
df.dtypes

# Define explicit schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True)
])

df = spark.read.csv("file.csv", schema=schema)
```

Common data types:
- StringType, IntegerType, LongType, DoubleType, FloatType
- BooleanType, DateType, TimestampType
- ArrayType, MapType, StructType

### 4. Reading Data

Read data from various sources:

```python
# CSV with options
df = spark.read.csv(
    "path/to/file.csv",
    header=True,
    inferSchema=True,
    sep=",",
    quote='"',
    escape="\\",
    nullValue="NULL"
)

# JSON (single-line or multi-line)
df = spark.read.json("path/to/file.json")
df = spark.read.option("multiline", "true").json("path/to/file.json")

# Parquet (columnar format)
df = spark.read.parquet("path/to/file.parquet")

# Delta Lake (recommended)
df = spark.read.format("delta").load("path/to/delta/table")

# Multiple files with wildcard
df = spark.read.csv("path/to/*.csv", header=True)

# Read from table
df = spark.table("database.table_name")
```

### 5. Writing Data

Write DataFrames to various formats:

```python
# CSV
df.write.csv("path/to/output", header=True, mode="overwrite")

# JSON
df.write.json("path/to/output", mode="overwrite")

# Parquet
df.write.parquet("path/to/output", mode="overwrite")

# Delta Lake (recommended)
df.write.format("delta").save("path/to/output", mode="overwrite")

# Write modes
# - overwrite: Replace existing data
# - append: Add to existing data
# - ignore: Do nothing if data exists
# - error/errorifexists: Throw error if data exists (default)

# Partitioning
df.write.partitionBy("year", "month").parquet("path/to/output")

# Save as table
df.write.saveAsTable("database.table_name", mode="overwrite")
```

### 6. Basic DataFrame Operations

Common operations for viewing and inspecting data:

```python
# Display data
df.show()           # Show first 20 rows
df.show(5)          # Show first 5 rows
df.show(truncate=False)  # Don't truncate long strings
display(df)         # Databricks-specific, better formatting

# Get basic info
df.count()          # Number of rows
df.columns          # List of column names
df.printSchema()    # Schema structure
df.dtypes           # Column names and types

# Get first rows
df.head()           # First row as Row object
df.head(5)          # First 5 rows as list
df.first()          # First row
df.take(5)          # First 5 rows as list

# Summary statistics
df.describe().show()
df.summary().show()
```

### 7. Selecting and Filtering

Select columns and filter rows:

```python
from pyspark.sql.functions import col

# Select columns
df.select("name", "age")
df.select(col("name"), col("age"))
df.select(df.name, df.age)
df.select("*")  # All columns

# Select with expressions
df.select(col("name"), (col("age") + 1).alias("next_age"))

# Filter rows
df.filter(col("age") > 25)
df.filter("age > 25")  # SQL expression
df.where(col("age") > 25)  # where is alias for filter

# Multiple conditions
df.filter((col("age") > 25) & (col("name") == "Alice"))
df.filter((col("age") > 25) | (col("name") == "Alice"))
df.filter(~(col("age") > 25))  # NOT

# Filter with SQL
df.filter("age > 25 AND name = 'Alice'")
```

### 8. Adding and Modifying Columns

Transform data by adding or modifying columns:

```python
from pyspark.sql.functions import col, lit, when

# Add new column
df.withColumn("age_plus_10", col("age") + 10)
df.withColumn("country", lit("USA"))  # Literal value

# Modify existing column
df.withColumn("age", col("age") + 1)

# Conditional column
df.withColumn(
    "age_category",
    when(col("age") < 18, "Minor")
    .when(col("age") < 65, "Adult")
    .otherwise("Senior")
)

# Rename column
df.withColumnRenamed("name", "full_name")

# Drop columns
df.drop("age")
df.drop("age", "name")
```

### 9. Aggregations and Grouping

Perform aggregations on data:

```python
from pyspark.sql.functions import count, sum, avg, max, min, countDistinct

# Simple aggregations
df.select(count("*")).show()
df.select(avg("age"), max("age"), min("age")).show()

# Group by
df.groupBy("department").count().show()
df.groupBy("department").avg("salary").show()
df.groupBy("department").agg(
    count("*").alias("employee_count"),
    avg("salary").alias("avg_salary"),
    max("salary").alias("max_salary")
).show()

# Multiple grouping columns
df.groupBy("department", "location").count().show()
```

### 10. Sorting and Ordering

Sort DataFrame rows:

```python
from pyspark.sql.functions import col, desc, asc

# Sort ascending (default)
df.orderBy("age")
df.orderBy(col("age"))
df.sort("age")  # Alias for orderBy

# Sort descending
df.orderBy(col("age").desc())
df.orderBy(desc("age"))

# Multiple columns
df.orderBy(col("department").asc(), col("salary").desc())

# Sort with null handling
df.orderBy(col("age").asc_nulls_first())
df.orderBy(col("age").asc_nulls_last())
```

### 11. Removing Duplicates

Remove duplicate rows:

```python
# Remove all duplicate rows
df.distinct()

# Remove duplicates based on specific columns
df.dropDuplicates(["name", "email"])
df.drop_duplicates(["name"])  # Alias
```

### 12. Handling Null Values

Work with null/missing values:

```python
# Drop rows with nulls
df.dropna()  # Drop rows with any null
df.dropna(how="all")  # Drop rows where all values are null
df.dropna(subset=["age", "name"])  # Drop if null in specific columns
df.dropna(thresh=2)  # Drop if less than 2 non-null values

# Fill null values
df.fillna(0)  # Fill all nulls with 0
df.fillna({"age": 0, "name": "Unknown"})  # Fill specific columns
df.na.fill({"age": 0})  # Alternative syntax

# Replace values
df.replace(["old_value"], ["new_value"], subset=["column_name"])
```

### 13. DataFrame to SQL and Back

Convert between DataFrames and SQL:

```python
# Create temporary view
df.createOrReplaceTempView("people")

# Query with SQL
result = spark.sql("SELECT * FROM people WHERE age > 25")

# Global temporary view (accessible across sessions)
df.createOrReplaceGlobalTempView("global_people")
spark.sql("SELECT * FROM global_temp.global_people")
```

### 14. Actions vs Transformations

Understanding lazy evaluation:

Transformations (lazy - return new DataFrame):
- select, filter, where, withColumn, groupBy, orderBy, join, distinct

Actions (eager - trigger computation):
- show, count, collect, take, first, write, describe

```python
# Transformations are lazy
df2 = df.filter(col("age") > 25)  # Not executed yet
df3 = df2.select("name")          # Still not executed

# Action triggers execution
df3.show()  # Now all transformations are executed
```

## Hands-On Exercises

See `exercise.py` for 40 practical exercises covering:
- Creating DataFrames from various sources
- Reading and writing different file formats
- Schema definition and manipulation
- Selecting, filtering, and transforming data
- Aggregations and grouping
- Sorting and deduplication
- Null value handling
- SQL integration

## Key Takeaways

1. DataFrames are distributed, immutable collections with named columns
2. Operations are lazily evaluated until an action is called
3. Use explicit schemas for better performance and data quality
4. Delta Lake is the recommended format for reading/writing data
5. PySpark provides both DataFrame API and SQL for data manipulation
6. Always use column functions (col()) for complex expressions
7. Transformations return new DataFrames; actions trigger execution
8. Use display() in Databricks for better data visualization

## Exam Tips

- Know the difference between transformations and actions
- Understand when to use inferSchema vs explicit schema
- Remember write modes: overwrite, append, ignore, error
- Be familiar with common functions: col, lit, when, otherwise
- Know how to handle null values with dropna and fillna
- Understand the syntax for filter conditions (& for AND, | for OR)
- Remember to use parentheses around filter conditions
- Know how to create temporary views for SQL queries
- Understand partitioning for write operations
- Be comfortable switching between DataFrame API and SQL

## Common Pitfalls

1. Forgetting parentheses in filter conditions: `(col("a") > 5) & (col("b") < 10)`
2. Using `and`/`or` instead of `&`/`|` in filter conditions
3. Not using `col()` for column references in complex expressions
4. Calling `collect()` on large DataFrames (brings all data to driver)
5. Forgetting to specify mode when writing (default is error)
6. Using inferSchema=True on large files (slow)
7. Not handling null values properly

## Additional Resources

- [PySpark DataFrame API Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html)
- [Databricks PySpark Guide](https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-python.html)
- [PySpark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)

## Next Steps

Tomorrow: [Day 11 - Python DataFrames Advanced](../day-11-python-dataframes-advanced/README.md)

Practice the exercises thoroughly - DataFrame operations are fundamental to the exam!
