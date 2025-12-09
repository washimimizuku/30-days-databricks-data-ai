# Day 15: Week 2 Review & ELT Project

## Overview

Congratulations on completing Week 2! Today is dedicated to reviewing and consolidating everything you've learned about ELT with Spark SQL and Python. This review day includes:

- Comprehensive review of Days 8-14
- 50-question quiz covering all Week 2 topics
- Hands-on ELT project integrating multiple concepts
- Exam preparation tips

## Learning Objectives

By the end of today, you will:
- Consolidate knowledge from Days 8-14
- Identify areas needing additional review
- Build a complete end-to-end ELT pipeline
- Practice exam-style questions
- Build confidence for Week 3

## Week 2 Topics Recap

### Day 8: Advanced SQL - Window Functions
**Key Concepts**:
- Window functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE)
- Aggregate window functions (SUM, AVG, COUNT, MIN, MAX)
- PARTITION BY and ORDER BY clauses
- Frame specifications (ROWS BETWEEN, RANGE BETWEEN)
- LAG and LEAD functions
- Running totals and moving averages

**Critical for Exam**:
- Know the difference between ROW_NUMBER, RANK, and DENSE_RANK
- Understand PARTITION BY vs GROUP BY
- Know frame specification syntax
- Recognize when to use LAG/LEAD for comparisons

**Common Patterns**:
```sql
-- Ranking within groups
ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC)

-- Running totals
SUM(amount) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

-- Moving averages
AVG(value) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
```

### Day 9: Advanced SQL - Higher-Order Functions
**Key Concepts**:
- TRANSFORM for array element transformation
- FILTER for array filtering
- AGGREGATE for custom aggregations
- EXISTS for array element checking
- EXPLODE and EXPLODE_OUTER
- Array functions (array_contains, array_distinct, array_union)
- Struct and map operations

**Critical for Exam**:
- Know TRANSFORM syntax: `TRANSFORM(array, x -> expression)`
- Understand FILTER syntax: `FILTER(array, x -> condition)`
- Know when to use EXPLODE vs EXPLODE_OUTER
- Recognize nested data structure operations

**Common Patterns**:
```sql
-- Transform array elements
TRANSFORM(prices, p -> p * 1.1)

-- Filter array elements
FILTER(items, i -> i.quantity > 0)

-- Explode arrays to rows
SELECT id, EXPLODE(items) as item FROM orders
```

### Day 10: Python DataFrames Basics
**Key Concepts**:
- Creating DataFrames (createDataFrame, read methods)
- Reading data (CSV, JSON, Parquet, Delta)
- Writing data with modes (append, overwrite, error, ignore)
- Basic transformations (select, filter, withColumn, drop)
- Column operations (col, lit, when, otherwise)
- Schema definition and inference
- show(), display(), count(), collect()

**Critical for Exam**:
- Know DataFrame creation methods
- Understand read/write options
- Know transformation vs action operations
- Recognize column expression syntax

**Common Patterns**:
```python
# Read data
df = spark.read.format("delta").load("/path/to/table")

# Transform
df = df.filter(col("amount") > 100).withColumn("tax", col("amount") * 0.1)

# Write
df.write.format("delta").mode("overwrite").save("/path/to/output")
```

### Day 11: Python DataFrames Advanced
**Key Concepts**:
- Join types (inner, left, right, full, cross, semi, anti)
- Window functions in PySpark
- User-Defined Functions (UDFs)
- Pandas UDFs (vectorized UDFs)
- Broadcast variables
- Complex data types (arrays, structs, maps)
- explode() and posexplode()

**Critical for Exam**:
- Know all join types and their behavior
- Understand UDF vs Pandas UDF performance
- Know when to use broadcast joins
- Recognize complex type operations

**Common Patterns**:
```python
# Broadcast join
df.join(broadcast(small_df), "key")

# Window function
from pyspark.sql.window import Window
window = Window.partitionBy("category").orderBy("date")
df.withColumn("rank", row_number().over(window))

# UDF
from pyspark.sql.functions import udf
@udf("string")
def custom_func(value):
    return value.upper()
```

### Day 12: Data Ingestion Patterns
**Key Concepts**:
- COPY INTO for batch ingestion
- Auto Loader (cloudFiles) for streaming ingestion
- Schema inference and evolution
- File format options (CSV, JSON, Parquet)
- Idempotent ingestion patterns
- Error handling in ingestion
- Checkpoint management

**Critical for Exam**:
- Know COPY INTO syntax and use cases
- Understand Auto Loader benefits
- Know schema evolution options
- Recognize idempotent patterns

**Common Patterns**:
```sql
-- COPY INTO
COPY INTO target_table
FROM '/path/to/files'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')

-- Auto Loader (Python)
spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .load("/path/to/files")
```

### Day 13: Data Transformation Patterns
**Key Concepts**:
- Null handling (dropna, fillna, coalesce)
- Duplicate removal (distinct, dropDuplicates)
- Type conversions (cast, to_timestamp, to_date)
- String manipulation (trim, lower, upper, regexp_replace)
- Date transformations (date_add, date_diff, date_trunc)
- Data validation patterns
- Regular expressions

**Critical for Exam**:
- Know null handling methods
- Understand type casting
- Know string and date functions
- Recognize validation patterns

**Common Patterns**:
```python
# Null handling
df.fillna({"age": 0, "name": "Unknown"})
df.withColumn("value", coalesce(col("value"), lit(0)))

# Type conversion
df.withColumn("date", to_timestamp(col("date_str"), "yyyy-MM-dd"))

# String cleaning
df.withColumn("email", lower(trim(col("email"))))
```

### Day 14: ELT Best Practices
**Key Concepts**:
- Medallion Architecture (Bronze, Silver, Gold)
- Incremental processing patterns
- Data quality validation
- Idempotent pipeline design
- Error handling and dead letter queues
- Performance optimization (Z-ordering, partitioning)
- Naming conventions and documentation
- Pipeline orchestration

**Critical for Exam**:
- **MOST IMPORTANT**: Understand Medallion Architecture layers
- Know idempotent design principles
- Understand data quality patterns
- Know optimization techniques
- Recognize production best practices

**Common Patterns**:
```python
# Bronze: Raw ingestion
spark.readStream.format("cloudFiles").load("/raw")
  .withColumn("_ingestion_time", current_timestamp())
  .writeStream.table("bronze.orders")

# Silver: Cleaning
spark.table("bronze.orders")
  .filter(col("id").isNotNull())
  .dropDuplicates(["id"])
  .writeStream.table("silver.orders")

# Gold: Aggregation
spark.table("silver.orders")
  .groupBy("date", "customer_id")
  .agg(sum("amount").alias("total"))
  .write.table("gold.daily_summary")
```

## Key Exam Topics from Week 2

### 1. Window Functions (High Priority)
- Ranking functions: ROW_NUMBER, RANK, DENSE_RANK
- Aggregate functions with windows
- Frame specifications
- LAG/LEAD for row comparisons

### 2. Higher-Order Functions (Medium Priority)
- TRANSFORM for array transformations
- FILTER for array filtering
- EXPLODE for array expansion
- Nested data structure operations

### 3. DataFrame API (High Priority)
- Read/write operations
- Transformations vs actions
- Join types and syntax
- Column expressions

### 4. Data Ingestion (High Priority)
- COPY INTO vs Auto Loader
- Schema evolution
- Idempotent patterns
- Error handling

### 5. Medallion Architecture (Very High Priority)
- Bronze, Silver, Gold layer purposes
- Incremental processing
- Data quality validation
- Performance optimization

## Common Exam Question Patterns

### Pattern 1: Choose the Correct Function
"Which window function assigns unique sequential integers starting from 1?"
- Know: ROW_NUMBER (always unique), RANK (gaps), DENSE_RANK (no gaps)

### Pattern 2: Identify the Error
"What's wrong with this code?"
```python
df.write.mode("append").save("/path")  # Missing format
```
- Should specify format: `.format("delta")`

### Pattern 3: Best Practice Selection
"What's the best way to ingest streaming files?"
- Auto Loader (cloudFiles) for automatic schema evolution and file tracking

### Pattern 4: Performance Optimization
"How to optimize queries filtering on customer_id?"
- Use Z-ordering: `OPTIMIZE table ZORDER BY (customer_id)`

## Week 2 Integration Project

### Project: E-Commerce ELT Pipeline

Build a complete ELT pipeline for an e-commerce platform using the Medallion Architecture.

**Requirements**:
1. **Bronze Layer**: Ingest raw order data from JSON files
2. **Silver Layer**: Clean, validate, and enrich orders
3. **Gold Layer**: Create business metrics and aggregations
4. **Data Quality**: Implement validation checks
5. **Optimization**: Apply partitioning and Z-ordering

**Data Sources**:
- Orders (JSON files with quality issues)
- Customers (CSV file)
- Products (Parquet file)

**Deliverables**:
1. Bronze ingestion pipeline
2. Silver transformation pipeline
3. Gold aggregation pipeline
4. Data quality dashboard
5. Performance optimization report

See `exercise.py` for detailed project instructions.

## Study Tips for Week 3

### What to Review
1. **Window Functions**: Practice ranking and running totals
2. **Higher-Order Functions**: Master TRANSFORM and FILTER
3. **DataFrame API**: Know all join types
4. **Medallion Architecture**: Understand each layer's purpose
5. **Data Quality**: Know validation patterns

### Practice Areas
1. Write window functions from scratch
2. Transform nested data structures
3. Implement complete ELT pipelines
4. Optimize query performance
5. Handle data quality issues

### Common Mistakes to Avoid
1. Confusing RANK vs DENSE_RANK vs ROW_NUMBER
2. Forgetting PARTITION BY in window functions
3. Using wrong join type
4. Not handling nulls properly
5. Skipping data quality checks
6. Not optimizing for performance

## Week 3 Preview

Next week focuses on **Incremental Data Processing**:
- Day 16: Structured Streaming Basics
- Day 17: Streaming Transformations
- Day 18: Delta Lake MERGE (UPSERT)
- Day 19: Change Data Capture (CDC)
- Day 20: Multi-Hop Architecture
- Day 21: Optimization Techniques
- Day 22: Week 3 Review

**Key Topics**:
- Streaming queries
- Watermarking and windowing
- MERGE operations
- CDC patterns
- Bronze → Silver → Gold streaming

## Exam Readiness Checklist

After completing Week 2, you should be able to:

### SQL Skills
- [ ] Write complex window functions with PARTITION BY and ORDER BY
- [ ] Use TRANSFORM and FILTER on arrays
- [ ] Perform all types of joins
- [ ] Handle nested data structures

### Python Skills
- [ ] Create and transform DataFrames
- [ ] Implement window functions in PySpark
- [ ] Write UDFs and Pandas UDFs
- [ ] Read/write data in multiple formats

### ELT Skills
- [ ] Design Medallion Architecture pipelines
- [ ] Implement COPY INTO and Auto Loader
- [ ] Apply data quality validation
- [ ] Optimize pipeline performance

### Best Practices
- [ ] Design idempotent pipelines
- [ ] Handle errors gracefully
- [ ] Apply naming conventions
- [ ] Document data lineage

## Additional Resources

### Databricks Documentation
- [Window Functions](https://docs.databricks.com/sql/language-manual/sql-ref-window-functions.html)
- [Higher-Order Functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions-builtin.html#higher-order-functions)
- [DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
- [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

### Practice Resources
- Review all Day 8-14 exercises
- Complete the Week 2 project
- Take the 50-question quiz
- Review incorrect answers

## Success Metrics

### Week 2 Completion Criteria
- [ ] Completed all Day 8-14 exercises
- [ ] Scored 80%+ on all daily quizzes
- [ ] Completed Week 2 integration project
- [ ] Scored 80%+ on 50-question review quiz
- [ ] Can explain Medallion Architecture
- [ ] Can write window functions confidently
- [ ] Can build complete ELT pipelines

### Ready for Week 3 If You Can:
1. Write a window function to rank sales by region
2. Use TRANSFORM to apply a 10% discount to all prices
3. Build a Bronze → Silver → Gold pipeline
4. Implement data quality checks
5. Optimize a table with Z-ordering

## Key Takeaways

### Most Important Concepts
1. **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (aggregated)
2. **Window Functions**: PARTITION BY for grouping, ORDER BY for ordering
3. **Higher-Order Functions**: TRANSFORM and FILTER for array operations
4. **Data Ingestion**: COPY INTO (batch) vs Auto Loader (streaming)
5. **Data Quality**: Validate early, fail fast, log failures

### Exam Weight
Week 2 topics represent **30% of the certification exam** (ELT with Spark SQL and Python domain). This is the largest single domain, so mastery is critical.

### Time Investment
- Week 2 content: 16 hours
- Review and practice: 4-6 hours
- Total: 20-22 hours

## Next Steps

1. **Complete the 50-question quiz** to assess your knowledge
2. **Build the Week 2 integration project** to apply concepts
3. **Review any weak areas** identified by the quiz
4. **Take a break** - you've earned it!
5. **Start Week 3** with confidence

---

**Congratulations on completing Week 2!** You now have solid ELT skills with both SQL and Python. Week 3 will build on this foundation with incremental data processing and streaming patterns.

**Ready to test your knowledge?** Proceed to `quiz.md` for the 50-question comprehensive review.

**Ready to build?** Proceed to `exercise.py` for the Week 2 integration project.

