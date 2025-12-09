# Day 15: Week 2 Review - Comprehensive Quiz

Test your understanding of all Week 2 topics (Days 8-14).

**Time Limit**: 60 minutes  
**Passing Score**: 80% (40/50 correct)  
**Topics**: Window Functions, Higher-Order Functions, DataFrames, Data Ingestion, Transformation, ELT Best Practices

---

## SECTION 1: Window Functions (Questions 1-10)

### Question 1
What is the difference between ROW_NUMBER() and RANK()?

A) ROW_NUMBER() always assigns unique sequential integers; RANK() can have gaps  
B) RANK() always assigns unique sequential integers; ROW_NUMBER() can have gaps  
C) They are identical functions  
D) ROW_NUMBER() is faster than RANK()  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) ROW_NUMBER() always assigns unique sequential integers; RANK() can have gaps**

**Explanation**: ROW_NUMBER() assigns unique sequential integers (1, 2, 3, 4...) even for ties. RANK() assigns the same rank to ties and skips the next rank (1, 2, 2, 4...). DENSE_RANK() doesn't skip ranks (1, 2, 2, 3...).

</details>

---

### Question 2
Which clause is required in a window function to define groups?

A) GROUP BY  
B) PARTITION BY  
C) ORDER BY  
D) ROWS BETWEEN  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) PARTITION BY**

**Explanation**: PARTITION BY divides the result set into partitions (groups) for the window function. It's similar to GROUP BY but doesn't collapse rows. ORDER BY is optional and defines ordering within partitions.

</details>

---

### Question 3
What does this window frame specification mean: `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW`?

A) Last 6 rows only  
B) Current row and next 6 rows  
C) Current row and previous 6 rows (7 rows total)  
D) All rows in the partition  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Current row and previous 6 rows (7 rows total)**

**Explanation**: This frame includes the current row plus the 6 rows before it, totaling 7 rows. This is commonly used for 7-day moving averages.

</details>

---

### Question 4
Which function would you use to get the previous row's value in an ordered partition?

A) LEAD()  
B) LAG()  
C) FIRST_VALUE()  
D) LAST_VALUE()  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) LAG()**

**Explanation**: LAG() accesses data from a previous row in the same result set. LEAD() accesses data from a following row. Both require ORDER BY in the window specification.

</details>

---

### Question 5
What is the key difference between PARTITION BY and GROUP BY?

A) PARTITION BY is faster  
B) GROUP BY collapses rows; PARTITION BY doesn't  
C) They are identical  
D) PARTITION BY only works with window functions  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) GROUP BY collapses rows; PARTITION BY doesn't**

**Explanation**: GROUP BY aggregates and collapses rows into groups. PARTITION BY divides rows into groups for window functions but preserves all rows in the output.

</details>

---

### Question 6
How would you calculate a running total of sales ordered by date?

A) `SUM(sales) OVER (ORDER BY date)`  
B) `SUM(sales) GROUP BY date`  
C) `CUMSUM(sales) ORDER BY date`  
D) `TOTAL(sales) OVER (PARTITION BY date)`  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) `SUM(sales) OVER (ORDER BY date)`**

**Explanation**: This creates a running total by summing all rows from the start up to the current row. The default frame is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.

</details>

---

### Question 7
Which function divides rows into N equal groups?

A) RANK()  
B) NTILE()  
C) DENSE_RANK()  
D) PERCENT_RANK()  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) NTILE()**

**Explanation**: NTILE(n) divides rows into n approximately equal groups, assigning a number from 1 to n to each row. Commonly used for quartiles (NTILE(4)) or percentiles.

</details>

---

### Question 8
What happens if you use a window function without PARTITION BY?

A) Error occurs  
B) Each row becomes its own partition  
C) All rows are treated as one partition  
D) Results are undefined  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) All rows are treated as one partition**

**Explanation**: Without PARTITION BY, the entire result set is treated as a single partition, and the window function operates across all rows.

</details>

---

### Question 9
Which window frame type is based on logical ordering rather than physical rows?

A) ROWS  
B) RANGE  
C) GROUPS  
D) PARTITION  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) RANGE**

**Explanation**: RANGE frames are based on logical offsets in the ORDER BY column values, while ROWS frames are based on physical row positions. RANGE includes all rows with the same ORDER BY value.

</details>

---

### Question 10
How do you calculate the difference between current and previous row values?

A) `value - LAG(value) OVER (ORDER BY date)`  
B) `DIFF(value) OVER (ORDER BY date)`  
C) `value - PREVIOUS(value)`  
D) `DELTA(value, 1)`  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) `value - LAG(value) OVER (ORDER BY date)`**

**Explanation**: LAG() retrieves the previous row's value, which you can then subtract from the current value to calculate the difference.

</details>

---

## SECTION 2: Higher-Order Functions (Questions 11-15)

### Question 11
What does the TRANSFORM function do?

A) Converts data types  
B) Applies a function to each array element  
C) Reshapes DataFrames  
D) Optimizes queries  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Applies a function to each array element**

**Explanation**: TRANSFORM applies a lambda function to each element of an array, returning a new array with transformed elements. Syntax: `TRANSFORM(array, x -> expression)`.

</details>

---

### Question 12
Which function filters array elements based on a condition?

A) WHERE  
B) SELECT  
C) FILTER  
D) REDUCE  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) FILTER**

**Explanation**: FILTER returns a new array containing only elements that satisfy the given condition. Syntax: `FILTER(array, x -> condition)`.

</details>

---

### Question 13
What is the purpose of EXPLODE()?

A) Delete array elements  
B) Convert array elements to separate rows  
C) Combine multiple arrays  
D) Sort array elements  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Convert array elements to separate rows**

**Explanation**: EXPLODE creates a new row for each element in an array or map. EXPLODE_OUTER does the same but preserves rows with null or empty arrays.

</details>

---

### Question 14
What's the difference between EXPLODE and EXPLODE_OUTER?

A) EXPLODE is faster  
B) EXPLODE_OUTER preserves rows with null/empty arrays  
C) They are identical  
D) EXPLODE_OUTER works with maps only  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) EXPLODE_OUTER preserves rows with null/empty arrays**

**Explanation**: EXPLODE drops rows with null or empty arrays. EXPLODE_OUTER preserves these rows with NULL values, similar to an outer join.

</details>

---

### Question 15
How do you check if an array contains a specific value?

A) `CONTAINS(array, value)`  
B) `array_contains(array, value)`  
C) `EXISTS(array, x -> x = value)`  
D) Both B and C  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) Both B and C**

**Explanation**: Both `array_contains(array, value)` and `EXISTS(array, x -> x = value)` check if an array contains a value. EXISTS is more flexible as it allows complex conditions.

</details>

---

## SECTION 3: Python DataFrames (Questions 16-25)

### Question 16
Which method reads a Delta table in PySpark?

A) `spark.read.delta("/path")`  
B) `spark.read.format("delta").load("/path")`  
C) `spark.table("table_name")`  
D) Both B and C  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) Both B and C**

**Explanation**: You can read Delta tables using `spark.read.format("delta").load("/path")` for file paths or `spark.table("table_name")` for registered tables.

</details>

---

### Question 17
What's the difference between a transformation and an action in Spark?

A) Transformations are faster  
B) Transformations are lazy; actions trigger execution  
C) Actions are lazy; transformations trigger execution  
D) No difference  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Transformations are lazy; actions trigger execution**

**Explanation**: Transformations (filter, select, withColumn) are lazy and build an execution plan. Actions (count, collect, show) trigger actual computation.

</details>

---

### Question 18
Which write mode appends data without checking for duplicates?

A) overwrite  
B) append  
C) error  
D) ignore  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) append**

**Explanation**: `mode("append")` adds new data to existing data without checking for duplicates. Use MERGE for deduplication during append.

</details>

---

### Question 19
How do you create a new column based on a condition?

A) `df.withColumn("new_col", if(condition, value1, value2))`  
B) `df.withColumn("new_col", when(condition, value1).otherwise(value2))`  
C) `df.addColumn("new_col", condition ? value1 : value2)`  
D) `df.select(case(condition, value1, value2))`  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) `df.withColumn("new_col", when(condition, value1).otherwise(value2))`**

**Explanation**: Use `when().otherwise()` for conditional logic in PySpark. You can chain multiple `when()` clauses for multiple conditions.

</details>

---

### Question 20
What does `col("amount").cast("decimal(10,2)")` do?

A) Rounds to 2 decimal places  
B) Converts to decimal type with 10 total digits, 2 after decimal  
C) Multiplies by 100  
D) Formats as currency  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Converts to decimal type with 10 total digits, 2 after decimal**

**Explanation**: `cast("decimal(10,2)")` converts to decimal type with precision 10 (total digits) and scale 2 (digits after decimal point).

</details>

---

### Question 21
Which join type returns only rows that have matches in both DataFrames?

A) left  
B) right  
C) inner  
D) full  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) inner**

**Explanation**: Inner join returns only rows where the join condition is met in both DataFrames. It's the default join type.

</details>

---

### Question 22
What is a broadcast join used for?

A) Joining large tables  
B) Joining a small table with a large table efficiently  
C) Broadcasting data to all nodes  
D) Parallel processing  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Joining a small table with a large table efficiently**

**Explanation**: Broadcast joins send a small table to all executors, avoiding expensive shuffles. Use `broadcast(small_df)` to explicitly trigger this optimization.

</details>

---

### Question 23
How do you handle null values by replacing them with a default?

A) `df.fillna({"col": default_value})`  
B) `df.withColumn("col", coalesce(col("col"), lit(default_value)))`  
C) `df.na.fill({"col": default_value})`  
D) All of the above  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) All of the above**

**Explanation**: All three methods replace nulls with default values. `fillna()` and `na.fill()` are equivalent. `coalesce()` returns the first non-null value.

</details>

---

### Question 24
What's the difference between UDF and Pandas UDF?

A) UDFs are faster  
B) Pandas UDFs are vectorized and faster  
C) They are identical  
D) Pandas UDFs only work with Pandas DataFrames  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Pandas UDFs are vectorized and faster**

**Explanation**: Pandas UDFs (vectorized UDFs) process data in batches using Apache Arrow, making them significantly faster than row-at-a-time UDFs.

</details>

---

### Question 25
How do you remove duplicate rows based on specific columns?

A) `df.distinct(["col1", "col2"])`  
B) `df.dropDuplicates(["col1", "col2"])`  
C) `df.unique(["col1", "col2"])`  
D) `df.deduplicate(["col1", "col2"])`  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) `df.dropDuplicates(["col1", "col2"])`**

**Explanation**: `dropDuplicates()` removes duplicate rows based on specified columns. `distinct()` removes duplicates based on all columns.

</details>

---

## SECTION 4: Data Ingestion (Questions 26-32)

### Question 26
What is the primary advantage of Auto Loader over COPY INTO?

A) Faster processing  
B) Automatic schema evolution and file tracking  
C) Lower cost  
D) Better compression  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Automatic schema evolution and file tracking**

**Explanation**: Auto Loader (cloudFiles) automatically tracks processed files, handles schema evolution, and provides efficient incremental processing without manual file management.

</details>

---

### Question 27
Which format should you use for Auto Loader?

A) delta  
B) cloudFiles  
C) autoloader  
D) streaming  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) cloudFiles**

**Explanation**: Auto Loader uses the `cloudFiles` format: `spark.readStream.format("cloudFiles").option("cloudFiles.format", "json")`.

</details>

---

### Question 28
What does COPY INTO do if a file has already been processed?

A) Processes it again  
B) Skips it (idempotent)  
C) Throws an error  
D) Overwrites previous data  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Skips it (idempotent)**

**Explanation**: COPY INTO tracks processed files and skips them on subsequent runs, making it idempotent. This prevents duplicate data ingestion.

</details>

---

### Question 29
How do you enable schema evolution in COPY INTO?

A) `COPY_OPTIONS ('mergeSchema' = 'true')`  
B) `FORMAT_OPTIONS ('evolveSchema' = 'true')`  
C) `SCHEMA_OPTIONS ('autoMerge' = 'true')`  
D) Schema evolution is automatic  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) `COPY_OPTIONS ('mergeSchema' = 'true')`**

**Explanation**: Use `COPY_OPTIONS ('mergeSchema' = 'true')` to allow COPY INTO to handle schema changes by adding new columns.

</details>

---

### Question 30
What is a checkpoint in streaming?

A) A backup of data  
B) A location to track streaming progress for fault tolerance  
C) A performance optimization  
D) A data validation step  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) A location to track streaming progress for fault tolerance**

**Explanation**: Checkpoints store the streaming query's progress, enabling recovery from failures and ensuring exactly-once processing semantics.

</details>

---

### Question 31
Which trigger mode processes all available data once and stops?

A) `trigger(once=True)`  
B) `trigger(availableNow=True)`  
C) `trigger(processingTime="1 minute")`  
D) `trigger(continuous=True)`  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) `trigger(availableNow=True)`**

**Explanation**: `trigger(availableNow=True)` processes all available data in micro-batches and stops. It's ideal for scheduled batch jobs using streaming APIs.

</details>

---

### Question 32
What happens if you don't specify a checkpoint location for a streaming query?

A) Uses default location  
B) Error occurs  
C) Query runs but can't recover from failures  
D) Checkpoint is disabled  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Error occurs**

**Explanation**: Checkpoint location is required for streaming queries. Without it, Spark cannot track progress or recover from failures.

</details>

---

## SECTION 5: Data Transformation (Questions 33-38)

### Question 33
Which function parses a string to a timestamp?

A) `to_timestamp(col, format)`  
B) `parse_timestamp(col, format)`  
C) `cast(col as timestamp)`  
D) `timestamp(col, format)`  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) `to_timestamp(col, format)`**

**Explanation**: `to_timestamp()` parses a string to timestamp using the specified format pattern (e.g., "yyyy-MM-dd HH:mm:ss").

</details>

---

### Question 34
How do you handle multiple date formats in a single column?

A) Use multiple to_timestamp calls with coalesce  
B) Use try_to_timestamp  
C) Parse manually with regex  
D) Both A and B  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) Both A and B**

**Explanation**: Use `coalesce(to_timestamp(col, format1), to_timestamp(col, format2))` to try multiple formats, or use `try_to_timestamp()` which returns null on failure.

</details>

---

### Question 35
Which function removes leading and trailing whitespace?

A) `strip()`  
B) `trim()`  
C) `clean()`  
D) `normalize()`  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) `trim()`**

**Explanation**: `trim()` removes leading and trailing whitespace. Use `ltrim()` for leading only or `rtrim()` for trailing only.

</details>

---

### Question 36
How do you replace values using regex?

A) `replace(col, pattern, replacement)`  
B) `regexp_replace(col, pattern, replacement)`  
C) `substitute(col, pattern, replacement)`  
D) `regex_replace(col, pattern, replacement)`  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) `regexp_replace(col, pattern, replacement)`**

**Explanation**: `regexp_replace()` replaces all occurrences matching the regex pattern with the replacement string.

</details>

---

### Question 37
What does `dropna(thresh=3)` do?

A) Drops rows with more than 3 nulls  
B) Drops rows with less than 3 non-null values  
C) Drops rows with exactly 3 nulls  
D) Keeps only 3 rows  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Drops rows with less than 3 non-null values**

**Explanation**: `thresh=3` means "threshold" - keep rows that have at least 3 non-null values. Rows with fewer than 3 non-null values are dropped.

</details>

---

### Question 38
How do you calculate the number of days between two dates?

A) `days_between(date1, date2)`  
B) `datediff(date1, date2)`  
C) `date_diff(date1, date2)`  
D) `date1 - date2`  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) `datediff(date1, date2)`**

**Explanation**: `datediff(end_date, start_date)` returns the number of days between two dates as an integer.

</details>

---

## SECTION 6: ELT Best Practices & Medallion Architecture (Questions 39-50)

### Question 39
What is the purpose of the Bronze layer in Medallion Architecture?

A) Store aggregated metrics  
B) Store raw data with minimal transformations  
C) Store cleaned data  
D) Store business-level data  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Store raw data with minimal transformations**

**Explanation**: Bronze layer stores raw data in its original format with minimal transformations (typically just adding ingestion metadata). This preserves complete history.

</details>

---

### Question 40
Which layer in Medallion Architecture applies data quality validation?

A) Bronze  
B) Silver  
C) Gold  
D) All layers  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Silver**

**Explanation**: Silver layer applies data cleaning, validation, and standardization. Bronze preserves raw data, and Gold focuses on business aggregations.

</details>

---

### Question 41
What does "idempotent" mean for a data pipeline?

A) Runs very fast  
B) Can be re-run safely producing the same result  
C) Processes data incrementally  
D) Handles errors gracefully  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Can be re-run safely producing the same result**

**Explanation**: An idempotent pipeline produces the same result regardless of how many times it's executed with the same input, making it safe to re-run.

</details>

---

### Question 42
Which operation mode is most appropriate for Bronze layer?

A) Overwrite  
B) Append-only  
C) Merge/Upsert  
D) Delete and recreate  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Append-only**

**Explanation**: Bronze layer typically uses append-only operations to preserve all raw data history without modifications or deletions.

</details>

---

### Question 43
What is a dead letter queue (DLQ)?

A) A backup storage location  
B) A table for storing failed/invalid records  
C) A queue for processing delays  
D) A performance optimization technique  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) A table for storing failed/invalid records**

**Explanation**: DLQ stores records that fail validation or processing, allowing the pipeline to continue while preserving failed records for investigation.

</details>

---

### Question 44
What is Z-ordering used for?

A) Sorting data alphabetically  
B) Co-locating related data to improve query performance  
C) Compressing data  
D) Partitioning tables  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Co-locating related data to improve query performance**

**Explanation**: Z-ordering co-locates related data in the same files, improving performance for queries filtering on those columns. Use `OPTIMIZE table ZORDER BY (col1, col2)`.

</details>

---

### Question 45
What is the recommended maximum number of columns for Z-ordering?

A) 1  
B) 2  
C) 4  
D) 10  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) 4**

**Explanation**: Databricks recommends Z-ordering on a maximum of 4 columns. Beyond that, benefits diminish and the operation becomes more expensive.

</details>

---

### Question 46
Which Delta Lake operation removes old data files?

A) OPTIMIZE  
B) VACUUM  
C) CLEAN  
D) PURGE  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) VACUUM**

**Explanation**: VACUUM removes old data files that are no longer referenced by the Delta table (after the retention period, default 7 days).

</details>

---

### Question 47
How do you make a partition-specific update idempotent?

A) Use INSERT OVERWRITE  
B) Use mode("overwrite") with replaceWhere option  
C) Use MERGE  
D) Use DELETE then INSERT  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Use mode("overwrite") with replaceWhere option**

**Explanation**: `mode("overwrite").option("replaceWhere", "date = '2024-01-01'")` atomically replaces only the specified partition, making it idempotent.

</details>

---

### Question 48
What should you add to tables for audit purposes?

A) Primary keys  
B) Indexes  
C) Audit columns (_created_at, _created_by, _updated_at)  
D) Constraints  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Audit columns (_created_at, _created_by, _updated_at)**

**Explanation**: Audit columns track when records were created/modified and by whom, essential for debugging, compliance, and data lineage.

</details>

---

### Question 49
Which Gold layer pattern is best for real-time dashboards?

A) Full table overwrite daily  
B) Incremental aggregation with MERGE  
C) Append-only  
D) Delete and recreate hourly  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Incremental aggregation with MERGE**

**Explanation**: Incremental aggregation with MERGE updates only changed metrics without reprocessing all data, providing efficient near real-time updates.

</details>

---

### Question 50
What is the primary benefit of the Medallion Architecture?

A) Faster queries  
B) Lower storage costs  
C) Progressive data quality improvement and clear data lineage  
D) Easier to learn  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Progressive data quality improvement and clear data lineage**

**Explanation**: Medallion Architecture provides a structured approach where data quality improves through each layer (Bronze → Silver → Gold) with clear lineage and separation of concerns.

</details>

---

## Scoring Guide

- **50/50 (100%)**: Outstanding! You've mastered Week 2 content
- **45-49 (90-98%)**: Excellent! Minor review needed
- **40-44 (80-88%)**: Good! Review missed topics
- **35-39 (70-78%)**: Fair - significant review needed
- **Below 35 (<70%)**: Review all Week 2 materials and retake

## Score Breakdown by Section

- **Window Functions (Q1-10)**: ___/10
- **Higher-Order Functions (Q11-15)**: ___/5
- **Python DataFrames (Q16-25)**: ___/10
- **Data Ingestion (Q26-32)**: ___/7
- **Data Transformation (Q33-38)**: ___/6
- **ELT Best Practices (Q39-50)**: ___/12

**Total Score**: ___/50 (___%)

## Areas to Review Based on Score

### If you scored below 80% on:

**Window Functions**: Review Day 8, practice ranking and running totals  
**Higher-Order Functions**: Review Day 9, practice TRANSFORM and FILTER  
**DataFrames**: Review Days 10-11, practice joins and transformations  
**Data Ingestion**: Review Day 12, understand COPY INTO vs Auto Loader  
**Data Transformation**: Review Day 13, practice data cleaning  
**ELT Best Practices**: Review Day 14, understand Medallion Architecture  

## Next Steps

1. **Score 80%+**: Proceed to Week 3 with confidence
2. **Score 70-79%**: Review weak areas, retake quiz
3. **Score <70%**: Review all Week 2 materials, complete exercises, retake quiz

---

**Congratulations on completing Week 2!** 

You're now ready for Week 3: Incremental Data Processing, which covers streaming, MERGE operations, CDC, and multi-hop architecture.

