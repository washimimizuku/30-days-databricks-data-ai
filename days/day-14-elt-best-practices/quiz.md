# Day 14: ELT Best Practices - Quiz

Test your understanding of ELT best practices and the Medallion Architecture.

**Time Limit**: 15 minutes  
**Passing Score**: 80% (12/15 correct)

---

## Questions

### Question 1
What is the primary purpose of the Bronze layer in the Medallion Architecture?

A) Store aggregated business metrics  
B) Store raw data with minimal transformations  
C) Store cleaned and validated data  
D) Store denormalized data for reporting  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Store raw data with minimal transformations**

**Explanation**: The Bronze layer stores raw data in its original format with minimal transformations (typically just adding metadata like ingestion timestamp and source file). This preserves the complete history and allows for reprocessing if needed.

</details>

---

### Question 2
Which operation mode is most appropriate for the Bronze layer?

A) Complete overwrite  
B) Append-only  
C) Merge/Upsert  
D) Delete and recreate  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Append-only**

**Explanation**: Bronze layer typically uses append-only operations to preserve all raw data history. This allows for reprocessing and auditing. Data is never deleted or updated in Bronze.

</details>

---

### Question 3
What does "idempotent" mean in the context of data pipelines?

A) The pipeline runs very fast  
B) The pipeline can be re-run safely without changing the result  
C) The pipeline processes data incrementally  
D) The pipeline handles errors gracefully  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) The pipeline can be re-run safely without changing the result**

**Explanation**: An idempotent pipeline produces the same result regardless of how many times it's executed with the same input. This is crucial for production pipelines that may need to be re-run due to failures or data corrections.

</details>

---

### Question 4
Which Delta Lake operation is best for implementing idempotent updates to a specific date partition?

A) INSERT INTO  
B) MERGE  
C) OVERWRITE with replaceWhere  
D) DELETE then INSERT  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) OVERWRITE with replaceWhere**

**Explanation**: Using `mode("overwrite")` with `option("replaceWhere", "date = '2024-01-01'")` allows you to atomically replace data for a specific partition, making the operation idempotent. Running it multiple times produces the same result.

</details>

---

### Question 5
What is the recommended approach for handling data quality failures in a production pipeline?

A) Stop the entire pipeline immediately  
B) Skip bad records and continue processing  
C) Write failed records to a dead letter queue and continue  
D) Retry indefinitely until success  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Write failed records to a dead letter queue and continue**

**Explanation**: The dead letter queue (DLQ) pattern allows the pipeline to continue processing good records while capturing failed records for investigation. This prevents a few bad records from blocking the entire pipeline.

</details>

---

### Question 6
Which of the following is NOT a typical transformation in the Silver layer?

A) Removing duplicate records  
B) Standardizing date formats  
C) Aggregating to daily summaries  
D) Validating email formats  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Aggregating to daily summaries**

**Explanation**: Aggregations are typically done in the Gold layer. The Silver layer focuses on cleaning, validating, and standardizing individual records, not aggregating them.

</details>

---

### Question 7
What is the purpose of Z-ordering in Delta Lake?

A) Sort data alphabetically  
B) Improve query performance by co-locating related data  
C) Compress data more efficiently  
D) Enable time travel queries  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Improve query performance by co-locating related data**

**Explanation**: Z-ordering (using `ZORDER BY`) co-locates related data in the same files, improving query performance for columns frequently used in WHERE clauses. It's particularly effective for high-cardinality columns.

</details>

---

### Question 8
Which streaming trigger mode processes all available data once and then stops?

A) trigger(processingTime="1 minute")  
B) trigger(once=True)  
C) trigger(availableNow=True)  
D) trigger(continuous=True)  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) trigger(availableNow=True)**

**Explanation**: `trigger(availableNow=True)` processes all available data in multiple micro-batches and then stops. This is ideal for scheduled batch jobs that use streaming APIs. Note: `trigger(once=True)` is deprecated in favor of `availableNow`.

</details>

---

### Question 9
What is the recommended maximum number of columns to use with Z-ordering?

A) 1  
B) 2  
C) 4  
D) 10  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) 4**

**Explanation**: Delta Lake recommends using Z-ordering on a maximum of 4 columns. Beyond that, the benefits diminish and the operation becomes more expensive. Choose the columns most frequently used in query filters.

</details>

---

### Question 10
Which pattern is best for tracking historical changes in a dimension table?

A) Overwrite the entire table  
B) Slowly Changing Dimension (SCD) Type 2  
C) Append-only with duplicates  
D) Delete old records and insert new ones  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Slowly Changing Dimension (SCD) Type 2**

**Explanation**: SCD Type 2 maintains historical records by adding new rows for changes and marking old rows as inactive (using effective_date, end_date, and is_current columns). This preserves the complete history of changes.

</details>

---

### Question 11
What is the purpose of adding audit columns (_created_at, _created_by) to tables?

A) Improve query performance  
B) Track data lineage and changes  
C) Enable time travel  
D) Reduce storage costs  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Track data lineage and changes**

**Explanation**: Audit columns help track when records were created/modified and by whom. This is essential for debugging, compliance, and understanding data lineage in production systems.

</details>

---

### Question 12
Which operation should be run regularly to remove old data files and reclaim storage?

A) OPTIMIZE  
B) VACUUM  
C) ANALYZE  
D) COMPACT  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) VACUUM**

**Explanation**: VACUUM removes old data files that are no longer referenced by the Delta table (after the retention period). This reclaims storage space. The default retention is 7 days, but 30 days is recommended for production.

</details>

---

### Question 13
What is the recommended approach for joining a large fact table with a small dimension table?

A) Sort merge join  
B) Broadcast join  
C) Shuffle hash join  
D) Nested loop join  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Broadcast join**

**Explanation**: Broadcast joins are most efficient when one table is small enough to fit in memory. Spark broadcasts the small table to all executors, avoiding expensive shuffles. Use `broadcast(small_df)` to explicitly trigger this.

</details>

---

### Question 14
Which Gold layer pattern is most appropriate for a dashboard showing real-time metrics?

A) Full table overwrite daily  
B) Incremental aggregation with MERGE  
C) Append-only with duplicates  
D) Delete and recreate hourly  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Incremental aggregation with MERGE**

**Explanation**: Incremental aggregation with MERGE allows you to update only the changed metrics without reprocessing all historical data. This is efficient and provides near real-time updates for dashboards.

</details>

---

### Question 15
What is the primary benefit of using Auto Loader (cloudFiles) over traditional batch reads?

A) Faster query performance  
B) Automatic schema inference and evolution  
C) Lower storage costs  
D) Better compression  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Automatic schema inference and evolution**

**Explanation**: Auto Loader (cloudFiles) provides automatic schema inference and evolution, efficient incremental processing, and built-in file tracking. It automatically handles new files and schema changes without manual intervention.

</details>

---

## Scoring Guide

- **15/15**: Excellent! You've mastered ELT best practices
- **12-14**: Good understanding, review missed topics
- **9-11**: Fair, review Medallion Architecture and optimization techniques
- **Below 9**: Review the README.md and practice exercises again

## Key Topics to Review

If you scored below 80%, focus on these areas:

1. **Medallion Architecture**: Understand the purpose of each layer (Bronze, Silver, Gold)
2. **Idempotency**: Learn how to design pipelines that can be safely re-run
3. **Data Quality**: Implement validation checks and dead letter queues
4. **Performance Optimization**: Master Z-ordering, partitioning, and broadcast joins
5. **Pipeline Patterns**: Know when to use append, merge, or overwrite operations

---

**Next Steps**: 
- Review any incorrect answers
- Practice implementing complete pipelines in exercise.py
- Move on to Day 15: Week 2 Review & ELT Project

