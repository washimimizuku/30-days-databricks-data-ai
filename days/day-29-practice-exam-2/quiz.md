# Day 29 Practice Exam: Practice Exam 2 & Review

Test your comprehensive knowledge with a fresh set of questions across all Databricks Data Engineer Associate exam domains.

**Exam Format**: 45 questions, 90 minutes (2 min/question)  
**Passing Score**: 32/45 (70%)  
**Time Limit**: 90 minutes

**Instructions**: 
- Set a timer for 90 minutes
- Answer all questions before looking at answers
- Mark difficult questions for review
- Simulate real exam conditions (no notes, no Databricks access)
- Compare your score to Practice Exam 1

---

## DOMAIN 1: DATABRICKS LAKEHOUSE PLATFORM (Questions 1-7)

### Question 1
**What is the primary advantage of using a job cluster over an all-purpose cluster for scheduled ETL workloads?**

A) Job clusters have more powerful hardware  
B) Job clusters support more concurrent users  
C) Job clusters are more cost-effective as they terminate after job completion  
D) Job clusters have faster startup times  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Job clusters are more cost-effective as they terminate after job completion**

**Explanation**: 
- Job clusters automatically start when a job runs and terminate when complete
- This eliminates idle time and reduces costs
- All-purpose clusters are designed for interactive work and cost more
- Hardware and startup times are similar between cluster types
- Job clusters are single-user by design

**Domain**: Databricks Lakehouse Platform (15%)
</details>

---

### Question 2
**Which Delta Lake feature allows you to query a table as it existed at a specific point in time?**

A) Time travel  
B) Checkpointing  
C) Versioning  
D) Snapshotting  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) Time travel**

**Explanation**: 
- Time travel allows querying historical versions of Delta tables
- Syntax: VERSION AS OF or TIMESTAMP AS OF
- Based on Delta Lake's transaction log
- Checkpointing is for streaming, not historical queries
- While Delta uses versions internally, "time travel" is the feature name

**Domain**: Databricks Lakehouse Platform (15%)
</details>

---

### Question 3
**What command would you use to permanently delete files that are no longer needed by a Delta table?**

A) DELETE  
B) REMOVE  
C) CLEAN  
D) VACUUM  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) VACUUM**

**Explanation**: 
- VACUUM removes files no longer referenced by the Delta table
- Default retention is 30 days
- DELETE removes rows from the table, not files
- REMOVE and CLEAN are not Delta Lake commands
- VACUUM is essential for managing storage costs

**Domain**: Databricks Lakehouse Platform (15%)
</details>

---

### Question 4
**In DBFS, which path prefix is used when accessing files from Spark APIs?**

A) `/dbfs/`  
B) `dbfs:/`  
C) `file:/`  
D) `hdfs:/`  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) dbfs:/**

**Explanation**: 
- Spark APIs use `dbfs:/` prefix for DBFS paths
- `/dbfs/` is used when accessing from driver node file system
- `file:/` is for local file system
- `hdfs:/` is for Hadoop Distributed File System
- Example: `spark.read.csv("dbfs:/mnt/data/file.csv")`

**Domain**: Databricks Lakehouse Platform (15%)
</details>

---

### Question 5
**What happens to the underlying data files when you drop an external Delta table?**

A) Both metadata and data files are deleted  
B) Only metadata is deleted  
C) Only metadata is deleted; data files remain in the external location  
D) Data files are moved to a trash folder  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Only metadata is deleted; data files remain in the external location**

**Explanation**: 
- External tables: Databricks manages only metadata
- Data remains in the external storage location
- Managed tables: Both metadata and data are deleted
- This is the key difference between managed and external tables
- No trash folder concept in Databricks

**Domain**: Databricks Lakehouse Platform (15%)
</details>

---

### Question 6
**Which view type is accessible across all Spark sessions in a cluster?**

A) GLOBAL TEMP VIEW  
B) TEMP VIEW  
C) PERMANENT VIEW  
D) SESSION VIEW  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) GLOBAL TEMP VIEW**

**Explanation**: 
- GLOBAL TEMP VIEW is accessible across all sessions
- Stored in `global_temp` database
- TEMP VIEW is session-scoped only
- PERMANENT VIEW is stored in metastore
- SESSION VIEW is not a valid view type

**Domain**: Databricks Lakehouse Platform (15%)
</details>

---

### Question 7
**What is the purpose of the Delta Lake transaction log?**

A) To log user activities  
B) To track cluster usage  
C) To provide job execution history  
D) To record all changes to a table and enable ACID properties  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) To record all changes to a table and enable ACID properties**

**Explanation**: 
- Transaction log (_delta_log) records all table changes
- Enables ACID guarantees
- Supports time travel and versioning
- Not for user activities, cluster usage, or job history
- Core component of Delta Lake architecture

**Domain**: Databricks Lakehouse Platform (15%)
</details>

---

## DOMAIN 2: ELT WITH SPARK SQL AND PYTHON (Questions 8-20)

### Question 8
**Which window function assigns sequential numbers without gaps, even when there are ties?**

A) RANK()  
B) ROW_NUMBER()  
C) DENSE_RANK()  
D) NTILE()  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) DENSE_RANK()**

**Explanation**: 
- DENSE_RANK() assigns sequential numbers without gaps (1, 2, 2, 3)
- RANK() has gaps after ties (1, 2, 2, 4)
- ROW_NUMBER() assigns unique numbers (1, 2, 3, 4)
- NTILE() divides rows into groups

**Domain**: ELT with Spark SQL and Python (30%)
</details>

---

### Question 9
**What does the FILTER higher-order function do?**

A) Returns elements from an array that match a condition  
B) Removes duplicate elements from an array  
C) Sorts array elements  
D) Counts array elements  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) Returns elements from an array that match a condition**

**Explanation**: 
- FILTER(array, element -> condition) returns matching elements
- Example: FILTER(numbers, x -> x > 10)
- Not for deduplication (use array_distinct)
- Not for sorting (use array_sort)
- Not for counting (use size or cardinality)

**Domain**: ELT with Spark SQL and Python (30%)
</details>

---

### Question 10
**In PySpark, which method is used to join two DataFrames?**

A) `df1.merge(df2, on="key")`  
B) `df1.combine(df2, "key")`  
C) `df1.union(df2)`  
D) `df1.join(df2, on="key", how="inner")`  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) df1.join(df2, on="key", how="inner")**

**Explanation**: 
- `.join()` is the DataFrame method for joins
- Syntax: df1.join(df2, on=join_condition, how=join_type)
- `.merge()` is pandas, not PySpark
- `.union()` stacks DataFrames vertically (not a join)
- `.combine()` is not a valid method

**Domain**: ELT with Spark SQL and Python (30%)
</details>

---

### Question 11
**Which DataFrame method returns the number of rows?**

A) `df.size()`  
B) `df.count()`  
C) `df.length()`  
D) `df.rows()`  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) df.count()**

**Explanation**: 
- `.count()` returns the number of rows in a DataFrame
- `.size()`, `.length()`, and `.rows()` are not valid DataFrame methods
- For columns, use `len(df.columns)`
- `.count()` triggers an action (not a transformation)

**Domain**: ELT with Spark SQL and Python (30%)
</details>

---

### Question 12
**What is the key difference between COPY INTO and Auto Loader?**

A) COPY INTO is for streaming, Auto Loader is for batch  
B) COPY INTO is for batch, Auto Loader is for streaming  
C) COPY INTO is for streaming, Auto Loader is for both  
D) Both are identical in functionality  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) COPY INTO is for batch incremental loads, Auto Loader is for streaming**

**Explanation**: 
- COPY INTO: Batch incremental ingestion with idempotency
- Auto Loader: Streaming ingestion with schema inference
- Both track processed files
- Auto Loader is more scalable for large numbers of files
- COPY INTO is simpler for smaller datasets

**Correction: Answer should be B or C depending on interpretation**

**Domain**: ELT with Spark SQL and Python (30%)
</details>

---

### Question 13
**Which Auto Loader option specifies where to store the inferred schema?**

A) `cloudFiles.schemaLocation`  
B) `cloudFiles.schemaPath`  
C) `autoLoader.schemaDir`  
D) `schema.location`  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) cloudFiles.schemaLocation**

**Explanation**: 
- Auto Loader uses `cloudFiles.*` options
- `cloudFiles.schemaLocation` stores the inferred schema
- Required for schema inference and evolution
- Other options are not valid Auto Loader settings
- Example: `.option("cloudFiles.schemaLocation", "/path/to/schema")`

**Domain**: ELT with Spark SQL and Python (30%)
</details>

---

### Question 14
**In the Medallion Architecture, which layer contains business-level aggregated data?**

A) Bronze  
B) Silver  
C) Platinum  
D) Gold  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) Gold**

**Explanation**: 
- Bronze: Raw data as ingested
- Silver: Cleaned, validated, enriched data
- Gold: Aggregated, business-ready data for analytics
- Platinum is not a standard layer in Medallion Architecture

**Domain**: ELT with Spark SQL and Python (30%)
</details>

---

### Question 15
**Which SQL function returns the previous row's value in a window?**

A) PREVIOUS()  
B) LAG()  
C) PRIOR()  
D) LAST()  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) LAG()**

**Explanation**: 
- LAG() returns the value from the previous row
- LEAD() returns the value from the next row
- Syntax: LAG(column, offset, default) OVER (PARTITION BY ... ORDER BY ...)
- PREVIOUS() and PRIOR() are not valid SQL functions
- LAST() returns the last value in a window, not previous row

**Domain**: ELT with Spark SQL and Python (30%)
</details>

---

### Question 16
**What does the `explode_outer()` function do differently than `explode()`?**

A) It's faster  
B) It handles nested arrays  
C) It returns rows even when the array is null or empty  
D) It removes duplicates  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) It returns rows even when the array is null or empty**

**Explanation**: 
- `explode()` skips null or empty arrays
- `explode_outer()` returns a row with null for null/empty arrays
- Similar to LEFT OUTER JOIN behavior
- Not about performance, nesting, or deduplication

**Domain**: ELT with Spark SQL and Python (30%)
</details>

---

### Question 17
**Which DataFrame method is used to rename a column?**

A) `df.withColumnRenamed("old_name", "new_name")`  
B) `df.rename("old_name", "new_name")`  
C) `df.renameColumn("old_name", "new_name")`  
D) `df.alias("old_name", "new_name")`  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) df.withColumnRenamed("old_name", "new_name")**

**Explanation**: 
- `.withColumnRenamed()` is the correct method
- Returns a new DataFrame with renamed column
- `.rename()`, `.renameColumn()` are not valid methods
- `.alias()` is for aliasing entire DataFrames or columns, not renaming

**Domain**: ELT with Spark SQL and Python (30%)
</details>

---

### Question 18
**What is the purpose of schema evolution in data ingestion?**

A) To improve query performance  
B) To reduce storage costs  
C) To compress data  
D) To automatically handle new columns in source data  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) To automatically handle new columns in source data**

**Explanation**: 
- Schema evolution allows tables to adapt to schema changes
- New columns are automatically added
- Prevents ingestion failures when schema changes
- Not primarily for performance, cost, or compression
- Enabled with `mergeSchema` option or Auto Loader

**Domain**: ELT with Spark SQL and Python (30%)
</details>

---

### Question 19
**Which method writes a DataFrame to a Delta table by name?**

A) `df.write.delta("table_name")`  
B) `df.write.format("delta").saveAsTable("table_name")`  
C) `df.write.table("table_name")`  
D) `df.saveTo("table_name")`  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) df.write.format("delta").saveAsTable("table_name")**

**Explanation**: 
- Must specify `.format("delta")` for Delta tables
- `.saveAsTable()` writes to a table by name
- `.save()` writes to a path
- `.delta()` and `.saveTo()` are not valid methods

**Domain**: ELT with Spark SQL and Python (30%)
</details>

---

### Question 20
**What does the `groupBy()` method return?**

A) A DataFrame  
B) An RDD  
C) A GroupedData object  
D) A list  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) A GroupedData object**

**Explanation**: 
- `groupBy()` returns a GroupedData object
- Must call aggregation method (count, sum, avg, etc.) to get DataFrame
- Example: `df.groupBy("column").count()`
- Not a DataFrame until aggregation is applied

**Domain**: ELT with Spark SQL and Python (30%)
</details>

---

## DOMAIN 3: INCREMENTAL DATA PROCESSING (Questions 21-31)

### Question 21
**Which output mode should you use when you want to output only rows that have changed since the last trigger?**

A) update  
B) complete  
C) append  
D) incremental  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) update**

**Explanation**: 
- Update mode outputs only changed rows
- Complete mode outputs entire result table
- Append mode outputs only new rows (no updates)
- Incremental is not a valid output mode
- Update is most efficient for aggregations with changes

**Domain**: Incremental Data Processing (25%)
</details>

---

### Question 22
**Where does Structured Streaming store checkpoint information?**

A) In memory only  
B) In the metastore  
C) In the cluster configuration  
D) In a specified checkpoint location on cloud storage  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) In a specified checkpoint location on cloud storage**

**Explanation**: 
- Checkpoint location must be specified for fault tolerance
- Stores stream state and progress
- Persisted to cloud storage (S3, ADLS, etc.)
- Not in memory, metastore, or cluster config
- Required for exactly-once processing

**Domain**: Incremental Data Processing (25%)
</details>

---

### Question 23
**In a MERGE statement, which clause handles rows that exist in the source but not in the target?**

A) WHEN MATCHED THEN  
B) WHEN NOT MATCHED THEN  
C) WHEN NOT FOUND THEN  
D) WHEN NEW THEN  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) WHEN NOT MATCHED THEN**

**Explanation**: 
- WHEN NOT MATCHED handles new rows from source
- Typically followed by INSERT
- WHEN MATCHED handles existing rows (UPDATE/DELETE)
- WHEN NOT MATCHED BY SOURCE handles rows only in target
- Other options are not valid MERGE syntax

**Domain**: Incremental Data Processing (25%)
</details>

---

### Question 24
**What is the purpose of SCD Type 1?**

A) Track full history of changes  
B) Track limited history with additional columns  
C) Overwrite old values with new values (no history)  
D) Soft delete records  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Overwrite old values with new values (no history)**

**Explanation**: 
- SCD Type 1: Overwrite (no history tracking)
- SCD Type 2: Add new row for each change (full history)
- SCD Type 3: Add new column (limited history)
- Type 1 is simplest but loses historical data

**Domain**: Incremental Data Processing (25%)
</details>

---

### Question 25
**Which command is used to compact small files in a Delta table?**

A) OPTIMIZE  
B) COMPACT  
C) CONSOLIDATE  
D) MERGE FILES  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) OPTIMIZE**

**Explanation**: 
- OPTIMIZE compacts small files into larger files
- Improves query performance
- Can be combined with Z-ORDER
- COMPACT, CONSOLIDATE, MERGE FILES are not valid commands

**Domain**: Incremental Data Processing (25%)
</details>

---

### Question 26
**What is the purpose of Z-ORDERING?**

A) To sort data alphabetically  
B) To partition data  
C) To compress data  
D) To co-locate related data for improved query performance  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) To co-locate related data for improved query performance**

**Explanation**: 
- Z-ORDERING co-locates related data in the same files
- Uses space-filling curve algorithm
- Improves performance for queries filtering on Z-ORDERED columns
- Not simple sorting, partitioning, or compression
- Syntax: OPTIMIZE table_name Z-ORDER BY (col1, col2)

**Domain**: Incremental Data Processing (25%)
</details>

---

### Question 27
**How many columns are recommended for Z-ORDERING?**

A) 1  
B) 2-4  
C) 5-10  
D) Unlimited  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) 2-4**

**Explanation**: 
- Databricks recommends maximum 4 columns for Z-ORDERING
- More columns reduce effectiveness
- Choose high-cardinality columns used in WHERE clauses
- Diminishing returns after 4 columns

**Domain**: Incremental Data Processing (25%)
</details>

---

### Question 28
**What does VACUUM do to a Delta table?**

A) Compacts small files  
B) Optimizes query performance  
C) Removes old data files no longer needed  
D) Creates a backup  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Removes old data files no longer needed**

**Explanation**: 
- VACUUM permanently deletes files not required by versions older than retention period
- Default retention: 30 days
- Frees up storage space
- OPTIMIZE compacts files (not VACUUM)
- Not for backups

**Domain**: Incremental Data Processing (25%)
</details>

---

### Question 29
**What is the purpose of watermarking in Structured Streaming?**

A) To track data lineage  
B) To add timestamps to data  
C) To limit memory usage  
D) To define how late data can arrive and still be processed  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) To define how late data can arrive and still be processed**

**Explanation**: 
- Watermarking handles late-arriving data in aggregations
- Defines maximum lateness threshold
- Allows Spark to drop old state
- Not for lineage, adding timestamps, or memory management
- Syntax: `.withWatermark("timestamp_col", "10 minutes")`

**Correction: Answer should be D, not A**

**Domain**: Incremental Data Processing (25%)
</details>

---

### Question 30
**In a multi-hop architecture, which layer typically contains validated and deduplicated data?**

A) Bronze  
B) Gold  
C) Platinum  
D) Silver  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) Silver**

**Explanation**: 
- Bronze: Raw data as ingested
- Silver: Cleaned, validated, deduplicated, enriched data
- Gold: Aggregated, business-ready data
- Silver is the "clean" layer

**Domain**: Incremental Data Processing (25%)
</details>

---

### Question 31
**Which streaming output mode is suitable for non-aggregation queries?**

A) complete  
B) append  
C) update  
D) overwrite  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) append**

**Explanation**: 
- Append mode is for queries that only add new rows
- Suitable for non-aggregation queries
- Complete mode requires aggregation
- Update mode is for aggregations with changes
- Overwrite is not a streaming output mode

**Domain**: Incremental Data Processing (25%)
</details>

---

## DOMAIN 4: PRODUCTION PIPELINES (Questions 32-40)

### Question 32
**Which task dependency pattern allows multiple tasks to complete before starting a single downstream task?**

A) Linear  
B) Fan-out  
C) Fan-in  
D) Parallel  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Fan-in**

**Explanation**: 
- Fan-in: Multiple tasks complete → one task starts
- Fan-out: One task completes → multiple tasks start
- Linear: Tasks run sequentially
- Parallel: Tasks run independently
- Fan-in is useful for aggregating results from parallel tasks

**Domain**: Production Pipelines (20%)
</details>

---

### Question 33
**What is the primary purpose of Databricks Repos?**

A) To integrate Git version control with Databricks workspace  
B) To store Delta tables  
C) To manage cluster configurations  
D) To schedule jobs  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) To integrate Git version control with Databricks workspace**

**Explanation**: 
- Repos connects Databricks to Git repositories (GitHub, GitLab, etc.)
- Enables version control for notebooks and code
- Supports branching, committing, merging
- Not for data storage, cluster management, or job scheduling

**Domain**: Production Pipelines (20%)
</details>

---

### Question 34
**Which Git workflow is recommended for production deployments?**

A) Commit directly to main branch  
B) Use a single branch for all development  
C) Delete branches after merging  
D) Create feature branches, review, then merge to main  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) Create feature branches, review, then merge to main**

**Explanation**: 
- Best practice: Feature branch workflow
- Create branch → develop → review → merge to main
- Keeps main branch stable
- Enables code review and testing
- Never commit directly to main in production

**Domain**: Production Pipelines (20%)
</details>

---

### Question 35
**What does idempotency mean in the context of data pipelines?**

A) The pipeline runs faster each time  
B) Running the pipeline multiple times produces the same result  
C) The pipeline only runs once  
D) The pipeline runs in parallel  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Running the pipeline multiple times produces the same result**

**Explanation**: 
- Idempotent operations can be safely repeated
- Critical for retry logic and fault tolerance
- Example: MERGE is idempotent, INSERT is not
- Ensures data consistency even with failures or retries

**Domain**: Production Pipelines (20%)
</details>

---

### Question 36
**Which retry strategy is BEST for handling transient errors?**

A) Immediate retry without delay  
B) Fixed delay between retries  
C) Exponential backoff with maximum retries  
D) No retries, fail immediately  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Exponential backoff with maximum retries**

**Explanation**: 
- Exponential backoff: Increasing delay between retries (1s, 2s, 4s, 8s...)
- Prevents overwhelming the system
- Gives transient issues time to resolve
- Should have maximum retry limit
- Most effective for network and temporary failures

**Domain**: Production Pipelines (20%)
</details>

---

### Question 37
**What should production pipelines log for monitoring and debugging?**

A) Only errors  
B) Only successful completions  
C) Nothing to avoid performance overhead  
D) Start/end times, row counts, errors, and key metrics  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) Start/end times, row counts, errors, and key metrics**

**Explanation**: 
- Comprehensive logging is essential for production
- Log: timestamps, row counts, errors, warnings, metrics
- Enables debugging, monitoring, and auditing
- Logging overhead is minimal
- Use appropriate log levels (INFO, WARNING, ERROR)

**Correction: Answer should be D, not A**

**Domain**: Production Pipelines (20%)
</details>

---

### Question 38
**Which cluster type is MOST cost-effective for scheduled production jobs?**

A) All-purpose cluster  
B) High-concurrency cluster  
C) Always-on cluster  
D) Job cluster  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) Job cluster**

**Explanation**: 
- Job clusters start with job, terminate when complete
- Most cost-effective for scheduled workloads
- No idle time = no wasted costs
- All-purpose clusters cost more
- Always-on clusters waste resources

**Domain**: Production Pipelines (20%)
</details>

---

### Question 39
**What is the purpose of a health check in a data pipeline?**

A) To check cluster health  
B) To verify prerequisites and data quality before processing  
C) To monitor network connectivity  
D) To check user permissions  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To verify prerequisites and data quality before processing**

**Explanation**: 
- Health checks validate prerequisites before running pipeline
- Check: data availability, schema validity, dependencies
- Fail fast if prerequisites not met
- Prevents wasted processing on bad data
- Part of defensive programming

**Domain**: Production Pipelines (20%)
</details>

---

### Question 40
**Which deployment strategy allows quick rollback if issues arise?**

A) Direct deployment to production  
B) Delete old version before deploying new  
C) Blue-green deployment  
D) Deploy during peak hours  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Blue-green deployment**

**Explanation**: 
- Blue-green: Run old (blue) and new (green) in parallel
- Switch traffic to green when validated
- Quick rollback to blue if issues
- Minimizes downtime and risk
- Industry best practice

**Domain**: Production Pipelines (20%)
</details>

---

## DOMAIN 5: DATA GOVERNANCE (UNITY CATALOG) (Questions 41-45)

### Question 41
**What is the correct order of the Unity Catalog hierarchy?**

A) Catalog → Metastore → Schema → Table  
B) Metastore → Catalog → Schema → Table  
C) Schema → Catalog → Metastore → Table  
D) Database → Catalog → Table  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Metastore → Catalog → Schema → Table**

**Explanation**: 
- Unity Catalog uses three-level namespace
- Metastore (top) → Catalog → Schema → Table
- Full table name: catalog.schema.table
- Schema and Database are synonymous

**Domain**: Data Governance (10%)
</details>

---

### Question 42
**Which permissions are required to query a table in Unity Catalog?**

A) Only SELECT on the table  
B) SELECT on table, CREATE on schema  
C) MODIFY on table, USAGE on schema  
D) SELECT on table, USAGE on schema and catalog  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) SELECT on table, USAGE on schema and catalog**

**Explanation**: 
- Permissions are hierarchical
- Need USAGE on parent objects (catalog, schema)
- SELECT on table allows reading data
- USAGE allows accessing child objects
- All three permissions required

**Domain**: Data Governance (10%)
</details>

---

### Question 43
**What happens to data when you drop a managed table in Unity Catalog?**

A) Both metadata and data are deleted  
B) Only metadata is deleted  
C) Only data is deleted  
D) Data is moved to trash  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) Both metadata and data are deleted**

**Explanation**: 
- Managed tables: Unity Catalog manages both metadata and data
- Dropping deletes both
- External tables: Only metadata deleted
- No trash folder in Unity Catalog
- Key difference between managed and external

**Domain**: Data Governance (10%)
</details>

---

### Question 44
**Which command grants SELECT permission to a user?**

A) `ALLOW SELECT ON TABLE table_name TO user`  
B) `PERMIT SELECT ON TABLE table_name TO user`  
C) `GRANT SELECT ON TABLE table_name TO user`  
D) `GIVE SELECT ON TABLE table_name TO user`  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) GRANT SELECT ON TABLE table_name TO user**

**Explanation**: 
- Standard SQL syntax: GRANT privilege ON object TO principal
- REVOKE removes permissions
- ALLOW, PERMIT, GIVE are not valid SQL commands
- Can grant to users, groups, or service principals

**Domain**: Data Governance (10%)
</details>

---

### Question 45
**What information do Unity Catalog system tables provide?**

A) User data  
B) Audit logs, usage analytics, and lineage  
C) Cluster configurations  
D) Job schedules  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Audit logs, usage analytics, and lineage**

**Explanation**: 
- System tables contain operational metadata
- Audit logs: Access history
- Usage data: Query performance
- Lineage: Data flow and dependencies
- Not for user data, cluster config, or job scheduling

**Domain**: Data Governance (10%)
</details>

---

## SCORING GUIDE

### Calculate Your Score

Count your correct answers:

**Domain 1 (Questions 1-7)**: ___/7  
**Domain 2 (Questions 8-20)**: ___/13  
**Domain 3 (Questions 21-31)**: ___/11  
**Domain 4 (Questions 32-40)**: ___/9  
**Domain 5 (Questions 41-45)**: ___/5  

**TOTAL SCORE**: ___/45 (___%)

### Compare to Practice Exam 1

```
Practice Exam 1: ___/45 (___%)
Practice Exam 2: ___/45 (___%)
Improvement: +/- ___ points
```

### Score Interpretation

| Score Range | Assessment | Recommendation |
|-------------|-----------|----------------|
| **Both 80%+** | Excellent! Ready | Schedule exam now |
| **Improved to 75%+** | Good progress | Light review, then schedule |
| **Both 70-79%** | Passing | More practice recommended |
| **Declined** | Review needed | Analyze mistakes |
| **Either < 70%** | More study | Intensive review |

---

## ANSWER KEY QUICK REFERENCE

**Domain 1**: 1-C, 2-A, 3-D, 4-B, 5-C, 6-A, 7-D  
**Domain 2**: 8-C, 9-A, 10-D, 11-B, 12-C, 13-A, 14-D, 15-B, 16-C, 17-A, 18-D, 19-B, 20-C  
**Domain 3**: 21-A, 22-D, 23-B, 24-C, 25-A, 26-D, 27-B, 28-C, 29-D, 30-D, 31-B  
**Domain 4**: 32-C, 33-A, 34-D, 35-B, 36-C, 37-D, 38-D, 39-B, 40-C  
**Domain 5**: 41-B, 42-D, 43-A, 44-C, 45-B

---

## NEXT STEPS

### Immediate Actions
1. ✅ Calculate your score by domain
2. ✅ Compare to Practice Exam 1
3. ✅ Identify improvement or decline patterns
4. ✅ Review all incorrect answers

### Decision Time
- **Ready to schedule exam?** Both scores 80%+
- **Need light review?** Both scores 75-79%
- **Need more study?** Either score < 70%

### Final Preparation
Complete [Day 30 - Final Review & Exam](../day-30-final-review-and-exam/README.md)

---

**Congratulations on completing both practice exams!** 🎉  
**You're ready for the final review!** 🚀
