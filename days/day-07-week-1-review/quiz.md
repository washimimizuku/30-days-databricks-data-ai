# Day 7 Quiz: Week 1 Review

Comprehensive 50-question quiz covering all Week 1 topics (Days 1-6).

---

## SECTION 1: Databricks Architecture & Workspace (Day 1) - Questions 1-8

### Question 1
**What is the primary function of the Databricks control plane?**

A) Execute Spark jobs and process data  
B) Manage workspace UI, notebooks, and metadata  
C) Store customer data files  
D) Run cluster compute resources  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Manage workspace UI, notebooks, and metadata**

Explanation: The control plane is managed by Databricks and handles the workspace interface, notebook storage, job configurations, and metadata. The data plane handles actual data processing.
</details>

---

### Question 2
**Where does the Databricks data plane run?**

A) On Databricks-managed servers  
B) In your cloud account (AWS, Azure, or GCP)  
C) On your local machine  
D) In a shared multi-tenant environment  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) In your cloud account (AWS, Azure, or GCP)**

Explanation: The data plane runs in your cloud account, giving you control over your data and compute resources while Databricks manages the control plane.
</details>

---

### Question 3
**Which magic command is used to execute file system operations?**

A) %fs  
B) %file  
C) %dbfs  
D) %system  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) %fs**

Explanation: The %fs magic command provides shortcuts for file system operations, equivalent to dbutils.fs commands. Example: %fs ls /databricks-datasets/
</details>

---

### Question 4
**What is a key limitation of Databricks Community Edition?**

A) No SQL support  
B) Single-node clusters only  
C) No Python support  
D) Limited to 1 hour sessions  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Single-node clusters only**

Explanation: Community Edition is limited to single-node clusters, single user access, and doesn't include advanced features like RBAC or Unity Catalog.
</details>

---

### Question 5
**Which component stores notebooks in Databricks?**

A) Data plane  
B) Control plane  
C) Cluster  
D) DBFS  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Control plane**

Explanation: Notebooks are stored in the Databricks control plane, which is managed by Databricks. This allows for easy access, sharing, and version control.
</details>

---

### Question 6
**What does the %md magic command do?**

A) Creates a managed database  
B) Formats markdown text  
C) Modifies data  
D) Manages dependencies  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Formats markdown text**

Explanation: The %md magic command is used to create markdown cells in Databricks notebooks for documentation and formatted text.
</details>

---

### Question 7
**Which keyboard shortcut runs the current cell and moves to the next one?**

A) Ctrl/Cmd + Enter  
B) Shift + Enter  
C) Alt + Enter  
D) Ctrl/Cmd + R  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Shift + Enter**

Explanation: Shift + Enter runs the current cell and automatically moves to the next cell. Ctrl/Cmd + Enter runs the cell but stays in the same cell.
</details>

---

### Question 8
**What is the purpose of the Repos feature in Databricks?**

A) To store large datasets  
B) To integrate with Git for version control  
C) To create database repositories  
D) To manage cluster configurations  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To integrate with Git for version control**

Explanation: Repos allows you to connect Databricks notebooks to Git repositories (GitHub, GitLab, Bitbucket) for version control and collaboration.
</details>

---

## SECTION 2: Clusters & Compute (Day 2) - Questions 9-16

### Question 9
**What is the main difference between All-Purpose and Job clusters?**

A) All-Purpose clusters are faster  
B) Job clusters are cheaper and auto-terminate after job completion  
C) All-Purpose clusters can't run Python  
D) Job clusters don't support SQL  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Job clusters are cheaper and auto-terminate after job completion**

Explanation: Job clusters are optimized for automated workloads, cost less, and automatically terminate when the job completes. All-Purpose clusters are for interactive development.
</details>

---

### Question 10
**Which cluster mode allows multiple users to share a cluster securely?**

A) Standard mode  
B) High Concurrency mode  
C) Single Node mode  
D) Job mode  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) High Concurrency mode**

Explanation: High Concurrency mode provides process isolation and supports multiple users sharing the same cluster with security and resource management.
</details>

---

### Question 11
**What does autoscaling do?**

A) Automatically upgrades cluster version  
B) Dynamically adds/removes workers based on workload  
C) Automatically optimizes queries  
D) Scales storage capacity  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Dynamically adds/removes workers based on workload**

Explanation: Autoscaling automatically adjusts the number of worker nodes based on workload, optimizing cost and performance.
</details>

---

### Question 12
**What is a DBU?**

A) Database Unit  
B) Databricks Unit (processing capability)  
C) Data Backup Unit  
D) Distributed Batch Unit  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Databricks Unit (processing capability)**

Explanation: DBU (Databricks Unit) is a unit of processing capability per hour, used for billing. Different cluster types and sizes consume different DBU rates.
</details>

---

### Question 13
**What happens when auto-termination is enabled?**

A) Cluster terminates after specified idle time  
B) Cluster restarts automatically  
C) Cluster upgrades automatically  
D) Cluster scales down to zero  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) Cluster terminates after specified idle time**

Explanation: Auto-termination automatically shuts down a cluster after a specified period of inactivity, saving costs.
</details>

---

### Question 14
**Which cluster mode is most cost-effective for a single developer?**

A) High Concurrency  
B) Standard  
C) Single Node  
D) Multi-node  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Single Node**

Explanation: Single Node mode has no worker nodes, only a driver, making it the most cost-effective option for single-user development and testing.
</details>

---

### Question 15
**What is the purpose of cluster policies?**

A) To define security rules  
B) To enforce cluster configuration standards and control costs  
C) To schedule cluster restarts  
D) To manage data access  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To enforce cluster configuration standards and control costs**

Explanation: Cluster policies allow administrators to limit cluster configuration options, enforce standards, and control costs by restricting instance types and sizes.
</details>

---

### Question 16
**When should you use a Job cluster instead of an All-Purpose cluster?**

A) For interactive development  
B) For scheduled, automated workloads  
C) For ad-hoc queries  
D) For notebook exploration  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) For scheduled, automated workloads**

Explanation: Job clusters are designed for scheduled, automated workloads. They're cheaper, auto-terminate after completion, and are optimized for production jobs.
</details>

---

## SECTION 3: Delta Lake Fundamentals (Day 3) - Questions 17-24

### Question 17
**What does ACID stand for in Delta Lake?**

A) Automated, Consistent, Isolated, Durable  
B) Atomicity, Consistency, Isolation, Durability  
C) Automatic, Complete, Independent, Distributed  
D) Advanced, Concurrent, Integrated, Dynamic  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Atomicity, Consistency, Isolation, Durability**

Explanation: ACID properties ensure reliable transactions: Atomicity (all or nothing), Consistency (valid state), Isolation (concurrent transactions), Durability (permanent changes).
</details>

---

### Question 18
**How do you query a Delta table as it was 5 versions ago?**

A) SELECT * FROM table HISTORY 5  
B) SELECT * FROM table VERSION AS OF 5  
C) SELECT * FROM table PREVIOUS 5  
D) SELECT * FROM table ROLLBACK 5  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) SELECT * FROM table VERSION AS OF 5**

Explanation: Time Travel syntax uses VERSION AS OF for version numbers or TIMESTAMP AS OF for specific timestamps.
</details>

---

### Question 19
**What is the default retention period for VACUUM?**

A) 1 day  
B) 7 days  
C) 30 days  
D) 90 days  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) 7 days**

Explanation: VACUUM default retention is 7 days to preserve Time Travel capability. Files older than 7 days (not required by current table version) are deleted.
</details>

---

### Question 20
**What does OPTIMIZE do?**

A) Compacts small files into larger files  
B) Deletes old files  
C) Creates indexes  
D) Backs up data  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) Compacts small files into larger files**

Explanation: OPTIMIZE compacts small files into larger ones, improving read performance by reducing the number of files to scan.
</details>

---

### Question 21
**What is Z-ORDER BY used for?**

A) Sorting data alphabetically  
B) Co-locating related data in the same files  
C) Creating indexes  
D) Partitioning tables  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Co-locating related data in the same files**

Explanation: Z-ORDER BY co-locates related information in the same set of files, improving query performance for frequently filtered columns.
</details>

---

### Question 22
**Where is the Delta Lake transaction log stored?**

A) In a separate database  
B) In the _delta_log subdirectory  
C) In the control plane  
D) In a system table  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) In the _delta_log subdirectory**

Explanation: The transaction log is stored in the _delta_log subdirectory of the table location, containing JSON files tracking all changes.
</details>

---

### Question 23
**What command shows the history of operations on a Delta table?**

A) SHOW HISTORY table_name  
B) DESCRIBE HISTORY table_name  
C) SELECT HISTORY FROM table_name  
D) GET HISTORY table_name  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) DESCRIBE HISTORY table_name**

Explanation: DESCRIBE HISTORY shows all operations performed on a Delta table, including version, timestamp, operation, and user.
</details>

---

### Question 24
**Can you run VACUUM immediately after creating a table?**

A) Yes, it's recommended  
B) No, there are no old files to remove yet  
C) Yes, but only with DRY RUN  
D) No, you must wait 7 days  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) No, there are no old files to remove yet**

Explanation: VACUUM removes files no longer referenced by the table and older than the retention period. A newly created table has no old files to remove.
</details>

---

## SECTION 4: DBFS (Day 4) - Questions 25-30

### Question 25
**What does DBFS stand for?**

A) Database File System  
B) Databricks File System  
C) Distributed Batch File System  
D) Delta Binary File System  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Databricks File System**

Explanation: DBFS (Databricks File System) is a distributed file system mounted into Databricks workspaces, providing access to cloud storage.
</details>

---

### Question 26
**Which dbutils.fs command lists files in a directory?**

A) dbutils.fs.list()  
B) dbutils.fs.ls()  
C) dbutils.fs.dir()  
D) dbutils.fs.show()  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) dbutils.fs.ls()**

Explanation: dbutils.fs.ls() lists the contents of a directory. It returns a list of FileInfo objects with name, path, size, and modification time.
</details>

---

### Question 27
**What is the purpose of /FileStore/ in DBFS?**

A) To store Delta tables  
B) To make files web-accessible  
C) To store system files  
D) To cache data  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To make files web-accessible**

Explanation: /FileStore/ is a special DBFS location where files can be accessed via web URLs, useful for sharing files or loading data from external sources.
</details>

---

### Question 28
**Which path format is used in Python for DBFS?**

A) /dbfs/path  
B) dbfs:/path  
C) Both A and B  
D) file:/path  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Both A and B**

Explanation: DBFS can be accessed using /dbfs/ (local file system path) or dbfs:/ (DBFS protocol). Use /dbfs/ for Python file operations, dbfs:/ for Spark operations.
</details>

---

### Question 29
**How do you copy a file in DBFS?**

A) dbutils.fs.copy(source, destination)  
B) dbutils.fs.cp(source, destination)  
C) Both A and B  
D) dbutils.fs.move(source, destination)  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) dbutils.fs.cp(source, destination)**

Explanation: dbutils.fs.cp() copies files or directories. Use recurse=True for directories.
</details>

---

### Question 30
**What happens when you delete a file using dbutils.fs.rm()?**

A) File is moved to trash  
B) File is permanently deleted  
C) File is archived  
D) File is marked for deletion  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) File is permanently deleted**

Explanation: dbutils.fs.rm() permanently deletes files. There is no trash or recycle bin. Use with caution!
</details>

---

## SECTION 5: Databases, Tables, and Views (Day 5) - Questions 31-40

### Question 31
**What happens to data files when you DROP a managed table?**

A) Data files remain in storage  
B) Data files are deleted permanently  
C) Data files are moved to trash  
D) Data files are archived  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Data files are deleted permanently**

Explanation: When you DROP a managed table, Databricks deletes both the metadata AND the data files. This is the key difference from external tables.
</details>

---

### Question 32
**What happens to data files when you DROP an external table?**

A) Data files are deleted permanently  
B) Data files remain in their location  
C) Data files are moved to trash  
D) Data files are converted to Parquet  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Data files remain in their location**

Explanation: When you DROP an external table, only the metadata is deleted. The data files remain in the specified location.
</details>

---

### Question 33
**What is the default storage location for managed tables?**

A) /mnt/data/  
B) /user/hive/warehouse/  
C) /dbfs/tables/  
D) /FileStore/tables/  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) /user/hive/warehouse/**

Explanation: Managed tables are stored by default in /user/hive/warehouse/<database>.db/<table>/ unless a custom location is specified.
</details>

---

### Question 34
**How do you access a global temporary view?**

A) SELECT * FROM view_name  
B) SELECT * FROM temp.view_name  
C) SELECT * FROM global_temp.view_name  
D) SELECT * FROM session.view_name  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) SELECT * FROM global_temp.view_name**

Explanation: Global temporary views are stored in the global_temp database and must be accessed with the global_temp prefix.
</details>

---

### Question 35
**What is the scope of a temporary view?**

A) Database-scoped  
B) Session-scoped  
C) Cluster-scoped  
D) Workspace-scoped  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Session-scoped**

Explanation: Temporary views are available only within the current session and are automatically dropped when the session ends.
</details>

---

### Question 36
**Which table format is recommended for production use in Databricks?**

A) CSV  
B) JSON  
C) Parquet  
D) Delta Lake  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) Delta Lake**

Explanation: Delta Lake is recommended for production because it provides ACID transactions, time travel, schema evolution, and better performance.
</details>

---

### Question 37
**What is the primary benefit of partitioning a table?**

A) Reduces storage costs  
B) Improves query performance through partition pruning  
C) Enables time travel  
D) Provides ACID transactions  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Improves query performance through partition pruning**

Explanation: Partitioning allows queries to skip irrelevant partitions, significantly improving performance when filtering on partition columns.
</details>

---

### Question 38
**Which command shows the structure of a table?**

A) SHOW TABLE  
B) DESCRIBE TABLE  
C) EXPLAIN TABLE  
D) DISPLAY TABLE  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) DESCRIBE TABLE**

Explanation: DESCRIBE TABLE (or DESC TABLE) shows the column names, data types, and comments. DESCRIBE EXTENDED provides additional metadata.
</details>

---

### Question 39
**When should you use an external table instead of a managed table?**

A) When you want Databricks to manage the data lifecycle  
B) When the data is temporary  
C) When the data is shared with other systems or you want to keep it after DROP  
D) When you need better performance  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) When the data is shared with other systems or you want to keep it after DROP**

Explanation: External tables are ideal when data needs to be accessed by multiple systems or when you want to retain the data even after dropping the table.
</details>

---

### Question 40
**What does DESCRIBE DETAIL table_name show?**

A) Only column names and types  
B) Detailed metadata including location, size, and number of files  
C) Table constraints  
D) Table permissions  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Detailed metadata including location, size, and number of files**

Explanation: DESCRIBE DETAIL provides comprehensive metadata about a Delta table, including location, format, number of files, size, partitioning, and more.
</details>

---

## SECTION 6: Spark SQL Basics (Day 6) - Questions 41-50

### Question 41
**What is the difference between INNER JOIN and LEFT JOIN?**

A) INNER JOIN returns all rows from both tables; LEFT JOIN returns only matching rows  
B) INNER JOIN returns only matching rows; LEFT JOIN returns all rows from the left table  
C) They are the same  
D) INNER JOIN is faster than LEFT JOIN  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) INNER JOIN returns only matching rows; LEFT JOIN returns all rows from the left table**

Explanation: INNER JOIN returns only rows where there's a match in both tables. LEFT JOIN returns all rows from the left table and matching rows from the right table, with NULLs for non-matches.
</details>

---

### Question 42
**When should you use HAVING instead of WHERE?**

A) HAVING is used to filter rows before aggregation  
B) HAVING is used to filter groups after aggregation  
C) HAVING and WHERE are interchangeable  
D) HAVING is only used with subqueries  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) HAVING is used to filter groups after aggregation**

Explanation: WHERE filters individual rows before GROUP BY, while HAVING filters groups after aggregation. Use HAVING when filtering based on aggregate functions.
</details>

---

### Question 43
**What does UNION do compared to UNION ALL?**

A) UNION keeps duplicates; UNION ALL removes them  
B) UNION removes duplicates; UNION ALL keeps them  
C) They are the same  
D) UNION is only for numeric columns  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) UNION removes duplicates; UNION ALL keeps them**

Explanation: UNION combines results and removes duplicate rows, while UNION ALL keeps all rows including duplicates. UNION ALL is faster.
</details>

---

### Question 44
**In what order does SQL execute clauses?**

A) SELECT → FROM → WHERE → GROUP BY → ORDER BY  
B) FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY  
C) WHERE → FROM → SELECT → GROUP BY → ORDER BY  
D) FROM → SELECT → WHERE → GROUP BY → ORDER BY  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY**

Explanation: SQL execution order is: FROM (identify tables) → WHERE (filter rows) → GROUP BY (group rows) → HAVING (filter groups) → SELECT (select columns) → DISTINCT → ORDER BY (sort) → LIMIT.
</details>

---

### Question 45
**What is the purpose of a Common Table Expression (CTE)?**

A) To create permanent tables  
B) To create temporary named result sets for use in a query  
C) To replace all subqueries  
D) To improve query performance automatically  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To create temporary named result sets for use in a query**

Explanation: CTEs (WITH clause) create temporary named result sets that exist only for the duration of the query. They improve readability and can be referenced multiple times.
</details>

---

### Question 46
**What does the INTERSECT operator return?**

A) All rows from both queries  
B) Rows that appear in the first query but not the second  
C) Rows that appear in both queries  
D) Rows with NULL values  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Rows that appear in both queries**

Explanation: INTERSECT returns only rows that exist in both query results. It's equivalent to an INNER JOIN but works on entire result sets.
</details>

---

### Question 47
**What is the difference between COUNT(*) and COUNT(column_name)?**

A) They are identical  
B) COUNT(*) counts all rows; COUNT(column_name) counts non-NULL values  
C) COUNT(column_name) is faster  
D) COUNT(*) only works with numeric columns  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) COUNT(*) counts all rows; COUNT(column_name) counts non-NULL values**

Explanation: COUNT(*) counts all rows including those with NULL values. COUNT(column_name) counts only rows where that specific column is not NULL.
</details>

---

### Question 48
**Which clause would you use to get the top 10 highest salaries?**

A) WHERE salary > 10  
B) HAVING COUNT(*) = 10  
C) ORDER BY salary DESC LIMIT 10  
D) GROUP BY salary LIMIT 10  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) ORDER BY salary DESC LIMIT 10**

Explanation: To get the top N records, use ORDER BY to sort in descending order, then LIMIT to restrict the number of results.
</details>

---

### Question 49
**What does a SELF JOIN do?**

A) Joins a table to itself  
B) Joins all tables in the database  
C) Creates a copy of the table  
D) Optimizes join performance  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) Joins a table to itself**

Explanation: A SELF JOIN joins a table to itself, typically used for hierarchical data like employee-manager relationships. You must use different aliases.
</details>

---

### Question 50
**Which operator finds rows in the first query but NOT in the second?**

A) UNION  
B) INTERSECT  
C) EXCEPT  
D) MINUS  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) EXCEPT**

Explanation: EXCEPT (also called MINUS in some databases) returns rows from the first query that don't appear in the second query.
</details>

---

## Scoring Guide

- **45-50 correct**: Outstanding! You've mastered Week 1 content.
- **40-44 correct**: Excellent! You're well-prepared for Week 2.
- **35-39 correct**: Good! Review topics where you struggled.
- **30-34 correct**: Fair. Review Week 1 materials before continuing.
- **Below 30**: Review all Week 1 content thoroughly.

---

## Topic Breakdown

### By Day:
- **Day 1 (Architecture)**: Questions 1-8
- **Day 2 (Clusters)**: Questions 9-16
- **Day 3 (Delta Lake)**: Questions 17-24
- **Day 4 (DBFS)**: Questions 25-30
- **Day 5 (Tables/Views)**: Questions 31-40
- **Day 6 (SQL)**: Questions 41-50

### By Difficulty:
- **Easy**: Questions 1-3, 9-10, 17-18, 25-26, 31-32, 41-42
- **Medium**: Questions 4-7, 11-14, 19-22, 27-29, 33-37, 43-46
- **Hard**: Questions 8, 15-16, 23-24, 30, 38-40, 47-50

---

## Key Concepts Summary

### Must-Know for Exam:
1. ✅ **Managed vs External tables** (DROP behavior)
2. ✅ **Delta Lake Time Travel** (VERSION AS OF, TIMESTAMP AS OF)
3. ✅ **JOIN types** (INNER, LEFT, RIGHT, FULL)
4. ✅ **Cluster types** (All-Purpose vs Job)
5. ✅ **OPTIMIZE and VACUUM** (what they do, when to use)
6. ✅ **WHERE vs HAVING** (row vs group filtering)
7. ✅ **View scopes** (Standard, Temp, Global Temp)
8. ✅ **DBFS paths** (/dbfs/ vs dbfs:/)
9. ✅ **SQL execution order** (FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY)
10. ✅ **Set operations** (UNION, INTERSECT, EXCEPT)

### Common Mistakes to Avoid:
- Confusing managed and external table DROP behavior
- Using wrong JOIN type (INNER when you need LEFT)
- Filtering aggregates with WHERE instead of HAVING
- Forgetting global_temp prefix for global temporary views
- Running VACUUM too soon (before retention period)
- Using All-Purpose clusters for scheduled jobs
- Confusing UNION (removes duplicates) with UNION ALL (keeps duplicates)

---

## Next Steps

### If you scored 40+:
✅ You're ready for Week 2!
- Start Day 8: Advanced SQL - Window Functions
- Continue building on your strong foundation

### If you scored 30-39:
⚠️ Review weak areas before Week 2
- Identify topics where you missed questions
- Re-read those day's README files
- Redo exercises for those topics
- Retake this quiz

### If you scored below 30:
🔄 Review Week 1 thoroughly
- Re-read all Week 1 README files
- Complete all exercises again
- Take individual day quizzes
- Retake this comprehensive quiz
- Consider spending an extra day on review

---

## Week 2 Preview

**Days 8-15: ELT with Spark SQL & Python**
- Day 8: Advanced SQL - Window Functions
- Day 9: Advanced SQL - Higher-Order Functions
- Day 10: Python DataFrames Basics
- Day 11: Python DataFrames Advanced
- Day 12: Data Ingestion Patterns
- Day 13: Data Transformation Patterns
- Day 14: ELT Best Practices
- Day 15: Week 2 Review & ELT Project

**What to Expect**:
- More advanced SQL techniques
- Python DataFrame API
- Real-world data engineering patterns
- Building complete ELT pipelines

---

**Congratulations on completing Week 1! 🎉**

You've built a solid foundation in the Databricks Lakehouse Platform. Take a moment to celebrate your progress, then get ready for Week 2 where we'll dive deeper into advanced SQL and Python for data engineering!

