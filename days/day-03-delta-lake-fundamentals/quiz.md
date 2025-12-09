# Day 3 Quiz: Delta Lake Fundamentals

Test your knowledge of Delta Lake fundamentals, ACID transactions, and time travel.

---

## Question 1
**What is Delta Lake built on top of?**

A) CSV files  
B) JSON files  
C) Parquet files  
D) Avro files  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Parquet files**

Explanation: Delta Lake is built on top of Parquet files and adds a transaction log layer to provide ACID transactions, time travel, and other features. The data is stored in Parquet format for efficient columnar storage.
</details>

---

## Question 2
**Which SQL syntax is used to query a Delta table as it existed at version 5?**

A) SELECT * FROM table AT VERSION 5  
B) SELECT * FROM table VERSION AS OF 5  
C) SELECT * FROM table WHERE version = 5  
D) SELECT * FROM table USING VERSION 5  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) SELECT * FROM table VERSION AS OF 5**

Explanation: The correct syntax for time travel by version is `VERSION AS OF`. You can also use `TIMESTAMP AS OF` for time-based queries.
</details>

---

## Question 3
**What is the default retention period for VACUUM in Delta Lake?**

A) 24 hours  
B) 7 days (168 hours)  
C) 30 days  
D) 90 days  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) 7 days (168 hours)**

Explanation: The default retention period for VACUUM is 7 days (168 hours). Files older than this retention period are permanently deleted. You cannot time travel beyond the VACUUM retention period.
</details>

---

## Question 4
**What happens to the data files when you DROP a managed Delta table?**

A) Only metadata is deleted, data files remain  
B) Both metadata and data files are deleted  
C) Data files are moved to a recycle bin  
D) Data files are archived  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Both metadata and data files are deleted**

Explanation: For managed tables, Databricks manages both metadata and data. When you DROP a managed table, both are deleted. For external tables, only metadata is deleted and data files remain.
</details>

---

## Question 5
**Which command is used to compact small files in a Delta table?**

A) COMPACT table_name  
B) OPTIMIZE table_name  
C) MERGE table_name  
D) CONSOLIDATE table_name  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) OPTIMIZE table_name**

Explanation: The OPTIMIZE command compacts small files into larger files, improving query performance. It can also be combined with Z-ORDER for data co-location.
</details>

---

## Question 6
**What does the "A" in ACID stand for?**

A) Automatic  
B) Atomicity  
C) Asynchronous  
D) Availability  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Atomicity**

Explanation: ACID stands for Atomicity, Consistency, Isolation, and Durability. Atomicity means transactions either complete fully or not at all (all-or-nothing).
</details>

---

## Question 7
**Where is the Delta Lake transaction log stored?**

A) In a separate database  
B) In the `_delta_log/` directory within the table location  
C) In the Databricks metastore  
D) In a cloud storage bucket  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) In the `_delta_log/` directory within the table location**

Explanation: The transaction log is stored in a `_delta_log/` subdirectory within the table's location. It contains JSON files that record all changes to the table.
</details>

---

## Question 8
**What is the maximum recommended number of columns for Z-ordering?**

A) 2 columns  
B) 4 columns  
C) 8 columns  
D) Unlimited  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) 4 columns**

Explanation: While Z-ordering can technically use more columns, Databricks recommends a maximum of 4 columns for optimal performance. Beyond 4 columns, the benefits diminish significantly.
</details>

---

## Question 9
**Which operation allows you to perform both INSERT and UPDATE in a single statement?**

A) UPSERT  
B) MERGE  
C) INSERT OR UPDATE  
D) REPLACE  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) MERGE**

Explanation: The MERGE statement (also known as UPSERT) allows you to perform INSERT, UPDATE, and DELETE operations in a single atomic transaction based on matching conditions.
</details>

---

## Question 10
**How do you enable automatic schema merging when writing to a Delta table?**

A) SET mergeSchema = true  
B) .option("mergeSchema", "true")  
C) ALTER TABLE SET mergeSchema  
D) CREATE TABLE WITH mergeSchema  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) .option("mergeSchema", "true")**

Explanation: When writing with PySpark, use `.option("mergeSchema", "true")` to automatically merge schemas. This allows adding new columns without explicitly altering the table schema.
</details>

---

## Question 11
**What happens when you run VACUUM on a Delta table?**

A) It compacts small files  
B) It removes old data files no longer referenced by the transaction log  
C) It updates table statistics  
D) It rebuilds the transaction log  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) It removes old data files no longer referenced by the transaction log**

Explanation: VACUUM permanently deletes data files that are no longer referenced by the transaction log and are older than the retention period. This frees up storage but prevents time travel beyond the retention period.
</details>

---

## Question 12
**Which statement about Delta Lake time travel is TRUE?**

A) Time travel is only available for the last 24 hours  
B) Time travel requires additional storage for each version  
C) Time travel works by reading the transaction log and appropriate data files  
D) Time travel creates copies of data for each version  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Time travel works by reading the transaction log and appropriate data files**

Explanation: Time travel doesn't create copies of data. It works by reading the transaction log to determine which data files were valid at a specific version or timestamp, then reading those files.
</details>

---

## Question 13
**What is the primary purpose of the Delta Lake transaction log?**

A) To store compressed data  
B) To record all changes and enable ACID properties  
C) To cache query results  
D) To store user permissions  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To record all changes and enable ACID properties**

Explanation: The transaction log records all changes to the table (adds, removes, updates) and is the foundation for ACID properties, time travel, and concurrent reads/writes.
</details>

---

## Question 14
**Which command would you use to revert a Delta table to a previous version?**

A) ROLLBACK TABLE table_name TO VERSION 5  
B) RESTORE TABLE table_name TO VERSION AS OF 5  
C) REVERT TABLE table_name VERSION 5  
D) UNDO TABLE table_name TO VERSION 5  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) RESTORE TABLE table_name TO VERSION AS OF 5**

Explanation: The RESTORE TABLE command reverts a table to a previous version. You can use VERSION AS OF (version number) or TIMESTAMP AS OF (timestamp).
</details>

---

## Question 15
**What is the main difference between a shallow clone and a deep clone in Delta Lake?**

A) Shallow clone is faster but less reliable  
B) Shallow clone shares data files, deep clone copies all data files  
C) Shallow clone only copies metadata, deep clone copies everything  
D) There is no difference  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Shallow clone shares data files, deep clone copies all data files**

Explanation: A shallow clone creates a new table that shares the data files with the source table (only copies metadata), making it fast and space-efficient. A deep clone copies all data files, creating a fully independent copy.
</details>

---

## Scoring Guide

- **13-15 correct (87-100%)**: Excellent! You've mastered Delta Lake fundamentals.
- **11-12 correct (73-80%)**: Good job! Minor review recommended.
- **9-10 correct (60-67%)**: Fair. Review the material again.
- **Below 9 (< 60%)**: Re-read Day 3 content thoroughly.

---

## Key Concepts to Remember

1. **Delta Lake = Parquet + Transaction Log + ACID**
2. **Time Travel**: VERSION AS OF (version) or TIMESTAMP AS OF (timestamp)
3. **VACUUM**: Default 7 days retention, permanently deletes old files
4. **OPTIMIZE**: Compacts small files, use with Z-ORDER for co-location
5. **MERGE**: Upsert operation (INSERT + UPDATE in one statement)
6. **Transaction Log**: `_delta_log/` directory, records all changes
7. **ACID**: Atomicity, Consistency, Isolation, Durability
8. **Managed vs External**: DROP behavior differs (both vs metadata only)
9. **Schema Evolution**: mergeSchema option or ALTER TABLE
10. **RESTORE**: Revert table to previous version

---

## Common Mistakes to Avoid

1. Confusing VERSION AS OF with TIMESTAMP AS OF syntax
2. Forgetting VACUUM retention period (default 7 days)
3. Not understanding managed vs external table DROP behavior
4. Thinking time travel creates data copies (it doesn't)
5. Using too many columns in Z-ORDER (max 4 recommended)
6. Running VACUUM too aggressively (loses time travel ability)
7. Forgetting that OPTIMIZE is needed after many small writes

---

## Next Steps

- Review any questions you got wrong
- Re-read relevant sections in Day 3 README
- Practice time travel queries with different versions
- Experiment with OPTIMIZE and VACUUM
- Move on to Day 4: Databricks File System (DBFS)

Good luck! 🚀
