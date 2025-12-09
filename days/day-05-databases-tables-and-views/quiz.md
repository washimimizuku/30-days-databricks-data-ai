# Day 5 Quiz: Databases, Tables, and Views

Test your knowledge of databases, tables, and views in Databricks.

---

## Question 1
**What happens to the data files when you DROP a managed table?**

A) Data files are moved to a trash folder  
B) Data files are deleted permanently  
C) Data files remain in their location  
D) Data files are archived  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Data files are deleted permanently**

Explanation: When you DROP a managed table, Databricks deletes both the metadata and the data files. This is a key difference from external tables.
</details>

---

## Question 2
**What happens to the data files when you DROP an external table?**

A) Data files are deleted permanently  
B) Data files are moved to a trash folder  
C) Data files remain in their location  
D) Data files are converted to Parquet format  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Data files remain in their location**

Explanation: When you DROP an external table, only the metadata is deleted. The data files remain in the specified location, allowing you to recreate the table or use the data elsewhere.
</details>

---

## Question 3
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

## Question 4
**Which SQL command creates a table from a query result?**

A) CREATE TABLE AS SELECT (CTAS)  
B) INSERT INTO SELECT  
C) SELECT INTO  
D) COPY TABLE  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) CREATE TABLE AS SELECT (CTAS)**

Explanation: CTAS (Create Table As Select) creates a new table and populates it with the results of a SELECT query in one operation.
</details>

---

## Question 5
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

## Question 6
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

## Question 7
**Which table format is recommended for production use in Databricks?**

A) CSV  
B) JSON  
C) Parquet  
D) Delta Lake  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) Delta Lake**

Explanation: Delta Lake is recommended for production because it provides ACID transactions, time travel, schema evolution, and better performance compared to other formats.
</details>

---

## Question 8
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

## Question 9
**Which command shows the structure of a table?**

A) SHOW TABLE  
B) DESCRIBE TABLE  
C) EXPLAIN TABLE  
D) DISPLAY TABLE  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) DESCRIBE TABLE**

Explanation: DESCRIBE TABLE (or DESC TABLE) shows the column names, data types, and comments for a table. DESCRIBE EXTENDED provides additional metadata.
</details>

---

## Question 10
**What is a database also called in Databricks?**

A) Catalog  
B) Schema  
C) Namespace  
D) Collection  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Schema**

Explanation: In Databricks, the terms "database" and "schema" are interchangeable. SHOW DATABASES and SHOW SCHEMAS return the same results.
</details>

---

## Question 11
**Which type of view persists after the session ends?**

A) Temporary view  
B) Global temporary view  
C) Standard view  
D) Session view  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Standard view**

Explanation: Standard views are persistent and stored in the database. They remain available across sessions and clusters until explicitly dropped.
</details>

---

## Question 12
**What does the PARTITIONED BY clause do?**

A) Splits data into multiple tables  
B) Organizes data into subdirectories based on column values  
C) Creates multiple copies of the data  
D) Compresses data by partition  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Organizes data into subdirectories based on column values**

Explanation: PARTITIONED BY creates a directory structure where data is organized into subdirectories based on the partition column values, enabling partition pruning.
</details>

---

## Question 13
**Which command adds a new column to an existing table?**

A) ALTER TABLE table_name ADD COLUMN column_name type  
B) UPDATE TABLE table_name ADD column_name type  
C) MODIFY TABLE table_name ADD column_name type  
D) INSERT COLUMN column_name type INTO table_name  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) ALTER TABLE table_name ADD COLUMN column_name type**

Explanation: ALTER TABLE with ADD COLUMN is the correct syntax to add a new column to an existing table.
</details>

---

## Question 14
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

## Question 15
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

## Scoring Guide

- **13-15 correct**: Excellent! You have a strong understanding of databases, tables, and views.
- **10-12 correct**: Good job! Review the questions you missed.
- **7-9 correct**: Fair. Review the Day 5 materials and try again.
- **Below 7**: Review the Day 5 README and setup materials thoroughly.

---

## Key Concepts to Remember

1. **Managed Tables**: Databricks manages data + metadata; DROP deletes both
2. **External Tables**: User manages data, Databricks manages metadata; DROP keeps data
3. **Default Location**: /user/hive/warehouse/ for managed tables
4. **View Types**: Standard (persistent), Temp (session), Global Temp (cluster)
5. **Global Temp Access**: Use global_temp.view_name
6. **Delta Lake**: Recommended format for production (ACID, time travel)
7. **Partitioning**: Improves performance through partition pruning
8. **CTAS**: CREATE TABLE AS SELECT creates and populates table
9. **DESCRIBE**: Shows table structure and metadata
10. **ALTER TABLE**: Modifies table structure (add/rename columns)

---

## Common Exam Scenarios

### Scenario 1: Table Type Selection
**Question**: You need to create a table for customer data that will be accessed by both Databricks and an external reporting tool. The data should persist even if the table is dropped. What type of table should you create?

**Answer**: External table with a specified LOCATION. This ensures the data remains accessible to other systems and persists after DROP TABLE.

### Scenario 2: View Scope
**Question**: You create a temporary view in Notebook A. Can you access it from Notebook B on the same cluster?

**Answer**: No. Temporary views are session-scoped and only available in the notebook where they were created. Use a global temporary view (global_temp) for cluster-wide access.

### Scenario 3: Partition Strategy
**Question**: You have a 5TB sales table with queries that frequently filter by date. What optimization should you apply?

**Answer**: Partition the table by date using PARTITIONED BY (sale_date). This enables partition pruning, allowing queries to skip irrelevant partitions.

### Scenario 4: Table Cleanup
**Question**: You need to remove a table but keep the underlying data files for backup. What should you do?

**Answer**: Use an external table. When you DROP an external table, only the metadata is deleted, and the data files remain in their location.

---

## Practice Questions

### Question A
What command would you use to see all partitions in a table named 'sales'?

<details>
<summary>Click to reveal answer</summary>

**Answer**: `SHOW PARTITIONS sales;`

This command lists all partitions in the specified table.
</details>

### Question B
How do you create a database with a custom location?

<details>
<summary>Click to reveal answer</summary>

**Answer**: 
```sql
CREATE DATABASE my_database
LOCATION '/custom/path/';
```

This creates a database where tables will be stored in the specified location.
</details>

### Question C
What's the difference between DESCRIBE TABLE and DESCRIBE DETAIL?

<details>
<summary>Click to reveal answer</summary>

**Answer**: 
- `DESCRIBE TABLE`: Shows column names, types, and comments
- `DESCRIBE DETAIL`: Shows comprehensive metadata (location, size, files, format, partitions, etc.)

DESCRIBE DETAIL is specific to Delta tables and provides much more information.
</details>

---

## Next Steps

- Review any questions you got wrong
- Re-read the relevant sections in the Day 5 README
- Practice creating managed and external tables
- Experiment with different view types
- Try partitioning strategies
- Move on to Day 6: Spark SQL Basics

Good luck! 🚀
