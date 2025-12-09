# Day 21: Optimization Techniques - Quiz

## Instructions
- Answer all 15 questions
- Each question has one correct answer unless stated otherwise
- Check your answers at the end

---

## Questions

### Question 1
What is the primary purpose of the OPTIMIZE command?

A) To delete old data  
B) To compact small files into larger files  
C) To create partitions  
D) To backup the table  

### Question 2
What is the default retention period for VACUUM?

A) 24 hours  
B) 7 days (168 hours)  
C) 30 days  
D) 90 days  

### Question 3
What is the recommended target file size for Delta tables?

A) 1-10 MB  
B) 10-50 MB  
C) 128 MB - 1 GB  
D) 5-10 GB  

### Question 4
What is the maximum recommended number of columns for Z-ordering?

A) 2  
B) 4  
C) 8  
D) Unlimited  

### Question 5
What does Z-ordering do?

A) Sorts data alphabetically  
B) Co-locates related data for better data skipping  
C) Creates indexes  
D) Compresses data  

### Question 6
When should you partition a table?

A) Always, for every table  
B) When you have high-cardinality columns frequently used in filters  
C) Only for small tables  
D) Never, partitioning is deprecated  

### Question 7
What happens when you run VACUUM?

A) Compacts small files  
B) Deletes old file versions no longer referenced  
C) Creates backups  
D) Optimizes queries  

### Question 8
Can you time travel to a version that has been VACUUMed?

A) Yes, always  
B) No, VACUUMed files are permanently deleted  
C) Only with special permissions  
D) Yes, but only for 24 hours  

### Question 9
What is partition pruning?

A) Deleting old partitions  
B) Skipping irrelevant partitions during query execution  
C) Creating new partitions  
D) Merging partitions  

### Question 10
What is data skipping?

A) Ignoring NULL values  
B) Using statistics to skip files that can't contain matching data  
C) Skipping validation  
D) Bypassing security checks  

### Question 11
Which command should you run before VACUUM in production?

A) OPTIMIZE  
B) VACUUM DRY RUN  
C) DELETE  
D) BACKUP  

### Question 12
What is the "small file problem"?

A) Files smaller than 1 KB  
B) Too many small files causing performance issues  
C) Missing files  
D) Corrupted files  

### Question 13
When should you Z-order a table?

A) After every write  
B) On low-cardinality columns  
C) On high-cardinality columns frequently used in filters  
D) Never  

### Question 14
What is the relationship between partitioning and Z-ordering?

A) They are mutually exclusive  
B) They can be used together (partition by one set of columns, Z-order by others)  
C) Z-ordering replaces partitioning  
D) They are the same thing  

### Question 15
What does DESCRIBE DETAIL show?

A) Only table schema  
B) Table metadata including file count, size, and partitions  
C) Only partition information  
D) Query history  

---

## Answer Key

<details>
<summary>Click to reveal answers</summary>

### Answer 1
**B) To compact small files into larger files**

Explanation: OPTIMIZE compacts small files into larger files, improving read performance and reducing metadata overhead. It also removes deleted rows.

### Answer 2
**B) 7 days (168 hours)**

Explanation: The default VACUUM retention period is 7 days (168 hours). This allows time travel for the past week and protects against concurrent readers.

### Answer 3
**C) 128 MB - 1 GB**

Explanation: The recommended target file size is 128 MB to 1 GB. This balances parallelism with metadata overhead and I/O efficiency.

### Answer 4
**B) 4**

Explanation: The maximum recommended number of columns for Z-ordering is 4. Beyond this, the benefits diminish due to the multi-dimensional nature of Z-ordering.

### Answer 5
**B) Co-locates related data for better data skipping**

Explanation: Z-ordering uses a Z-order curve to co-locate related data in the same files, enabling better data skipping when querying specific values.

### Answer 6
**B) When you have high-cardinality columns frequently used in filters**

Explanation: Partition when you have columns with reasonable cardinality (like date, region) that are frequently used in WHERE clauses, enabling partition pruning.

### Answer 7
**B) Deletes old file versions no longer referenced**

Explanation: VACUUM permanently deletes old data files that are no longer referenced by the table, freeing up storage space.

### Answer 8
**B) No, VACUUMed files are permanently deleted**

Explanation: Once files are VACUUMed, they are permanently deleted and you cannot time travel to those versions. This is why VACUUM retention is important.

### Answer 9
**B) Skipping irrelevant partitions during query execution**

Explanation: Partition pruning is when the query optimizer skips scanning partitions that can't contain matching data based on the WHERE clause.

### Answer 10
**B) Using statistics to skip files that can't contain matching data**

Explanation: Data skipping uses min/max statistics collected on files to skip reading files that can't possibly contain matching data for a query.

### Answer 11
**B) VACUUM DRY RUN**

Explanation: Always run VACUUM DRY RUN first in production to see what files would be deleted before actually running VACUUM.

### Answer 12
**B) Too many small files causing performance issues**

Explanation: The small file problem occurs when a table has too many small files (< 128 MB), causing high metadata overhead and slow query performance.

### Answer 13
**C) On high-cardinality columns frequently used in filters**

Explanation: Z-order tables on high-cardinality columns that are frequently used in WHERE clauses or JOIN conditions to maximize data skipping benefits.

### Answer 14
**B) They can be used together (partition by one set of columns, Z-order by others)**

Explanation: Partitioning and Z-ordering are complementary. Typically partition by date/region and Z-order by high-cardinality columns like customer_id.

### Answer 15
**B) Table metadata including file count, size, and partitions**

Explanation: DESCRIBE DETAIL shows comprehensive table metadata including number of files, total size, partition columns, and other important statistics.

</details>

---

## Scoring Guide

- **13-15 correct**: Excellent! You understand optimization techniques well
- **10-12 correct**: Good! Review the concepts you missed
- **7-9 correct**: Fair. Practice more with optimization commands
- **Below 7**: Review the README.md and complete more exercises

---

## Key Concepts to Remember

1. **OPTIMIZE** compacts small files (target: 128 MB - 1 GB)
2. **VACUUM** deletes old files (default: 7 days retention)
3. **Z-ordering** co-locates data (max 4 columns)
4. **Partitioning** enables partition pruning
5. **Data skipping** uses statistics to skip files
6. **Small file problem** = too many files < 128 MB
7. **VACUUM DRY RUN** before actual VACUUM
8. **Z-order** high-cardinality filter columns
9. **Partition** by date/region, Z-order by ID columns
10. **DESCRIBE DETAIL** shows table statistics

---

**Next**: Day 22 - Week 3 Review & Incremental Project
