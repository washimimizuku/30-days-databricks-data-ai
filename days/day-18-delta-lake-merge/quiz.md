# Day 18: Delta Lake MERGE (UPSERT) - Quiz

## Instructions
- Answer all 15 questions
- Each question has one correct answer unless stated otherwise
- Check your answers at the end

---

## Questions

### Question 1
What is the primary purpose of the MERGE operation in Delta Lake?

A) To delete all records from a table  
B) To combine INSERT, UPDATE, and DELETE operations in a single atomic transaction  
C) To create a new table from an existing table  
D) To optimize table performance  

### Question 2
Which of the following is a requirement for the target table in a MERGE operation?

A) It must be a Parquet table  
B) It must be a Delta table  
C) It must be partitioned  
D) It must have a primary key defined  

### Question 3
What does UPSERT stand for?

A) Update and Insert  
B) Unique Insert  
C) Update or Insert  
D) Universal Insert  

### Question 4
In a MERGE statement, what does the WHEN MATCHED clause handle?

A) Records that exist only in the source  
B) Records that exist only in the target  
C) Records that exist in both source and target  
D) Records that should be deleted  

### Question 5
Which clause would you use to insert records that exist in the source but not in the target?

A) WHEN MATCHED  
B) WHEN NOT MATCHED  
C) WHEN NOT MATCHED BY SOURCE  
D) WHEN DELETED  

### Question 6
What happens when you have multiple WHEN MATCHED clauses in a MERGE statement?

A) All matching clauses are executed  
B) The last matching clause is executed  
C) The first matching clause is executed  
D) An error is thrown  

### Question 7
Which Python API is used to perform MERGE operations in PySpark?

A) DataFrame.merge()  
B) DeltaTable.merge()  
C) spark.merge()  
D) Table.upsert()  

### Question 8
What is the purpose of the WHEN NOT MATCHED BY SOURCE clause?

A) To insert new records from source  
B) To update records that don't exist in source  
C) To handle records that exist in target but not in source  
D) To delete all records  

### Question 9
Before performing a MERGE, what should you do if your source data contains duplicates?

A) Nothing, MERGE handles duplicates automatically  
B) Deduplicate the source data first  
C) Use a different merge key  
D) Disable duplicate checking  

### Question 10
Which of the following is a best practice for MERGE performance?

A) Always scan all partitions  
B) Use partition filters in the merge condition  
C) Avoid using indexes  
D) Disable optimization  

### Question 11
What SQL command can you use to view the history of MERGE operations on a Delta table?

A) SHOW MERGE HISTORY  
B) DESCRIBE HISTORY table_name  
C) SELECT * FROM table_history  
D) SHOW TABLE OPERATIONS  

### Question 12
In a conditional MERGE, how do you update only if the source version is greater than the target version?

A) WHEN MATCHED AND source.version > target.version THEN UPDATE  
B) WHEN MATCHED WHERE source.version > target.version THEN UPDATE  
C) WHEN source.version > target.version MATCHED THEN UPDATE  
D) IF source.version > target.version WHEN MATCHED THEN UPDATE  

### Question 13
What is a "soft delete" in the context of MERGE operations?

A) Permanently deleting records  
B) Marking records as inactive instead of physically removing them  
C) Deleting records temporarily  
D) Deleting only some columns  

### Question 14
Which configuration enables automatic schema evolution during MERGE?

A) spark.databricks.delta.merge.enabled  
B) spark.databricks.delta.schema.autoMerge.enabled  
C) spark.sql.merge.schemaEvolution  
D) spark.delta.autoSchema  

### Question 15
What is the recommended approach for using MERGE in a streaming context?

A) Use MERGE directly in writeStream  
B) Use foreachBatch with MERGE  
C) MERGE cannot be used with streaming  
D) Use append mode only  

---

## Answer Key

<details>
<summary>Click to reveal answers</summary>

### Answer 1
**B) To combine INSERT, UPDATE, and DELETE operations in a single atomic transaction**

Explanation: MERGE is designed to perform multiple DML operations (INSERT, UPDATE, DELETE) in a single atomic transaction, making it ideal for UPSERT scenarios.

### Answer 2
**B) It must be a Delta table**

Explanation: The target table in a MERGE operation must be a Delta table. The source can be any table, view, or DataFrame, but the target must be Delta to support ACID transactions.

### Answer 3
**C) Update or Insert**

Explanation: UPSERT is a combination of UPDATE and INSERT - it updates existing records and inserts new ones in a single operation.

### Answer 4
**C) Records that exist in both source and target**

Explanation: WHEN MATCHED handles records that exist in both the source and target tables based on the merge condition.

### Answer 5
**B) WHEN NOT MATCHED**

Explanation: WHEN NOT MATCHED handles records that exist in the source but not in the target, typically used for INSERT operations.

### Answer 6
**C) The first matching clause is executed**

Explanation: When multiple WHEN MATCHED clauses are present, the first one that matches is executed. Order matters!

### Answer 7
**B) DeltaTable.merge()**

Explanation: In PySpark, you use the DeltaTable API's merge() method to perform MERGE operations: `DeltaTable.forName(spark, "table").merge(source, condition)`.

### Answer 8
**C) To handle records that exist in target but not in source**

Explanation: WHEN NOT MATCHED BY SOURCE handles records that exist in the target table but not in the source, often used for archiving or soft deletes.

### Answer 9
**B) Deduplicate the source data first**

Explanation: Source data should be deduplicated before MERGE to avoid ambiguous updates. Use window functions or groupBy to deduplicate.

### Answer 10
**B) Use partition filters in the merge condition**

Explanation: Including partition columns in the merge condition enables partition pruning, significantly improving MERGE performance by scanning only relevant partitions.

### Answer 11
**B) DESCRIBE HISTORY table_name**

Explanation: `DESCRIBE HISTORY table_name` shows all operations on a Delta table, including MERGE operations with their metrics.

### Answer 12
**A) WHEN MATCHED AND source.version > target.version THEN UPDATE**

Explanation: Conditional MERGE uses AND to add conditions to WHEN clauses: `WHEN MATCHED AND condition THEN action`.

### Answer 13
**B) Marking records as inactive instead of physically removing them**

Explanation: Soft delete marks records as deleted (e.g., is_active = false) rather than physically removing them, preserving data for audit trails.

### Answer 14
**B) spark.databricks.delta.schema.autoMerge.enabled**

Explanation: Setting `spark.databricks.delta.schema.autoMerge.enabled` to true allows MERGE to automatically add new columns from the source.

### Answer 15
**B) Use foreachBatch with MERGE**

Explanation: In streaming, use foreachBatch to apply MERGE to each micro-batch: `writeStream.foreachBatch(merge_function)`.

</details>

---

## Scoring Guide

- **13-15 correct**: Excellent! You have a strong understanding of MERGE operations
- **10-12 correct**: Good! Review the topics you missed
- **7-9 correct**: Fair. Spend more time practicing MERGE operations
- **Below 7**: Review the README.md and practice more exercises

---

## Key Concepts to Remember

1. **MERGE Syntax**: MERGE INTO target USING source ON condition WHEN MATCHED/NOT MATCHED
2. **Target Must Be Delta**: Only Delta tables can be MERGE targets
3. **UPSERT Pattern**: Update if exists, insert if not
4. **Deduplication**: Always deduplicate source before MERGE
5. **Conditional Logic**: Use AND to add conditions to WHEN clauses
6. **Order Matters**: First matching WHEN clause wins
7. **Performance**: Use partition filters for better performance
8. **Schema Evolution**: Enable with spark.databricks.delta.schema.autoMerge.enabled
9. **Streaming**: Use foreachBatch for streaming MERGE
10. **Monitoring**: Use DESCRIBE HISTORY to track MERGE operations

---

**Next**: Day 19 - Change Data Capture (CDC)
