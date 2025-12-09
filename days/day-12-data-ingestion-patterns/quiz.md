# Day 12 Quiz: Data Ingestion Patterns

## Instructions
- 15 multiple choice questions
- Each question has one correct answer
- Answers are at the bottom
- Try to answer without looking at the answers first!

---

## Questions

### Question 1
Which command is used for batch data ingestion in Databricks with built-in idempotency?

A) LOAD DATA  
B) COPY INTO  
C) INSERT INTO  
D) BULK INSERT

### Question 2
What is the format name used for Auto Loader in Spark?

A) `autoLoader`  
B) `cloudFiles`  
C) `streamingFiles`  
D) `deltaStream`

### Question 3
How does COPY INTO ensure idempotency?

A) By using a unique transaction ID  
B) By tracking loaded files in the transaction log  
C) By comparing file checksums  
D) By maintaining a separate tracking table

### Question 4
Which option is required for Auto Loader to maintain exactly-once processing semantics?

A) `schemaLocation`  
B) `checkpointLocation`  
C) `triggerLocation`  
D) `stateLocation`

### Question 5
What is the default schema evolution mode in Auto Loader?

A) `failOnNewColumns`  
B) `addNewColumns`  
C) `rescue`  
D) `none`

### Question 6
Which trigger option processes all available files once and then stops?

A) `trigger(once=True)`  
B) `trigger(availableNow=True)`  
C) `trigger(continuous=True)`  
D) Both A and B

### Question 7
How do you enable schema merging in COPY INTO?

A) `FORMAT_OPTIONS ('mergeSchema' = 'true')`  
B) `COPY_OPTIONS ('mergeSchema' = 'true')`  
C) `SCHEMA_OPTIONS ('merge' = 'true')`  
D) Schema merging is automatic

### Question 8
What happens if you run COPY INTO twice with the same source files?

A) Duplicate records are created  
B) An error is thrown  
C) Files are skipped (no duplicates)  
D) The table is overwritten

### Question 9
Which option limits the number of files processed per trigger in Auto Loader?

A) `maxFiles`  
B) `filesPerTrigger`  
C) `maxFilesPerTrigger`  
D) `fileLimit`

### Question 10
What is the purpose of `cloudFiles.schemaLocation` in Auto Loader?

A) To store the source files  
B) To store inferred schema and track schema evolution  
C) To store checkpoint data  
D) To store processed files list

### Question 11
Which COPY INTO option forces reloading of already-loaded files?

A) `reload = 'true'`  
B) `force = 'true'`  
C) `overwrite = 'true'`  
D) `reprocess = 'true'`

### Question 12
When should you use Auto Loader instead of COPY INTO?

A) For small, scheduled batch loads  
B) For SQL-only workflows  
C) For continuous, large-scale ingestion  
D) For one-time data loads

### Question 13
How do you specify schema hints in Auto Loader?

A) `.option("schemaHints", "id INT, amount DOUBLE")`  
B) `.option("cloudFiles.schemaHints", "id INT, amount DOUBLE")`  
C) `.option("hints", "id INT, amount DOUBLE")`  
D) `.schema("id INT, amount DOUBLE")`

### Question 14
What does the `rescue` schema evolution mode do?

A) Fails the stream on schema changes  
B) Adds new columns automatically  
C) Saves unparseable data to _rescued_data column  
D) Ignores schema changes

### Question 15
Which file formats are supported by both COPY INTO and Auto Loader?

A) Only CSV and JSON  
B) CSV, JSON, and Parquet  
C) CSV, JSON, Parquet, and Avro  
D) All file formats

---

## Answers

### Answer 1
**B) COPY INTO**

Explanation: COPY INTO is the SQL command for batch data ingestion in Databricks. It automatically tracks loaded files to ensure idempotency, meaning you can safely re-run it without creating duplicates.

### Answer 2
**B) `cloudFiles`**

Explanation: Auto Loader uses the format name "cloudFiles" in Spark readStream operations. Example: `.format("cloudFiles")`.

### Answer 3
**B) By tracking loaded files in the transaction log**

Explanation: COPY INTO tracks which files have been loaded in the Delta table's transaction log. It uses file path and modification time to identify already-loaded files and skips them automatically.

### Answer 4
**B) `checkpointLocation`**

Explanation: The checkpoint location is required for Auto Loader (and all structured streaming queries) to maintain state and ensure exactly-once processing. It tracks which files have been processed.

### Answer 5
**B) `addNewColumns`**

Explanation: The default schema evolution mode in Auto Loader is "addNewColumns", which automatically adds new columns when detected in the source data.

### Answer 6
**D) Both A and B**

Explanation: Both `trigger(once=True)` and `trigger(availableNow=True)` process available files and then stop. However, `availableNow=True` is recommended as it's more efficient for large backlogs.

### Answer 7
**B) `COPY_OPTIONS ('mergeSchema' = 'true')`**

Explanation: Schema merging in COPY INTO is enabled using `COPY_OPTIONS ('mergeSchema' = 'true')`, not FORMAT_OPTIONS.

### Answer 8
**C) Files are skipped (no duplicates)**

Explanation: COPY INTO is idempotent by default. If you run it twice with the same files, the already-loaded files are skipped, preventing duplicates.

### Answer 9
**C) `maxFilesPerTrigger`**

Explanation: Use `.option("cloudFiles.maxFilesPerTrigger", "100")` to limit the number of files processed in each trigger, which helps control resource usage.

### Answer 10
**B) To store inferred schema and track schema evolution**

Explanation: The schema location stores the inferred schema and tracks schema changes over time. It's separate from the checkpoint location.

### Answer 11
**B) `force = 'true'`**

Explanation: Use `COPY_OPTIONS ('force' = 'true')` to force reloading of files that have already been loaded. Use with caution as it can create duplicates.

### Answer 12
**C) For continuous, large-scale ingestion**

Explanation: Auto Loader is ideal for continuous, large-scale ingestion with automatic schema evolution. COPY INTO is better for scheduled batch loads.

### Answer 13
**B) `.option("cloudFiles.schemaHints", "id INT, amount DOUBLE")`**

Explanation: Schema hints in Auto Loader use the "cloudFiles.schemaHints" option to specify data types for specific columns.

### Answer 14
**C) Saves unparseable data to _rescued_data column**

Explanation: The "rescue" mode saves data that doesn't match the schema to a special `_rescued_data` column, allowing you to handle it separately.

### Answer 15
**C) CSV, JSON, Parquet, and Avro**

Explanation: Both COPY INTO and Auto Loader support CSV, JSON, Parquet, and Avro formats. They also support other formats with appropriate configurations.

---

## Scoring

- 13-15 correct: Excellent! You have mastered data ingestion patterns.
- 10-12 correct: Good job! Review the topics you missed.
- 7-9 correct: Fair. Practice more with COPY INTO and Auto Loader.
- Below 7: Review the Day 12 materials and practice the exercises.

---

## Key Concepts to Remember

1. **COPY INTO**: SQL-based batch ingestion with built-in idempotency
2. **Auto Loader**: Streaming ingestion using cloudFiles format
3. **Idempotency**: COPY INTO tracks files, Auto Loader uses checkpoints
4. **Schema Evolution**: Auto Loader supports automatic schema evolution
5. **Checkpoint Location**: Required for Auto Loader, stores stream state
6. **Schema Location**: Stores inferred schema for Auto Loader
7. **Trigger Options**: once, availableNow, processingTime, continuous
8. **Format Options**: Specify file format details (header, delimiter, etc.)
9. **Copy Options**: Control COPY INTO behavior (mergeSchema, force)
10. **Use Cases**: COPY INTO for batch, Auto Loader for streaming
