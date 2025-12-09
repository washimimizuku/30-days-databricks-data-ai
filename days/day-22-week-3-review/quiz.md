# Day 22 Quiz: Week 3 Review - Incremental Data Processing

Test your comprehensive knowledge of Week 3 topics (Days 16-21).

**Instructions**: This quiz contains 50 questions covering all Week 3 concepts. Take your time and review the explanations for any questions you miss.

---

## Structured Streaming Basics (Questions 1-10)

## Question 1
**What is the default output mode for structured streaming?**

A) complete  
B) update  
C) append  
D) overwrite  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) append**

Explanation: The default output mode is "append", which only writes new rows to the sink. This is the most common mode for streaming applications.
</details>

---

## Question 2
**Which of the following is required for fault-tolerant streaming?**

A) Watermark  
B) Checkpoint location  
C) Trigger interval  
D) Output mode  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Checkpoint location**

Explanation: A checkpoint location is required for fault tolerance in structured streaming. It stores the state and progress of the streaming query, allowing it to recover from failures.
</details>

---

## Question 3
**Which output mode requires an aggregation?**

A) append  
B) complete  
C) update  
D) Both B and C  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) Both B and C**

Explanation: Both "complete" and "update" output modes require aggregations. "complete" outputs the entire result table, while "update" outputs only changed rows. "append" can work without aggregations.
</details>

---

## Question 4
**What does trigger(once=True) do?**

A) Processes data continuously  
B) Processes available data once and stops  
C) Triggers every second  
D) Requires manual triggering  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Processes available data once and stops**

Explanation: trigger(once=True) processes all available data in a single batch and then stops. This is useful for incremental batch processing patterns.
</details>

---

## Question 5
**Which method stops a streaming query?**

A) query.terminate()  
B) query.stop()  
C) query.end()  
D) query.close()  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) query.stop()**

Explanation: The stop() method gracefully stops a streaming query. It waits for the current micro-batch to complete before stopping.
</details>

---

## Question 6
**What is the purpose of awaitTermination()?**

A) To stop the query  
B) To wait for the query to finish  
C) To start the query  
D) To restart the query  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To wait for the query to finish**

Explanation: awaitTermination() blocks the current thread until the streaming query terminates, either by completion or error. It can optionally take a timeout parameter.
</details>

---

## Question 7
**Which trigger type processes data as fast as possible?**

A) trigger(once=True)  
B) trigger(processingTime="1 second")  
C) trigger(continuous="1 second")  
D) No trigger specified  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) No trigger specified**

Explanation: When no trigger is specified, the streaming query processes data as fast as possible in micro-batches. This is the default behavior.
</details>

---

## Question 8
**What does readStream return?**

A) DataFrame  
B) DataStreamReader  
C) StreamingQuery  
D) Dataset  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) DataFrame**

Explanation: readStream returns a streaming DataFrame, which represents an unbounded table that can be queried using the same DataFrame API as batch data.
</details>

---

## Question 9
**Which method starts a streaming query?**

A) start()  
B) begin()  
C) execute()  
D) run()  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) start()**

Explanation: The start() method begins execution of a streaming query. It returns a StreamingQuery object that can be used to monitor and control the query.
</details>

---

## Question 10
**What happens if a streaming query fails without a checkpoint?**

A) It restarts from the beginning  
B) It resumes from where it failed  
C) It cannot restart  
D) It skips failed data  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) It restarts from the beginning**

Explanation: Without a checkpoint, a streaming query has no way to track its progress. If it fails, it must restart from the beginning of the data source.
</details>

---

## Streaming Transformations (Questions 11-20)

## Question 11
**What is the purpose of withWatermark()?**

A) To filter old data  
B) To handle late-arriving data  
C) To add timestamps  
D) To remove duplicates  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To handle late-arriving data**

Explanation: withWatermark() defines how late data can arrive and still be processed. It's essential for handling out-of-order events in streaming applications.
</details>

---

## Question 12
**What is the syntax for a 5-minute tumbling window?**

A) window("timestamp", "5 minutes")  
B) tumblingWindow("timestamp", "5 minutes")  
C) window("timestamp", "5 minutes", "5 minutes")  
D) tumbling("timestamp", 5)  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) window("timestamp", "5 minutes")**

Explanation: window("timestamp", "5 minutes") creates a 5-minute tumbling window. When only two parameters are provided, the window size and slide duration are the same, creating non-overlapping windows.
</details>

---

## Question 13
**How do you create a sliding window that's 10 minutes long and slides every 5 minutes?**

A) window("timestamp", "10 minutes")  
B) window("timestamp", "10 minutes", "5 minutes")  
C) slidingWindow("timestamp", 10, 5)  
D) window("timestamp", "5 minutes", "10 minutes")  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) window("timestamp", "10 minutes", "5 minutes")**

Explanation: window("timestamp", "10 minutes", "5 minutes") creates a 10-minute window that slides every 5 minutes, resulting in overlapping windows.
</details>

---

## Question 14
**What does dropDuplicates() require in streaming mode?**

A) Nothing special  
B) A watermark  
C) An aggregation  
D) Complete output mode  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) A watermark**

Explanation: In streaming mode, dropDuplicates() requires a watermark to limit the amount of state it needs to maintain. Without a watermark, state would grow indefinitely.
</details>

---

## Question 15
**Which window type uses inactivity gaps?**

A) Tumbling window  
B) Sliding window  
C) Session window  
D) Fixed window  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Session window**

Explanation: Session windows group events based on periods of activity separated by gaps of inactivity. They're useful for analyzing user sessions.
</details>

---

## Question 16
**What is the watermark delay in: withWatermark("timestamp", "10 minutes")?**

A) 10 seconds  
B) 10 minutes  
C) 10 hours  
D) 10 milliseconds  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) 10 minutes**

Explanation: The watermark delay is 10 minutes, meaning data can arrive up to 10 minutes late and still be processed. Data arriving later than this will be dropped.
</details>

---

## Question 17
**Can you join two streaming DataFrames?**

A) No, never  
B) Yes, but both need watermarks  
C) Yes, without any special requirements  
D) Only with complete output mode  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Yes, but both need watermarks**

Explanation: Stream-stream joins are supported but require watermarks on both sides to limit state growth. The watermarks define how long to wait for matching records.
</details>

---

## Question 18
**What happens to data that arrives after the watermark?**

A) It's processed normally  
B) It's dropped  
C) It causes an error  
D) It's queued for later  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) It's dropped**

Explanation: Data arriving after the watermark threshold is considered too late and is dropped. This prevents unbounded state growth in streaming applications.
</details>

---

## Question 19
**Which aggregation function works with streaming?**

A) count()  
B) sum()  
C) avg()  
D) All of the above  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) All of the above**

Explanation: All standard aggregation functions (count, sum, avg, min, max, etc.) work with structured streaming, though some require specific output modes.
</details>

---

## Question 20
**What is the purpose of session_window()?**

A) To create fixed-size windows  
B) To group events by activity sessions  
C) To filter sessions  
D) To join sessions  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To group events by activity sessions**

Explanation: session_window() groups events into sessions based on gaps of inactivity. It's useful for analyzing user behavior patterns.
</details>

---

## Delta Lake MERGE (Questions 21-30)

## Question 21
**What is the correct order of clauses in a MERGE statement?**

A) USING, MERGE INTO, ON, WHEN  
B) MERGE INTO, USING, ON, WHEN  
C) MERGE INTO, ON, USING, WHEN  
D) USING, ON, MERGE INTO, WHEN  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) MERGE INTO, USING, ON, WHEN**

Explanation: The correct syntax is: MERGE INTO target USING source ON condition WHEN clauses. This order is required by SQL syntax.
</details>

---

## Question 22
**Which clause handles records that exist in the target but not in the source?**

A) WHEN MATCHED  
B) WHEN NOT MATCHED  
C) WHEN NOT MATCHED BY SOURCE  
D) None - MERGE doesn't handle this  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) WHEN NOT MATCHED BY SOURCE**

Explanation: WHEN NOT MATCHED BY SOURCE handles records in the target that don't have a match in the source. This is useful for deleting records that no longer exist in the source.
</details>

---

## Question 23
**Can you have multiple WHEN MATCHED clauses?**

A) No, only one is allowed  
B) Yes, with different conditions  
C) Yes, but they must be identical  
D) Only in Delta Lake 2.0+  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Yes, with different conditions**

Explanation: You can have multiple WHEN MATCHED clauses with different predicates. They're evaluated in order, and the first matching condition is executed.
</details>

---

## Question 24
**What should you do before MERGE if the source has duplicates?**

A) Nothing, MERGE handles it  
B) Deduplicate the source  
C) Use DISTINCT in MERGE  
D) Add a WHERE clause  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Deduplicate the source**

Explanation: You must deduplicate the source before MERGE. If the source has duplicates, MERGE will fail with a "cardinality violation" error.
</details>

---

## Question 25
**Which operation is only valid in WHEN MATCHED?**

A) INSERT  
B) UPDATE  
C) DELETE  
D) Both B and C  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) Both B and C**

Explanation: DELETE and UPDATE are only valid in WHEN MATCHED clauses because they operate on existing records. INSERT is only valid in WHEN NOT MATCHED.
</details>

---

## Question 26
**What does SET * do in a MERGE UPDATE clause?**

A) Updates all columns  
B) Updates only changed columns  
C) Updates all columns from source  
D) Causes an error  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Updates all columns from source**

Explanation: SET * updates all columns in the target with values from the source. It's a shorthand for listing all columns explicitly.
</details>

---

## Question 27
**How do you perform a conditional DELETE in MERGE?**

A) WHEN MATCHED AND condition THEN DELETE  
B) WHEN MATCHED THEN DELETE WHERE condition  
C) DELETE WHEN condition  
D) WHEN MATCHED DELETE IF condition  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) WHEN MATCHED AND condition THEN DELETE**

Explanation: Add a predicate to the WHEN MATCHED clause: WHEN MATCHED AND condition THEN DELETE. This deletes only rows meeting the condition.
</details>

---

## Question 28
**What is UPSERT?**

A) UPDATE only  
B) INSERT only  
C) UPDATE if exists, INSERT if not  
D) DELETE and INSERT  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) UPDATE if exists, INSERT if not**

Explanation: UPSERT (UPDATE + INSERT) updates existing records and inserts new ones. It's implemented with MERGE using WHEN MATCHED UPDATE and WHEN NOT MATCHED INSERT.
</details>

---

## Question 29
**Can MERGE be used in streaming with foreachBatch?**

A) No, MERGE is batch-only  
B) Yes, in the foreachBatch function  
C) Only with complete output mode  
D) Only for small datasets  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Yes, in the foreachBatch function**

Explanation: MERGE can be used in streaming by implementing it inside a foreachBatch function, which processes each micro-batch as a batch operation.
</details>

---

## Question 30
**What happens if you MERGE without deduplicating the source?**

A) It works fine  
B) Cardinality violation error  
C) Only first match is used  
D) All matches are applied  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Cardinality violation error**

Explanation: If the source has duplicates for a key, MERGE fails with a cardinality violation error because it doesn't know which source row to use for the update.
</details>

---

## Change Data Capture (Questions 31-40)

## Question 31
**What are the three main CDC operation types?**

A) CREATE, READ, UPDATE  
B) INSERT, UPDATE, DELETE  
C) ADD, MODIFY, REMOVE  
D) NEW, CHANGE, DROP  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) INSERT, UPDATE, DELETE**

Explanation: The three main CDC operations are INSERT (new records), UPDATE (modified records), and DELETE (removed records). These represent all possible changes to data.
</details>

---

## Question 32
**What is SCD Type 1?**

A) Keep all history  
B) Overwrite with new values  
C) Add new columns for changes  
D) Create separate history table  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Overwrite with new values**

Explanation: SCD Type 1 simply overwrites old values with new ones, maintaining no history. It's the simplest approach but loses historical data.
</details>

---

## Question 33
**What is SCD Type 2?**

A) Overwrite old values  
B) Keep full history with effective dates  
C) Store only last two versions  
D) Archive old records separately  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Keep full history with effective dates**

Explanation: SCD Type 2 maintains full history by creating new records for changes, with effective start/end dates and an is_current flag to identify the active version.
</details>

---

## Question 34
**In SCD Type 2, what indicates the current record?**

A) Latest timestamp  
B) is_current = true  
C) effective_end_date IS NULL  
D) Both B and C  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) Both B and C**

Explanation: Current records are typically identified by both is_current = true and effective_end_date IS NULL. Using both provides redundancy and clarity.
</details>

---

## Question 35
**How do you handle out-of-order CDC events?**

A) Process them as they arrive  
B) Sort by timestamp and deduplicate  
C) Reject them  
D) Store them separately  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Sort by timestamp and deduplicate**

Explanation: To handle out-of-order events, sort by timestamp and keep only the latest operation per key using window functions or deduplication.
</details>

---

## Question 36
**What should you do when processing a DELETE CDC event in SCD Type 2?**

A) Delete the record  
B) Close the current record (set is_current = false)  
C) Mark it as deleted but keep it  
D) Ignore it  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Close the current record (set is_current = false)**

Explanation: In SCD Type 2, DELETE events close the current record by setting is_current = false and effective_end_date. This maintains the history of the deleted record.
</details>

---

## Question 37
**Which MERGE clause handles CDC INSERT operations?**

A) WHEN MATCHED  
B) WHEN NOT MATCHED  
C) WHEN NOT MATCHED BY SOURCE  
D) WHEN INSERTED  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) WHEN NOT MATCHED**

Explanation: INSERT operations are handled by WHEN NOT MATCHED, which inserts new records that don't exist in the target table.
</details>

---

## Question 38
**How do you implement SCD Type 2 for an UPDATE?**

A) UPDATE the existing record  
B) Close old record and INSERT new record  
C) INSERT new record only  
D) DELETE old and INSERT new  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Close old record and INSERT new record**

Explanation: SCD Type 2 updates require two steps: close the old record (set is_current = false, effective_end_date = now) and insert a new record with the updated values.
</details>

---

## Question 39
**What is the record_version column used for in SCD Type 2?**

A) Required by Delta Lake  
B) Tracks the number of changes  
C) Used for sorting  
D) Identifies the table version  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Tracks the number of changes**

Explanation: record_version tracks how many times a record has been updated. It increments with each change and helps identify the sequence of changes.
</details>

---

## Question 40
**Can you process CDC events in streaming mode?**

A) No, only batch  
B) Yes, using foreachBatch  
C) Yes, but only for INSERT  
D) Only with Auto Loader  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Yes, using foreachBatch**

Explanation: CDC can be processed in streaming mode using foreachBatch, which allows you to apply MERGE logic to each micro-batch of CDC events.
</details>

---

## Multi-Hop Architecture (Questions 41-45)

## Question 41
**What are the three layers of Medallion Architecture?**

A) Raw, Processed, Final  
B) Bronze, Silver, Gold  
C) Input, Transform, Output  
D) Source, Stage, Target  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Bronze, Silver, Gold**

Explanation: Medallion Architecture consists of Bronze (raw data), Silver (cleaned/conformed data), and Gold (business-level aggregates) layers.
</details>

---

## Question 42
**What is the purpose of the Bronze layer?**

A) Business aggregations  
B) Data cleaning  
C) Raw data ingestion  
D) Final reporting  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Raw data ingestion**

Explanation: The Bronze layer stores raw, unprocessed data exactly as it arrives from source systems. It provides a complete audit trail and allows reprocessing if needed.
</details>

---

## Question 43
**What transformations belong in the Silver layer?**

A) Business calculations  
B) Data cleaning and validation  
C) Raw data storage  
D) Report generation  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Data cleaning and validation**

Explanation: The Silver layer performs data cleaning, validation, deduplication, and schema conformance. It creates a clean, reliable foundation for analytics.
</details>

---

## Question 44
**What type of data goes in the Gold layer?**

A) Raw source data  
B) Cleaned transactional data  
C) Business-level aggregates  
D) Archived data  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Business-level aggregates**

Explanation: The Gold layer contains business-level aggregates, metrics, and denormalized data optimized for specific use cases and reporting.
</details>

---

## Question 45
**Should Bronze layer tables be partitioned?**

A) Never  
B) Always  
C) By ingestion date  
D) By business key  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) By ingestion date**

Explanation: Bronze tables are typically partitioned by ingestion date to enable efficient incremental processing and data lifecycle management.
</details>

---

## Optimization Techniques (Questions 46-50)

## Question 46
**What does OPTIMIZE do?**

A) Speeds up queries  
B) Compacts small files  
C) Removes old data  
D) Creates indexes  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Compacts small files**

Explanation: OPTIMIZE compacts small files into larger ones (target: ~1GB), improving query performance by reducing the number of files to read.
</details>

---

## Question 47
**What is the maximum number of columns recommended for Z-ORDER?**

A) 2  
B) 4  
C) 8  
D) Unlimited  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) 4**

Explanation: Z-ORDER is most effective with 2-4 columns. Beyond 4 columns, the benefits diminish due to the curse of dimensionality.
</details>

---

## Question 48
**What is the default retention period for VACUUM?**

A) 1 day  
B) 7 days  
C) 30 days  
D) 90 days  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) 7 days**

Explanation: The default VACUUM retention period is 7 days (168 hours). This ensures time travel queries can still access recent versions.
</details>

---

## Question 49
**When should you partition a table?**

A) Always  
B) For high-cardinality columns frequently filtered  
C) For low-cardinality columns frequently filtered  
D) Never, use Z-ORDER instead  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) For low-cardinality columns frequently filtered**

Explanation: Partition on low-cardinality columns (like date, region) that are frequently used in WHERE clauses. High-cardinality partitioning creates too many small partitions.
</details>

---

## Question 50
**What does data skipping use to improve performance?**

A) Indexes  
B) Statistics on file-level min/max values  
C) Caching  
D) Compression  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Statistics on file-level min/max values**

Explanation: Data skipping uses min/max statistics stored in the Delta transaction log to skip files that don't contain relevant data, dramatically improving query performance.
</details>

---

## Scoring Guide

- **45-50 correct (90-100%)**: Excellent! You've mastered Week 3 concepts
- **40-44 correct (80-89%)**: Very Good! Review missed topics
- **35-39 correct (70-79%)**: Good! Additional practice recommended
- **30-34 correct (60-69%)**: Fair! Review Week 3 materials
- **Below 30 (< 60%)**: Needs Improvement! Revisit Week 3 content

## Next Steps

1. Review explanations for any questions you missed
2. Revisit the relevant day's content for weak areas
3. Complete the hands-on exercises again
4. Build your own end-to-end pipeline
5. Move on to Week 4 when ready!

---

**Congratulations on completing Week 3!** 🎉

You've learned critical skills for incremental data processing that represent 25% of the Databricks Data Engineer Associate exam.
