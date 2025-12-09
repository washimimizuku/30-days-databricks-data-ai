# Day 16: Structured Streaming Basics - Quiz

Test your understanding of Structured Streaming fundamentals.

**Time Limit**: 15 minutes  
**Passing Score**: 80% (12/15 correct)

---

## Questions

### Question 1
What is the key difference between `spark.read` and `spark.readStream`?

A) readStream is faster  
B) readStream processes data as an unbounded table  
C) read is for Delta tables only  
D) They are identical  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) readStream processes data as an unbounded table**

**Explanation**: `readStream` treats data as a continuously growing unbounded table, while `read` processes a fixed batch of data. The API is similar, but readStream enables continuous processing.

</details>

---

### Question 2
Which output mode should you use for a query that only filters data without aggregations?

A) complete  
B) update  
C) append  
D) merge  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) append**

**Explanation**: Append mode is used for queries without aggregations. It only writes new rows to the sink. Complete and update modes are for aggregations only.

</details>

---

### Question 3
What is the purpose of a checkpoint in Structured Streaming?

A) To improve performance  
B) To track progress and enable fault tolerance  
C) To compress data  
D) To validate schema  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To track progress and enable fault tolerance**

**Explanation**: Checkpoints store the streaming query's progress (offsets, state) enabling recovery from failures and ensuring exactly-once processing semantics.

</details>

---

### Question 4
Which trigger mode processes all available data and then stops?

A) `trigger(processingTime="1 minute")`  
B) `trigger(once=True)`  
C) `trigger(availableNow=True)`  
D) `trigger(continuous=True)`  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) `trigger(availableNow=True)`**

**Explanation**: `availableNow=True` processes all available data in micro-batches and stops. It's ideal for scheduled batch jobs using streaming APIs. `once=True` is deprecated.

</details>

---

### Question 5
Which output mode writes the entire result table after every trigger?

A) append  
B) complete  
C) update  
D) overwrite  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) complete**

**Explanation**: Complete mode writes the entire result table to the sink after every trigger. It's only supported for aggregation queries and the entire result must fit in memory.

</details>

---

### Question 6
What happens if you don't specify a checkpoint location for a streaming query?

A) Uses default location  
B) Error occurs  
C) Query runs without fault tolerance  
D) Checkpoint is disabled  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Error occurs**

**Explanation**: Checkpoint location is required for streaming queries. Without it, Spark cannot track progress or recover from failures, so the query will fail to start.

</details>

---

### Question 7
Which sink is recommended for production streaming queries?

A) Console  
B) Memory  
C) Delta Lake  
D) CSV files  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Delta Lake**

**Explanation**: Delta Lake is the recommended sink for production because it provides ACID transactions, schema evolution, time travel, and supports all output modes.

</details>

---

### Question 8
What does `trigger(processingTime="10 seconds")` do?

A) Processes data every 10 seconds  
B) Times out after 10 seconds  
C) Processes 10 seconds of data  
D) Waits 10 seconds before starting  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) Processes data every 10 seconds**

**Explanation**: This trigger causes the streaming query to process new data at fixed 10-second intervals, regardless of how long the previous batch took.

</details>

---

### Question 9
Which output mode only writes rows that were updated?

A) append  
B) complete  
C) update  
D) delta  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) update**

**Explanation**: Update mode only writes rows that were updated in the result table. It's used for aggregations with large result sets and requires a sink that supports updates (like Delta).

</details>

---

### Question 10
How do you check if a streaming query is still running?

A) `query.status()`  
B) `query.isActive`  
C) `query.running()`  
D) `query.check()`  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) `query.isActive`**

**Explanation**: `query.isActive` returns a boolean indicating whether the streaming query is currently running. Use `query.status` to get detailed status information.

</details>

---

### Question 11
What type of join is supported between a streaming DataFrame and a static DataFrame?

A) Only inner join  
B) Only left join  
C) All join types  
D) No joins allowed  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) All join types**

**Explanation**: Stream-static joins support all join types (inner, left, right, full). The static DataFrame is treated as a lookup table that doesn't change during the query.

</details>

---

### Question 12
Which option limits the number of files processed per trigger?

A) `maxFilesPerBatch`  
B) `maxFilesPerTrigger`  
C) `filesPerTrigger`  
D) `triggerFiles`  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) `maxFilesPerTrigger`**

**Explanation**: `maxFilesPerTrigger` limits how many new files are processed in each micro-batch, useful for rate limiting and controlling resource usage.

</details>

---

### Question 13
What happens when you restart a streaming query with the same checkpoint location?

A) Error occurs  
B) Starts from beginning  
C) Resumes from last checkpoint  
D) Skips all data  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Resumes from last checkpoint**

**Explanation**: When restarted with the same checkpoint, the query automatically resumes from where it left off, ensuring no data is lost or reprocessed.

</details>

---

### Question 14
Which method blocks execution until the streaming query terminates?

A) `query.wait()`  
B) `query.block()`  
C) `query.awaitTermination()`  
D) `query.hold()`  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) `query.awaitTermination()`**

**Explanation**: `awaitTermination()` blocks the current thread until the streaming query terminates (either by failure or manual stop), keeping the query running.

</details>

---

### Question 15
Can you use window functions (like ROW_NUMBER) in streaming queries?

A) Yes, always  
B) No, never  
C) Yes, but only with watermarks  
D) Only in batch mode  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Yes, but only with watermarks**

**Explanation**: Window functions in streaming require watermarks to bound the state. Without watermarks, the state would grow indefinitely. We'll cover watermarks in Day 17.

</details>

---

## Scoring Guide

- **15/15**: Excellent! You've mastered streaming basics
- **12-14**: Good understanding, review missed topics
- **9-11**: Fair, review output modes and checkpointing
- **Below 9**: Review the README.md and practice exercises again

## Key Topics to Review

If you scored below 80%, focus on these areas:

1. **Output Modes**: Understand append, complete, and update
2. **Checkpointing**: Know why it's required and how it works
3. **Triggers**: Understand different trigger types and when to use each
4. **Monitoring**: Know how to check query status and progress
5. **Sinks**: Understand different sink types and their use cases

---

**Next Steps**: 
- Review any incorrect answers
- Practice creating streaming queries in exercise.py
- Move on to Day 17: Streaming Transformations (Watermarking and Windowing)

