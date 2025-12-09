# Day 17: Streaming Transformations - Quiz

Test your understanding of watermarking, windowing, and advanced streaming transformations.

**Time Limit**: 15 minutes  
**Passing Score**: 80% (12/15 correct)

---

## Questions

### Question 1
What is a watermark in Structured Streaming?

A) A data quality check  
B) A threshold defining how long to wait for late data  
C) A performance optimization  
D) A data compression technique  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) A threshold defining how long to wait for late data**

**Explanation**: A watermark defines how long Spark waits for late-arriving data before considering a time window complete. It's calculated as: Watermark = Max Event Time - Watermark Delay.

</details>

---

### Question 2
Why are watermarks necessary for streaming aggregations?

A) To improve performance  
B) To bound state and enable append mode  
C) To compress data  
D) To validate schema  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To bound state and enable append mode**

**Explanation**: Without watermarks, state would grow indefinitely and you couldn't use append mode with aggregations. Watermarks allow Spark to finalize windows and clean up old state.

</details>

---

### Question 3
What is the difference between event time and processing time?

A) They are the same  
B) Event time is when the event occurred; processing time is when Spark processes it  
C) Processing time is more accurate  
D) Event time is always later  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Event time is when the event occurred; processing time is when Spark processes it**

**Explanation**: Event time reflects when the event actually happened (from the data), while processing time is when Spark processes the event. Event time is preferred for accurate analytics.

</details>

---

### Question 4
What is a tumbling window?

A) A window that moves continuously  
B) A non-overlapping, fixed-size time window  
C) A window that overlaps with others  
D) A window based on event count  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) A non-overlapping, fixed-size time window**

**Explanation**: Tumbling windows are non-overlapping, fixed-size windows. Each event belongs to exactly one window. Example: [00:00-00:10), [00:10-00:20), [00:20-00:30).

</details>

---

### Question 5
How do you create a 10-minute tumbling window?

A) `window(col("timestamp"), "10 minutes")`  
B) `tumbling_window(col("timestamp"), 10)`  
C) `time_window(col("timestamp"), 600)`  
D) `window(col("timestamp"), 10)`  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) `window(col("timestamp"), "10 minutes")`**

**Explanation**: Use `window(timeColumn, windowDuration)` for tumbling windows. The duration is specified as a string like "10 minutes", "1 hour", etc.

</details>

---

### Question 6
What is a sliding window?

A) A non-overlapping window  
B) A window that overlaps with other windows  
C) A window that never closes  
D) A window based on row count  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) A window that overlaps with other windows**

**Explanation**: Sliding windows overlap when the slide duration is less than the window duration. Events can belong to multiple windows. Example: 10-minute windows sliding every 5 minutes.

</details>

---

### Question 7
How do you create a sliding window (10-minute window, 5-minute slide)?

A) `window(col("timestamp"), "10 minutes", "5 minutes")`  
B) `sliding_window(col("timestamp"), 10, 5)`  
C) `window(col("timestamp"), "5 minutes", "10 minutes")`  
D) `overlap_window(col("timestamp"), 10, 5)`  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) `window(col("timestamp"), "10 minutes", "5 minutes")`**

**Explanation**: Use `window(timeColumn, windowDuration, slideDuration)` for sliding windows. The first parameter is window size, second is slide interval.

</details>

---

### Question 8
Can you use append mode with aggregations if you have a watermark?

A) No, never  
B) Yes, watermarks enable append mode for aggregations  
C) Only with complete mode  
D) Only for non-windowed aggregations  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Yes, watermarks enable append mode for aggregations**

**Explanation**: Watermarks allow Spark to finalize windows, making it possible to use append mode with windowed aggregations. Without watermarks, only complete or update modes work.

</details>

---

### Question 9
What happens to events that arrive after the watermark threshold?

A) They are processed normally  
B) They are dropped as too late  
C) They are queued for later  
D) They cause an error  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) They are dropped as too late**

**Explanation**: Events with timestamps before the watermark threshold are considered too late and are dropped. This is the trade-off for bounded state.

</details>

---

### Question 10
What are the requirements for stream-stream joins?

A) No special requirements  
B) Both streams must have watermarks and time constraints  
C) Only one stream needs a watermark  
D) Watermarks are optional  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Both streams must have watermarks and time constraints**

**Explanation**: Stream-stream joins require watermarks on both streams and time constraints in the join condition to prevent unbounded state growth.

</details>

---

### Question 11
How does deduplication with watermarks differ from without?

A) No difference  
B) With watermarks, state is bounded; without, it grows indefinitely  
C) Without watermarks is faster  
D) Watermarks make deduplication impossible  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) With watermarks, state is bounded; without, it grows indefinitely**

**Explanation**: Watermarks allow Spark to clean up old deduplication state. Without watermarks, Spark must remember every event_id forever, causing unbounded state growth.

</details>

---

### Question 12
What is the trade-off when choosing watermark delay?

A) No trade-off exists  
B) Shorter delay = less state but more dropped data; longer delay = more state but less dropped data  
C) Longer delay is always better  
D) Shorter delay is always better  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Shorter delay = less state but more dropped data; longer delay = more state but less dropped data**

**Explanation**: Shorter watermarks finalize windows faster and use less memory but drop more late data. Longer watermarks accept more late data but require more memory and slower finalization.

</details>

---

### Question 13
Which time should you use for windowing operations?

A) Processing time  
B) Event time  
C) Current timestamp  
D) System time  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Event time**

**Explanation**: Always use event time (when the event occurred) for windowing, not processing time. Event time provides accurate analytics even when events arrive out of order.

</details>

---

### Question 14
How do you extract the window start time from a windowed aggregation?

A) `col("window_start")`  
B) `col("window.start")`  
C) `window_start(col("window"))`  
D) `start_time(col("window"))`  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) `col("window.start")`**

**Explanation**: The window column is a struct with `start` and `end` fields. Access them using `col("window.start")` and `col("window.end")`.

</details>

---

### Question 15
What happens to state in streaming queries with watermarks?

A) State grows indefinitely  
B) State is automatically cleaned up for data older than the watermark  
C) State is never cleaned up  
D) State is cleared after each batch  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) State is automatically cleaned up for data older than the watermark**

**Explanation**: Watermarks enable automatic state cleanup. Once data is older than the watermark threshold, Spark can safely remove it from state, preventing unbounded growth.

</details>

---

## Scoring Guide

- **15/15**: Excellent! You've mastered streaming transformations
- **12-14**: Good understanding, review missed topics
- **9-11**: Fair, review watermarking and windowing concepts
- **Below 9**: Review the README.md and practice exercises again

## Key Topics to Review

If you scored below 80%, focus on these areas:

1. **Watermarking**: Understand purpose, calculation, and trade-offs
2. **Event Time vs Processing Time**: Know when to use each
3. **Tumbling vs Sliding Windows**: Understand differences and use cases
4. **Append Mode**: Know when it's possible (with watermarks)
5. **Stream-Stream Joins**: Understand requirements
6. **State Management**: Know how watermarks bound state

---

**Next Steps**: 
- Review any incorrect answers
- Practice watermarking and windowing in exercise.py
- Move on to Day 18: Delta Lake MERGE (UPSERT)

