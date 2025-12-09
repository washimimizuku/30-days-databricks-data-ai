# Day 19: Change Data Capture (CDC) - Quiz

## Instructions
- Answer all 15 questions
- Each question has one correct answer unless stated otherwise
- Check your answers at the end

---

## Questions

### Question 1
What does CDC stand for in data engineering?

A) Continuous Data Collection  
B) Change Data Capture  
C) Central Data Catalog  
D) Cached Data Container  

### Question 2
Which CDC operation type represents adding a new record?

A) ADD  
B) CREATE  
C) INSERT  
D) NEW  

### Question 3
What is the primary Delta Lake operation used to process CDC events?

A) UPDATE  
B) INSERT  
C) MERGE  
D) APPLY  

### Question 4
In SCD Type 1, how is historical data handled?

A) Stored in a separate history table  
B) Tracked with effective dates  
C) Overwritten (no history kept)  
D) Archived to cold storage  

### Question 5
What is the purpose of a sequence number in CDC events?

A) To count total events  
B) To ensure events are processed in the correct order  
C) To identify the source system  
D) To calculate event frequency  

### Question 6
In SCD Type 2, what does the `is_current` flag indicate?

A) Whether the record is valid  
B) Whether the record is the latest version  
C) Whether the record has been processed  
D) Whether the record is archived  

### Question 7
How do you query the current state of an SCD Type 2 table?

A) SELECT * FROM table ORDER BY effective_date DESC LIMIT 1  
B) SELECT * FROM table WHERE is_current = true  
C) SELECT * FROM table WHERE end_date IS NULL  
D) Both B and C  

### Question 8
What is the recommended approach for streaming CDC processing in Databricks?

A) Use writeStream with append mode  
B) Use foreachBatch with MERGE  
C) Use complete output mode  
D) Use update mode only  

### Question 9
Why is deduplication important in CDC processing?

A) To reduce storage costs  
B) To prevent duplicate events from causing incorrect updates  
C) To improve query performance  
D) To compress data  

### Question 10
What does idempotency mean in CDC processing?

A) Processing events exactly once  
B) Processing can be run multiple times with the same result  
C) Processing events in parallel  
D) Processing events in batches  

### Question 11
In SCD Type 2, what should the `end_date` be for the current version of a record?

A) Current timestamp  
B) NULL  
C) Maximum date (9999-12-31)  
D) The effective_date  

### Question 12
How do you handle late-arriving CDC events?

A) Ignore them  
B) Use sequence numbers to determine correct order  
C) Always process them first  
D) Store them in a separate table  

### Question 13
What is the purpose of the `effective_date` in SCD Type 2?

A) When the record was created in the database  
B) When the change became effective in the real world  
C) When the record will expire  
D) When the record was last queried  

### Question 14
Which MERGE clause handles DELETE operations in CDC?

A) WHEN MATCHED THEN DELETE  
B) WHEN MATCHED DELETE  
C) WHEN MATCHED AND operation = 'DELETE' THEN DELETE  
D) WHEN DELETED THEN REMOVE  

### Question 15
What is a best practice for CDC validation?

A) Process all events without checking  
B) Validate operation types and required fields before processing  
C) Only validate after processing  
D) Validation is not necessary  

---

## Answer Key

<details>
<summary>Click to reveal answers</summary>

### Answer 1
**B) Change Data Capture**

Explanation: CDC stands for Change Data Capture, a design pattern that tracks changes (INSERT, UPDATE, DELETE) in source systems and propagates them to target systems.

### Answer 2
**C) INSERT**

Explanation: INSERT is the standard CDC operation type for adding new records. Other common operations are UPDATE and DELETE.

### Answer 3
**C) MERGE**

Explanation: MERGE is the primary Delta Lake operation for processing CDC events because it can handle INSERT, UPDATE, and DELETE operations in a single atomic transaction.

### Answer 4
**C) Overwritten (no history kept)**

Explanation: SCD Type 1 simply overwrites old values with new ones, maintaining no historical record of changes. This is appropriate when historical values don't matter.

### Answer 5
**B) To ensure events are processed in the correct order**

Explanation: Sequence numbers (or timestamps) ensure CDC events are applied in the correct order, especially important when events arrive out of order.

### Answer 6
**B) Whether the record is the latest version**

Explanation: In SCD Type 2, `is_current = true` indicates the current/active version of a record, while `is_current = false` indicates historical versions.

### Answer 7
**D) Both B and C**

Explanation: Both conditions work: `is_current = true` explicitly marks current records, and `end_date IS NULL` indicates records that haven't been superseded. Often both are used together.

### Answer 8
**B) Use foreachBatch with MERGE**

Explanation: The recommended pattern for streaming CDC is to use `foreachBatch` to apply MERGE operations to each micro-batch, allowing full MERGE functionality in streaming.

### Answer 9
**B) To prevent duplicate events from causing incorrect updates**

Explanation: Duplicate CDC events can cause incorrect state if not deduplicated. Deduplication ensures each change is applied exactly once.

### Answer 10
**B) Processing can be run multiple times with the same result**

Explanation: Idempotency means CDC processing can be safely rerun without changing the result. This is achieved using sequence numbers or timestamps to prevent reapplying old changes.

### Answer 11
**B) NULL**

Explanation: In SCD Type 2, the current version has `end_date = NULL` to indicate it's still active. When a new version is created, the old version's `end_date` is set to the new version's `effective_date`.

### Answer 12
**B) Use sequence numbers to determine correct order**

Explanation: Sequence numbers (or timestamps) allow you to process late-arriving events correctly by ensuring they're applied in the right order, even if they arrive out of sequence.

### Answer 13
**B) When the change became effective in the real world**

Explanation: `effective_date` represents when the change actually occurred or became effective in the business context, not when it was processed by the system.

### Answer 14
**C) WHEN MATCHED AND operation = 'DELETE' THEN DELETE**

Explanation: The correct syntax is `WHEN MATCHED AND condition THEN DELETE`, where the condition checks if the CDC operation is 'DELETE'.

### Answer 15
**B) Validate operation types and required fields before processing**

Explanation: Best practice is to validate CDC events before processing: check operation types are valid (INSERT/UPDATE/DELETE), required fields are not null, and data types are correct.

</details>

---

## Scoring Guide

- **13-15 correct**: Excellent! You have a strong understanding of CDC
- **10-12 correct**: Good! Review the topics you missed
- **7-9 correct**: Fair. Spend more time practicing CDC operations
- **Below 7**: Review the README.md and practice more exercises

---

## Key Concepts to Remember

1. **CDC** = Change Data Capture (INSERT, UPDATE, DELETE)
2. **MERGE** is the primary tool for applying CDC events
3. **SCD Type 1** = Overwrite (no history)
4. **SCD Type 2** = Historical tracking with effective dates
5. **Sequence numbers** ensure correct event ordering
6. **Deduplication** prevents duplicate event processing
7. **Idempotency** allows safe reprocessing
8. **foreachBatch** enables streaming CDC with MERGE
9. **is_current** flag identifies current records in SCD Type 2
10. **Validation** catches malformed events before processing

---

**Next**: Day 20 - Multi-Hop Architecture (Bronze → Silver → Gold)
