# Day 27 Quiz: Production Best Practices

Test your knowledge of production pipeline best practices.

---

## Question 1
**What is idempotency in data pipelines?**

A) Processing data faster  
B) The ability to run an operation multiple times with the same result  
C) Processing data in parallel  
D) Encrypting data at rest  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) The ability to run an operation multiple times with the same result**

Explanation: Idempotency means that running the same operation multiple times produces the same result, which is crucial for reliable pipelines that may need to retry or reprocess data.
</details>

---

## Question 2
**Which Delta Lake operation is best for implementing idempotent writes?**

A) INSERT  
B) APPEND  
C) MERGE  
D) OVERWRITE  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) MERGE**

Explanation: MERGE (UPSERT) is idempotent because it updates existing records and inserts new ones. Running it multiple times with the same data produces the same result.
</details>

---

## Question 3
**What is exponential backoff in retry logic?**

A) Retrying immediately after failure  
B) Increasing the delay between retries exponentially  
C) Decreasing the delay between retries  
D) Retrying a fixed number of times  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Increasing the delay between retries exponentially**

Explanation: Exponential backoff increases the wait time between retries (e.g., 1s, 2s, 4s, 8s), preventing overwhelming the system and allowing transient issues to resolve.
</details>

---

## Question 4
**What should you do with sensitive credentials in production pipelines?**

A) Hardcode them in the script  
B) Store them in a configuration file  
C) Use Databricks Secrets or environment variables  
D) Pass them as command-line arguments  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Use Databricks Secrets or environment variables**

Explanation: Never hardcode credentials. Use Databricks Secrets (accessed via dbutils.secrets.get()) or secure environment variables to protect sensitive information.
</details>

---

## Question 5
**What is the purpose of structured logging?**

A) To make logs look pretty  
B) To store logs in a structured format (JSON) for easy parsing and analysis  
C) To reduce log file size  
D) To encrypt log data  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To store logs in a structured format (JSON) for easy parsing and analysis**

Explanation: Structured logging uses formats like JSON with consistent fields (timestamp, level, message, context), making logs easily searchable and analyzable.
</details>

---

## Question 6
**Which exception should you typically retry in a data pipeline?**

A) ValueError (invalid data)  
B) ConnectionError (network issue)  
C) KeyError (missing key)  
D) ZeroDivisionError  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) ConnectionError (network issue)**

Explanation: Transient errors like ConnectionError, TimeoutError, or temporary network issues are good candidates for retry. Logic errors (ValueError, KeyError) should not be retried.
</details>

---

## Question 7
**What is a health check in production pipelines?**

A) A test that runs before deployment  
B) A periodic check to verify pipeline and data health  
C) A performance optimization technique  
D) A security scan  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) A periodic check to verify pipeline and data health**

Explanation: Health checks monitor data freshness, row counts, schema consistency, and other metrics to ensure the pipeline is functioning correctly.
</details>

---

## Question 8
**What is the purpose of a checkpoint in incremental processing?**

A) To save intermediate results  
B) To track the last successfully processed position  
C) To create backups  
D) To improve performance  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To track the last successfully processed position**

Explanation: Checkpoints store the last processed timestamp, ID, or offset, allowing the pipeline to resume from where it left off and avoid reprocessing data.
</details>

---

## Question 9
**What is schema evolution in production pipelines?**

A) Optimizing table schemas  
B) Handling changes to data schemas over time  
C) Creating new schemas  
D) Deleting old schemas  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Handling changes to data schemas over time**

Explanation: Schema evolution is the ability to handle schema changes (new columns, type changes) gracefully without breaking the pipeline or requiring manual intervention.
</details>

---

## Question 10
**What is the difference between a canary deployment and a blue-green deployment?**

A) Canary routes small percentage to new version; blue-green switches all traffic  
B) They are the same thing  
C) Canary is for testing; blue-green is for production  
D) Canary is faster than blue-green  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) Canary routes small percentage to new version; blue-green switches all traffic**

Explanation: Canary deployment gradually routes a small percentage of traffic to the new version to test it. Blue-green deployment maintains two identical environments and switches all traffic at once.
</details>

---

## Question 11
**What should you log when a pipeline fails?**

A) Only the error message  
B) Error message, stack trace, context (pipeline name, run ID, input data)  
C) Nothing, to save storage  
D) Only the timestamp  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Error message, stack trace, context (pipeline name, run ID, input data)**

Explanation: Comprehensive logging includes error details, stack trace, and context (pipeline name, run ID, input parameters) to facilitate debugging and root cause analysis.
</details>

---

## Question 12
**What is alert throttling?**

A) Sending alerts faster  
B) Preventing duplicate alerts within a time window  
C) Prioritizing critical alerts  
D) Encrypting alert messages  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Preventing duplicate alerts within a time window**

Explanation: Alert throttling prevents sending the same alert repeatedly within a specified time period, avoiding alert fatigue and noise.
</details>

---

## Question 13
**Which of the following is a best practice for error handling?**

A) Catch all exceptions with a generic except block  
B) Catch specific exceptions and handle them appropriately  
C) Ignore errors to keep the pipeline running  
D) Only log errors without taking action  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Catch specific exceptions and handle them appropriately**

Explanation: Catch specific exceptions (AnalysisException, ValueError, etc.) and handle each appropriately. Generic exception handlers can mask unexpected errors.
</details>

---

## Question 14
**What is the purpose of caching in Spark pipelines?**

A) To save data to disk permanently  
B) To store intermediate results in memory for reuse  
C) To compress data  
D) To encrypt data  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To store intermediate results in memory for reuse**

Explanation: Caching (`.cache()` or `.persist()`) stores DataFrame results in memory, improving performance when the same data is accessed multiple times. Always unpersist when done.
</details>

---

## Question 15
**What is shadow mode in deployment?**

A) Deploying at night  
B) Running new version alongside production without affecting output  
C) Hiding the deployment from users  
D) Testing in a separate environment  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Running new version alongside production without affecting output**

Explanation: Shadow mode runs the new pipeline version in parallel with production, comparing results without affecting the actual output. This validates the new version safely.
</details>

---

## Scoring Guide

- **13-15 correct (87-100%)**: Excellent! You're ready for production pipelines.
- **11-12 correct (73-80%)**: Good job! Minor review recommended.
- **9-10 correct (60-67%)**: Fair. Review the material again.
- **Below 9 (< 60%)**: Re-read Day 27 content thoroughly.

---

## Key Concepts to Remember

1. **Idempotency**: Use MERGE for idempotent writes
2. **Retry Logic**: Implement exponential backoff for transient errors
3. **Error Handling**: Catch specific exceptions, log comprehensively
4. **Secrets Management**: Use Databricks Secrets, never hardcode
5. **Structured Logging**: Use JSON format with consistent fields
6. **Health Checks**: Monitor data freshness, row counts, schema
7. **Checkpoints**: Track last processed position for incremental processing
8. **Schema Evolution**: Handle schema changes gracefully
9. **Alerting**: Implement throttling to prevent alert fatigue
10. **Deployment**: Use canary or blue-green strategies for safe rollouts

---

## Next Steps

- Review any questions you got wrong
- Re-read relevant sections in Day 27 README
- Practice implementing error handling and retry logic
- Move on to Day 28: Practice Exam 1

