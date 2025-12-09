# Day 23 Quiz: Databricks Jobs

Test your knowledge of Databricks Jobs.

## Question 1
**What is the recommended cluster type for production jobs?**

A) All-purpose cluster  
B) Job cluster  
C) High-concurrency cluster  
D) Single-node cluster  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Job cluster**

Explanation: Job clusters are recommended for production because they're created when the job starts and terminated when it completes, resulting in lower costs. They also provide better isolation between jobs.
</details>

---

## Question 2
**Which of the following is a valid cron expression for running a job daily at 2 AM?**

A) 0 2 * * * ?  
B) 0 0 2 * * ?  
C) 2 0 0 * * ?  
D) * * 2 * * ?  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) 0 0 2 * * ?**

Explanation: The Quartz cron format is: seconds minutes hours day month day-of-week. So "0 0 2 * * ?" means 0 seconds, 0 minutes, 2 hours (2 AM), every day.
</details>

---

## Question 3
**What does max_concurrent_runs control?**

A) Number of tasks running in parallel  
B) Number of simultaneous job executions  
C) Number of workers in the cluster  
D) Number of retries allowed  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Number of simultaneous job executions**

Explanation: max_concurrent_runs limits how many instances of the same job can run at the same time. Setting it to 1 ensures only one run executes at a time, preventing overlapping executions.
</details>

---

## Question 4
**In a multi-task job, what must task dependencies form?**

A) A tree structure  
B) A directed acyclic graph (DAG)  
C) A circular graph  
D) A linear chain  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) A directed acyclic graph (DAG)**

Explanation: Task dependencies must form a DAG - a directed graph with no cycles. This ensures tasks can be executed in a valid order without circular dependencies.
</details>

---

## Question 5
**How do you pass parameters to a notebook in a job?**

A) Environment variables  
B) Command-line arguments  
C) base_parameters in job configuration  
D) Config files  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) base_parameters in job configuration**

Explanation: Parameters are passed using base_parameters in the notebook_task configuration. The notebook accesses them using dbutils.widgets.get().
</details>

---

## Question 6
**What does dbutils.notebook.exit() do in a job task?**

A) Stops the entire job  
B) Returns a value to the calling task  
C) Exits the notebook with an error  
D) Restarts the task  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Returns a value to the calling task**

Explanation: dbutils.notebook.exit() returns a value that can be accessed by downstream tasks or the job orchestrator. It's useful for passing metrics or status information between tasks.
</details>

---

## Question 7
**Which job run status indicates successful completion?**

A) COMPLETED  
B) TERMINATED  
C) SUCCESS  
D) FINISHED  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) TERMINATED**

Explanation: TERMINATED is the life cycle state for a completed job. The result state will be SUCCESS if it completed successfully, or FAILED if it encountered errors.
</details>

---

## Question 8
**What is the purpose of a shared job cluster?**

A) Share cluster across multiple jobs  
B) Share cluster across tasks in the same job  
C) Share cluster with other users  
D) Share cluster configuration  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Share cluster across tasks in the same job**

Explanation: A shared job cluster (defined in job_clusters) allows multiple tasks within the same job to use the same cluster, saving cluster startup time and costs.
</details>

---

## Question 9
**What happens when a task exceeds its timeout?**

A) It continues running  
B) It's terminated and marked as failed  
C) It's paused  
D) It's automatically retried  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) It's terminated and marked as failed**

Explanation: When a task exceeds its timeout_seconds, it's terminated and marked as failed. If retry_on_timeout is true, it will be retried according to the retry configuration.
</details>

---

## Question 10
**Which parameter controls retry behavior on timeout?**

A) max_retries  
B) retry_on_timeout  
C) timeout_retries  
D) auto_retry  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) retry_on_timeout**

Explanation: retry_on_timeout (boolean) determines whether the task should be retried if it times out. When true, timeouts count toward max_retries.
</details>

---

## Question 11
**What is the format for dynamic job parameters like current date?**

A) ${job.start_time.iso_date}  
B) {{job.start_time.iso_date}}  
C) %job.start_time.iso_date%  
D) @job.start_time.iso_date@  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) {{job.start_time.iso_date}}**

Explanation: Databricks uses double curly braces {{}} for dynamic parameter substitution. Common variables include {{job.start_time.iso_date}}, {{job.run_id}}, and {{job.id}}.
</details>

---

## Question 12
**Which permission level allows triggering job runs but not editing the job?**

A) CAN VIEW  
B) CAN MANAGE RUN  
C) CAN MANAGE  
D) IS OWNER  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) CAN MANAGE RUN**

Explanation: CAN MANAGE RUN allows users to trigger runs and cancel runs, but not edit the job configuration. CAN MANAGE allows editing, and IS OWNER has full control.
</details>

---

## Question 13
**What is the recommended approach for making jobs idempotent?**

A) Use append mode only  
B) Delete existing data for the partition before inserting  
C) Never re-run jobs  
D) Use different table names each time  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Delete existing data for the partition before inserting**

Explanation: Idempotent jobs can be safely re-run. The recommended pattern is to delete existing data for the specific partition/date before inserting new data, or use MERGE for upsert operations.
</details>

---

## Question 14
**Which trigger type processes data once and stops?**

A) trigger(processingTime="once")  
B) trigger(once=True)  
C) trigger(continuous=True)  
D) trigger(manual=True)  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) trigger(once=True)**

Explanation: trigger(once=True) processes all available data in a single batch and then stops. This is useful for incremental batch processing patterns.
</details>

---

## Question 15
**What is the purpose of min_retry_interval_millis?**

A) Minimum time before job starts  
B) Minimum delay between retry attempts  
C) Minimum job duration  
D) Minimum cluster startup time  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Minimum delay between retry attempts**

Explanation: min_retry_interval_millis sets the minimum time to wait before retrying a failed task. This implements exponential backoff, giving transient issues time to resolve.
</details>

---

## Scoring

- **13-15 correct (87-100%)**: Excellent! You've mastered Databricks Jobs
- **10-12 correct (67-86%)**: Good! Review the topics you missed
- **7-9 correct (47-66%)**: Fair - More practice needed
- **Below 7 (< 47%)**: Review the material and try again

---

**Next**: Day 24 - Databricks Repos & Version Control
