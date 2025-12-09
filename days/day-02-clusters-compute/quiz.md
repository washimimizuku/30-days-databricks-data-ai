# Day 2 Quiz: Clusters & Compute

Test your knowledge of Databricks clusters and compute resources.

---

## Question 1
**What is the primary difference between all-purpose and job clusters?**

A) All-purpose clusters are faster than job clusters  
B) Job clusters are automatically created and terminated, while all-purpose clusters have manual lifecycle management  
C) All-purpose clusters can only run SQL, while job clusters support all languages  
D) Job clusters are more expensive than all-purpose clusters  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Job clusters are automatically created and terminated, while all-purpose clusters have manual lifecycle management**

Explanation: Job clusters are created when a job starts and terminated when it completes, making them cost-effective for scheduled workloads. All-purpose clusters are manually started and stopped, suitable for interactive work.
</details>

---

## Question 2
**Which cluster mode provides process-level isolation for multiple users?**

A) Standard mode  
B) Single Node mode  
C) High Concurrency mode  
D) Serverless mode  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) High Concurrency mode**

Explanation: High Concurrency mode provides process-level isolation between users, table access controls, and is designed for shared analytics environments. Standard mode doesn't provide this isolation.
</details>

---

## Question 3
**What is the default auto-termination period for all-purpose clusters?**

A) 30 minutes  
B) 60 minutes  
C) 120 minutes  
D) No auto-termination by default  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) 120 minutes**

Explanation: The default auto-termination period for all-purpose clusters is 120 minutes (2 hours). This can be configured when creating or editing a cluster.
</details>

---

## Question 4
**Which Databricks Runtime type includes pre-installed machine learning libraries like TensorFlow and PyTorch?**

A) Standard Runtime  
B) ML Runtime  
C) Photon Runtime  
D) Light Runtime  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) ML Runtime**

Explanation: ML Runtime includes all Standard Runtime features plus pre-installed ML libraries such as TensorFlow, PyTorch, scikit-learn, and XGBoost.
</details>

---

## Question 5
**What happens to running queries when a cluster is terminated?**

A) Queries continue running in the background  
B) Queries are paused and resume when cluster restarts  
C) Queries are cancelled and fail  
D) Queries are automatically moved to another cluster  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Queries are cancelled and fail**

Explanation: When a cluster terminates, all running queries are cancelled and will fail. Any unsaved results are lost. This is why it's important to save work before terminating a cluster.
</details>

---

## Question 6
**Which node type is best suited for workloads with large datasets requiring significant memory?**

A) General Purpose (m5 family)  
B) Compute Optimized (c5 family)  
C) Memory Optimized (r5 family)  
D) Storage Optimized (i3 family)  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Memory Optimized (r5 family)**

Explanation: Memory Optimized instances (r5 family) have a high memory-to-CPU ratio (8:1) and are ideal for workloads with large datasets, aggregations, and caching requirements.
</details>

---

## Question 7
**What is the recommended Databricks Runtime version for production workloads?**

A) Latest version for newest features  
B) LTS (Long Term Support) version  
C) ML Runtime regardless of workload  
D) Light Runtime for better performance  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) LTS (Long Term Support) version**

Explanation: LTS versions are thoroughly tested, stable, and receive long-term support including bug fixes and security patches. They are recommended for production workloads.
</details>

---

## Question 8
**How long does autoscaling typically wait before scaling down workers during low utilization?**

A) Immediately  
B) 5 minutes  
C) 10 minutes  
D) 30 minutes  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) 10 minutes**

Explanation: Autoscaling waits approximately 10 minutes of low utilization before scaling down workers. This prevents frequent scaling up and down. Scaling up happens immediately when tasks are pending.
</details>

---

## Question 9
**Which languages are supported in High Concurrency mode?**

A) SQL, Python, Scala, and R  
B) SQL and Python only  
C) Python and Scala only  
D) SQL only  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) SQL and Python only**

Explanation: High Concurrency mode supports SQL and Python only. Scala and R are not supported due to the process isolation requirements. Use Standard mode if you need Scala or R.
</details>

---

## Question 10
**What is the maximum recommended number of columns for Z-ordering in Delta Lake optimization?**

A) 2 columns  
B) 4 columns  
C) 8 columns  
D) Unlimited  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) 4 columns**

Explanation: While Z-ordering can technically use more columns, Databricks recommends a maximum of 4 columns for optimal performance. Beyond 4 columns, the benefits diminish significantly.
</details>

---

## Question 11
**Which cluster type should be used for scheduled ETL jobs to minimize costs?**

A) All-purpose cluster with autoscaling  
B) All-purpose cluster with auto-termination  
C) Job cluster  
D) High Concurrency cluster  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Job cluster**

Explanation: Job clusters are created when the job starts and terminated when it completes, so you only pay for the actual job execution time. This makes them the most cost-effective option for scheduled workloads.
</details>

---

## Question 12
**What happens to persistent data (Delta tables, files in DBFS) when a cluster terminates?**

A) All data is deleted  
B) Data is preserved  
C) Data is archived to cold storage  
D) Data is moved to another cluster  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Data is preserved**

Explanation: Persistent data stored in Delta tables and DBFS is preserved when a cluster terminates. Only cluster state (variables, temp views, cached data) is lost.
</details>

---

## Question 13
**Which cluster mode is required to use table access controls (ACLs)?**

A) Standard mode  
B) High Concurrency mode  
C) Single Node mode  
D) Any mode with proper configuration  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) High Concurrency mode**

Explanation: Table access controls (ACLs) are only available in High Concurrency mode. This mode provides the necessary process isolation and security features for table-level permissions.
</details>

---

## Question 14
**What is the typical startup time for a new cluster?**

A) 30-60 seconds  
B) 1-2 minutes  
C) 3-5 minutes  
D) 10-15 minutes  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) 3-5 minutes**

Explanation: A new cluster typically takes 3-5 minutes to start. This includes provisioning instances, installing Databricks Runtime, and initializing Spark. Cluster pools can reduce this to 30-60 seconds.
</details>

---

## Question 15
**Which of the following is NOT a benefit of using spot/preemptible instances?**

A) 50-70% cost savings  
B) Guaranteed availability  
C) Same performance as on-demand instances  
D) Suitable for fault-tolerant workloads  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Guaranteed availability**

Explanation: Spot/preemptible instances can be interrupted by the cloud provider when capacity is needed elsewhere. They offer significant cost savings but do NOT guarantee availability. Use them for fault-tolerant workloads that can handle interruptions.
</details>

---

## Scoring Guide

- **13-15 correct (87-100%)**: Excellent! You've mastered today's content.
- **11-12 correct (73-80%)**: Good job! Minor review recommended.
- **9-10 correct (60-67%)**: Fair. Review the material again.
- **Below 9 (< 60%)**: Re-read Day 2 content thoroughly.

---

## Key Concepts to Remember

1. **Cluster Types**: All-purpose (interactive, manual lifecycle) vs Job (automated, auto-created/terminated)
2. **Cluster Modes**: Standard (full API), High Concurrency (shared, isolated), Single Node (no workers)
3. **Runtime Versions**: LTS for production, ML for machine learning, Photon for SQL optimization
4. **Autoscaling**: Scales up immediately, scales down after 10 minutes of low utilization
5. **Auto-termination**: Default 120 minutes, recommended 15-30 minutes for development
6. **Node Types**: General Purpose (balanced), Memory Optimized (large datasets), Compute Optimized (processing), Storage Optimized (I/O), GPU (ML)
7. **Cost Optimization**: Use job clusters for scheduled work, enable autoscaling, use spot instances
8. **High Concurrency**: Process isolation, table ACLs, SQL/Python only
9. **Data Persistence**: Delta tables and DBFS data preserved, cluster state lost on termination
10. **Cluster Pools**: Reduce startup time from 3-5 minutes to 30-60 seconds

---

## Next Steps

- Review any questions you got wrong
- Re-read relevant sections in Day 2 README
- Practice creating and configuring clusters
- Move on to Day 3: Delta Lake Fundamentals

Good luck! 🚀
