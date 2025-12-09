# Day 1 Quiz: Databricks Architecture & Workspace

Test your knowledge of Databricks architecture and workspace fundamentals.

---

## Question 1
**What is the primary function of the Databricks control plane?**

A) Process and store customer data  
B) Run Spark clusters and execute queries  
C) Manage workspace UI, notebooks, and metadata  
D) Store Delta Lake tables  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Manage workspace UI, notebooks, and metadata**

Explanation: The control plane is managed by Databricks and handles the workspace interface, notebook storage, job configurations, and metadata. The data plane handles actual data processing.
</details>

---

## Question 2
**Where does the Databricks data plane run?**

A) On Databricks-managed servers  
B) In your cloud account (AWS, Azure, or GCP)  
C) On your local machine  
D) In a shared multi-tenant environment  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) In your cloud account (AWS, Azure, or GCP)**

Explanation: The data plane runs in your cloud account, giving you control over your data and compute resources while Databricks manages the control plane.
</details>

---

## Question 3
**Which magic command is used to write markdown in a Databricks notebook?**

A) %markdown  
B) %md  
C) %text  
D) %doc  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) %md**

Explanation: The `%md` magic command is used to create markdown cells in Databricks notebooks for documentation and formatted text.
</details>

---

## Question 4
**What is a key limitation of Databricks Community Edition?**

A) No SQL support  
B) Single-node clusters only  
C) No Python support  
D) Limited to 1 hour sessions  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Single-node clusters only**

Explanation: Community Edition is limited to single-node clusters, single user access, and doesn't include advanced features like RBAC or Unity Catalog. However, it supports all languages and is great for learning.
</details>

---

## Question 5
**Which of the following is NOT a valid magic command in Databricks?**

A) %sql  
B) %python  
C) %java  
D) %scala  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) %java**

Explanation: Databricks supports %sql, %python, %scala, %r, %md, and %sh magic commands, but not %java. While Scala runs on the JVM, Java is not directly supported as a notebook language.
</details>

---

## Question 6
**What is the correct hierarchy for organizing work in Databricks workspace?**

A) Workspace → Notebooks → Folders  
B) Workspace → Folders → Notebooks  
C) Folders → Workspace → Notebooks  
D) Notebooks → Folders → Workspace  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Workspace → Folders → Notebooks**

Explanation: The workspace is the top-level container, within which you create folders to organize your notebooks and other files.
</details>

---

## Question 7
**Which component of Databricks is responsible for executing Spark jobs?**

A) Control plane  
B) Workspace UI  
C) Data plane (clusters)  
D) Notebook server  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Data plane (clusters)**

Explanation: The data plane contains the compute clusters that execute Spark jobs and process data. The control plane manages the configuration but doesn't execute the jobs.
</details>

---

## Question 8
**What happens when you use multiple magic commands in a single notebook?**

A) Only the first magic command works  
B) The notebook will error  
C) Each cell can use a different language  
D) All cells must use the same language  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Each cell can use a different language**

Explanation: Magic commands allow you to switch languages between cells, enabling you to use SQL, Python, Scala, and R in the same notebook.
</details>

---

## Question 9
**Where are Databricks notebooks stored?**

A) On your local machine  
B) In the control plane  
C) In the data plane  
D) In your cloud storage  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) In the control plane**

Explanation: Notebooks are stored in the Databricks control plane, which is managed by Databricks. This allows for easy access, sharing, and version control.
</details>

---

## Question 10
**Which Databricks edition is required for Unity Catalog?**

A) Community Edition  
B) Standard  
C) Premium or Enterprise  
D) All editions  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Premium or Enterprise**

Explanation: Unity Catalog is only available in Premium and Enterprise editions. Community and Standard editions do not include Unity Catalog features.
</details>

---

## Question 11
**What is the purpose of the Repos feature in Databricks?**

A) To store large datasets  
B) To integrate with Git for version control  
C) To create database repositories  
D) To manage cluster configurations  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To integrate with Git for version control**

Explanation: Repos allows you to connect Databricks notebooks to Git repositories (GitHub, GitLab, Bitbucket) for version control and collaboration.
</details>

---

## Question 12
**Which keyboard shortcut runs the current cell and moves to the next one?**

A) Ctrl/Cmd + Enter  
B) Shift + Enter  
C) Alt + Enter  
D) Ctrl/Cmd + R  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Shift + Enter**

Explanation: Shift + Enter runs the current cell and automatically moves to the next cell. Ctrl/Cmd + Enter runs the cell but stays in the same cell.
</details>

---

## Question 13
**What does the %fs magic command do?**

A) Formats SQL queries  
B) Provides file system utilities  
C) Creates functions  
D) Filters data  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Provides file system utilities**

Explanation: The %fs magic command provides shortcuts for file system operations, equivalent to dbutils.fs commands. For example, %fs ls /databricks-datasets/
</details>

---

## Question 14
**In Databricks architecture, what is stored in your cloud storage?**

A) Notebooks and workspace configuration  
B) User credentials and permissions  
C) Data files and Delta Lake tables  
D) Cluster metadata  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Data files and Delta Lake tables**

Explanation: Your actual data, including files and Delta Lake tables, is stored in your cloud storage (S3, ADLS, GCS). The control plane stores notebooks and metadata.
</details>

---

## Question 15
**What is the default language for a new Databricks notebook if not specified?**

A) SQL  
B) Python  
C) Scala  
D) It depends on workspace settings  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) It depends on workspace settings**

Explanation: The default language can be configured in user settings. However, you can always override it when creating a notebook or use magic commands to switch languages within cells.
</details>

---

## Scoring Guide

- **13-15 correct**: Excellent! You have a strong understanding of Databricks architecture.
- **10-12 correct**: Good job! Review the questions you missed.
- **7-9 correct**: Fair. Review the Day 1 materials and try again.
- **Below 7**: Review the Day 1 README and setup materials thoroughly.

---

## Key Concepts to Remember

1. **Control Plane**: Managed by Databricks (UI, notebooks, metadata)
2. **Data Plane**: Runs in your cloud (clusters, data processing)
3. **Magic Commands**: %md, %sql, %python, %scala, %r, %sh, %fs
4. **Workspace Organization**: Workspace → Folders → Notebooks
5. **Community Edition**: Free but limited (single-node, single user)
6. **Repos**: Git integration for version control
7. **Notebooks**: Support multiple languages via magic commands

---

## Next Steps

- Review any questions you got wrong
- Re-read the relevant sections in the Day 1 README
- Practice creating notebooks and using magic commands
- Move on to Day 2: Clusters & Compute

Good luck! 🚀
