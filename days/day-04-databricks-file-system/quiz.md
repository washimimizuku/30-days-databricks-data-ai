# Day 4 Quiz: Databricks File System (DBFS)

Test your knowledge of DBFS, dbutils.fs commands, and file operations.

---

## Question 1
**What is the correct path format for accessing FileStore using dbutils.fs?**

A) /dbfs/FileStore/  
B) dbfs:/FileStore/  
C) filestore:/  
D) /FileStore/ only  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) dbfs:/FileStore/ or D) /FileStore/**

Explanation: Both `dbfs:/FileStore/` and `/FileStore/` work with dbutils.fs commands. The `dbfs:/` prefix is optional but explicit. Never use `/dbfs/` with dbutils.fs - that's for Python file operations.
</details>

---

## Question 2
**Which dbutils.fs command is used to copy a directory recursively?**

A) dbutils.fs.cp("/source/", "/dest/")  
B) dbutils.fs.cp("/source/", "/dest/", recurse=True)  
C) dbutils.fs.copy("/source/", "/dest/", recursive=True)  
D) dbutils.fs.rcopy("/source/", "/dest/")  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) dbutils.fs.cp("/source/", "/dest/", recurse=True)**

Explanation: The `cp` command requires the `recurse=True` parameter to copy directories and their contents recursively. Without it, only files in the top-level directory are copied.
</details>

---

## Question 3
**What is the default location for managed Delta tables in DBFS?**

A) /FileStore/tables/  
B) /user/hive/warehouse/  
C) /mnt/tables/  
D) /databricks-datasets/  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) /user/hive/warehouse/**

Explanation: Managed tables are stored in `/user/hive/warehouse/` by default. The full path is `/user/hive/warehouse/<database>.db/<table>/`. This is the Hive metastore default location.
</details>

---

## Question 4
**Which path format should be used for Python file operations (open, read, write)?**

A) dbfs:/path/to/file  
B) /path/to/file  
C) /dbfs/path/to/file  
D) file://path/to/file  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) /dbfs/path/to/file**

Explanation: Python file operations require the `/dbfs/` prefix to access DBFS files as local file system paths within the cluster. For example: `open("/dbfs/FileStore/file.txt", "r")`.
</details>

---

## Question 5
**What happens to files stored in /tmp/ when a cluster terminates?**

A) Files are permanently deleted  
B) Files are moved to /FileStore/  
C) Files may be deleted (not guaranteed to persist)  
D) Files are automatically backed up  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Files may be deleted (not guaranteed to persist)**

Explanation: The `/tmp/` directory is for temporary files and they may be deleted when the cluster terminates. Never use `/tmp/` for data that needs to persist. Use other DBFS locations for persistent storage.
</details>

---

## Question 6
**How do you make a file in FileStore publicly accessible via web URL?**

A) Set file permissions to public  
B) Files in FileStore are automatically web-accessible  
C) Use dbutils.fs.publish()  
D) Move file to /public/ directory  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Files in FileStore are automatically web-accessible**

Explanation: Files uploaded to `/FileStore/` are automatically accessible via web URLs in the format: `https://<databricks-instance>/files/<path-after-FileStore>`. No additional configuration needed.
</details>

---

## Question 7
**Which command lists files in a directory?**

A) dbutils.fs.list("/path/")  
B) dbutils.fs.ls("/path/")  
C) dbutils.fs.dir("/path/")  
D) dbutils.fs.listdir("/path/")  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) dbutils.fs.ls("/path/")**

Explanation: The correct command is `dbutils.fs.ls()`. It returns a list of FileInfo objects containing name, path, size, and modification time for each file/directory.
</details>

---

## Question 8
**What is the purpose of mount points in DBFS?**

A) To increase storage capacity  
B) To create shortcuts to cloud storage with simplified paths  
C) To backup data automatically  
D) To compress files  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To create shortcuts to cloud storage with simplified paths**

Explanation: Mount points create persistent connections to cloud storage (S3, ADLS, GCS) with simplified paths like `/mnt/my-data` instead of full cloud URLs. They also centralize credential management and improve security.
</details>

---

## Question 9
**Which command reads the first 65536 bytes of a file?**

A) dbutils.fs.read("/path/to/file")  
B) dbutils.fs.head("/path/to/file")  
C) dbutils.fs.cat("/path/to/file")  
D) dbutils.fs.preview("/path/to/file")  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) dbutils.fs.head("/path/to/file")**

Explanation: `dbutils.fs.head()` reads and returns the first 65536 bytes of a file by default. You can specify a different byte count: `dbutils.fs.head("/path/", 1000)`.
</details>

---

## Question 10
**What is DBFS?**

A) A database file system for SQL queries  
B) A distributed file system abstraction over cloud object storage  
C) A backup system for Databricks  
D) A data format like Parquet  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) A distributed file system abstraction over cloud object storage**

Explanation: DBFS (Databricks File System) is an abstraction layer that provides file system semantics over cloud object storage (S3, ADLS, GCS). It makes object storage look and behave like a traditional file system.
</details>

---

## Question 11
**Which location contains sample datasets provided by Databricks?**

A) /samples/  
B) /databricks-datasets/  
C) /FileStore/samples/  
D) /examples/  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) /databricks-datasets/**

Explanation: Databricks provides sample datasets in `/databricks-datasets/` for learning and testing. These datasets are read-only and include various data formats and use cases.
</details>

---

## Question 12
**How do you remove a directory and all its contents?**

A) dbutils.fs.rm("/path/")  
B) dbutils.fs.rm("/path/", recurse=True)  
C) dbutils.fs.rmdir("/path/")  
D) dbutils.fs.delete("/path/", all=True)  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) dbutils.fs.rm("/path/", recurse=True)**

Explanation: To remove a directory and all its contents, use `dbutils.fs.rm()` with `recurse=True`. Without this parameter, the command will fail if the directory is not empty.
</details>

---

## Question 13
**What is the difference between managed and external tables regarding DBFS?**

A) Managed tables are faster than external tables  
B) Managed tables store data in /user/hive/warehouse/, external tables store data in specified location  
C) External tables cannot use DBFS  
D) There is no difference  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Managed tables store data in /user/hive/warehouse/, external tables store data in specified location**

Explanation: Managed tables store data in the default location (`/user/hive/warehouse/`), while external tables store data in a user-specified location. When you DROP a managed table, both metadata and data are deleted. For external tables, only metadata is deleted.
</details>

---

## Question 14
**Which command creates a new directory in DBFS?**

A) dbutils.fs.mkdir("/path/")  
B) dbutils.fs.mkdirs("/path/")  
C) dbutils.fs.createdir("/path/")  
D) dbutils.fs.makedirectory("/path/")  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) dbutils.fs.mkdirs("/path/")**

Explanation: The correct command is `dbutils.fs.mkdirs()`. It creates the directory and any necessary parent directories (like `mkdir -p` in Unix). Note the plural "mkdirs".
</details>

---

## Question 15
**What is the recommended storage format for structured data in DBFS?**

A) CSV files  
B) JSON files  
C) Parquet files  
D) Delta Lake tables  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) Delta Lake tables**

Explanation: Delta Lake is the recommended format for structured data in DBFS because it provides ACID transactions, time travel, schema evolution, and better performance than raw file formats. While Parquet is good, Delta Lake (which uses Parquet) adds critical features for production workloads.
</details>

---

## Scoring Guide

- **13-15 correct (87-100%)**: Excellent! You've mastered DBFS fundamentals.
- **11-12 correct (73-80%)**: Good job! Minor review recommended.
- **9-10 correct (60-67%)**: Fair. Review the material again.
- **Below 9 (< 60%)**: Re-read Day 4 content thoroughly.

---

## Key Concepts to Remember

1. **DBFS** is an abstraction layer over cloud object storage
2. **Path formats**: `dbfs:/` or `/` for dbutils.fs, `/dbfs/` for Python file operations
3. **dbutils.fs commands**: ls, cp, mv, rm, mkdirs, head
4. **FileStore**: `/FileStore/` for web-accessible uploads
5. **Default table location**: `/user/hive/warehouse/`
6. **Temporary storage**: `/tmp/` (may be deleted)
7. **Mount points**: Simplify cloud storage access
8. **recurse=True**: Required for recursive operations
9. **Sample datasets**: `/databricks-datasets/`
10. **Delta Lake**: Recommended for structured data

---

## Common Mistakes to Avoid

1. Using `/dbfs/` with dbutils.fs commands (use `dbfs:/` or `/`)
2. Forgetting `recurse=True` when copying/removing directories
3. Storing persistent data in `/tmp/`
4. Not understanding managed vs external table locations
5. Confusing FileStore with other DBFS locations
6. Using wrong path format for Python file operations
7. Forgetting that FileStore files are publicly accessible

---

## Next Steps

- Review any questions you got wrong
- Re-read relevant sections in Day 4 README
- Practice dbutils.fs commands in your notebook
- Experiment with different file formats
- Move on to Day 5: Databases, Tables, and Views

Good luck! 🚀
