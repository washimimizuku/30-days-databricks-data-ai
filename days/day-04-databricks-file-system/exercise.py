# Day 4: Databricks File System (DBFS) - Exercises
# =========================================================
# Complete these exercises to master DBFS and dbutils.fs

# =========================================================
# PART 1: EXPLORING DBFS
# =========================================================

# Exercise 1: List Root Directories
# TODO: List all directories in the DBFS root

# YOUR CODE HERE


# Exercise 2: Explore Sample Datasets
# TODO: List all sample datasets in /databricks-datasets/

# YOUR CODE HERE


# Exercise 3: Find Specific Dataset
# TODO: Find and list the contents of /databricks-datasets/samples/

# YOUR CODE HERE


# Exercise 4: File Information
# TODO: Get detailed information (name, size, modification time) for files in /databricks-datasets/samples/

# YOUR CODE HERE


# Exercise 5: Count Files
# TODO: Count the number of files in /FileStore/day4-exercises/

# YOUR CODE HERE


# =========================================================
# PART 2: FILE OPERATIONS
# =========================================================

# Exercise 6: Create Directory
# TODO: Create a directory called /FileStore/my-workspace/

# YOUR CODE HERE


# Exercise 7: Create Nested Directories
# TODO: Create nested directories /FileStore/projects/2024/data/

# YOUR CODE HERE


# Exercise 8: Copy File
# TODO: Copy employees.csv to /FileStore/my-workspace/

# YOUR CODE HERE


# Exercise 9: Move File
# TODO: Move employees_backup.csv to /FileStore/my-workspace/

# YOUR CODE HERE


# Exercise 10: Rename File
# TODO: Rename employees.csv to staff.csv in /FileStore/my-workspace/

# YOUR CODE HERE


# =========================================================
# PART 3: READING FILES
# =========================================================

# Exercise 11: Read File Head
# TODO: Read the first 500 bytes of employees.csv

# YOUR CODE HERE


# Exercise 12: Read Entire File
# TODO: Read the entire employees.csv file using Python file operations
# Hint: Use /dbfs/ prefix

# YOUR CODE HERE


# Exercise 13: Read CSV with Spark
# TODO: Read employees.csv into a Spark DataFrame

# YOUR CODE HERE


# Exercise 14: Read JSON with Spark
# TODO: Read products.json into a Spark DataFrame

# YOUR CODE HERE


# Exercise 15: Read Multiple Files
# TODO: Read all CSV files from /FileStore/day4-exercises/ using wildcard

# YOUR CODE HERE


# =========================================================
# PART 4: WRITING FILES
# =========================================================

# Exercise 16: Write Text File
# TODO: Create a text file with your name and today's date

# YOUR CODE HERE


# Exercise 17: Write CSV File
# TODO: Create a DataFrame and write it as CSV to /FileStore/my-workspace/

# YOUR CODE HERE


# Exercise 18: Write JSON File
# TODO: Create a DataFrame and write it as JSON to /FileStore/my-workspace/

# YOUR CODE HERE


# Exercise 19: Write Parquet File
# TODO: Create a DataFrame and write it as Parquet to /FileStore/my-workspace/

# YOUR CODE HERE


# Exercise 20: Write Delta Table
# TODO: Create a DataFrame and write it as Delta to /FileStore/my-workspace/delta/

# YOUR CODE HERE


# =========================================================
# PART 5: FILE SYSTEM UTILITIES
# =========================================================

# Exercise 21: Check if File Exists
# TODO: Write a function to check if a file exists in DBFS

def file_exists(path):
    # YOUR CODE HERE
    pass

# Test your function
# print(file_exists("/FileStore/day4-exercises/employees.csv"))


# Exercise 22: Get File Size
# TODO: Write a function to get the size of a file in MB

def get_file_size_mb(path):
    # YOUR CODE HERE
    pass

# Test your function
# print(get_file_size_mb("/FileStore/day4-exercises/employees.csv"))


# Exercise 23: List Files Recursively
# TODO: Write a function to list all files in a directory and subdirectories

def list_files_recursive(path):
    # YOUR CODE HERE
    pass

# Test your function
# list_files_recursive("/FileStore/my-workspace/")


# Exercise 24: Find Files by Extension
# TODO: Write a function to find all files with a specific extension

def find_files_by_extension(path, extension):
    # YOUR CODE HERE
    pass

# Test your function
# find_files_by_extension("/FileStore/day4-exercises/", ".csv")


# Exercise 25: Calculate Directory Size
# TODO: Write a function to calculate total size of all files in a directory

def calculate_directory_size(path):
    # YOUR CODE HERE
    pass

# Test your function
# print(calculate_directory_size("/FileStore/day4-exercises/"))


# =========================================================
# PART 6: WORKING WITH FILESTORE
# =========================================================

# Exercise 26: Upload to FileStore
# TODO: Create a sample dataset and save it to FileStore

# YOUR CODE HERE


# Exercise 27: Generate Public URL
# TODO: Construct the public URL for a file in FileStore
# Format: https://<databricks-instance>/files/<path-after-FileStore>

# YOUR CODE HERE


# Exercise 28: Download from FileStore
# TODO: Read a file from FileStore and display its contents

# YOUR CODE HERE


# =========================================================
# PART 7: PATH OPERATIONS
# =========================================================

# Exercise 29: Convert DBFS Path to Local Path
# TODO: Write a function to convert dbfs:/ path to /dbfs/ path

def dbfs_to_local(dbfs_path):
    # YOUR CODE HERE
    pass

# Test
# print(dbfs_to_local("dbfs:/FileStore/my-file.csv"))


# Exercise 30: Convert Local Path to DBFS Path
# TODO: Write a function to convert /dbfs/ path to dbfs:/ path

def local_to_dbfs(local_path):
    # YOUR CODE HERE
    pass

# Test
# print(local_to_dbfs("/dbfs/FileStore/my-file.csv"))


# Exercise 31: Extract Filename from Path
# TODO: Write a function to extract filename from a full path

def get_filename(path):
    # YOUR CODE HERE
    pass

# Test
# print(get_filename("/FileStore/day4-exercises/employees.csv"))


# Exercise 32: Get Parent Directory
# TODO: Write a function to get the parent directory of a path

def get_parent_directory(path):
    # YOUR CODE HERE
    pass

# Test
# print(get_parent_directory("/FileStore/day4-exercises/employees.csv"))


# =========================================================
# PART 8: DATA PROCESSING PIPELINE
# =========================================================

# Exercise 33: Build ETL Pipeline
# TODO: Create a pipeline that:
# 1. Reads CSV from FileStore
# 2. Transforms data (add a column)
# 3. Writes as Parquet to a new location

# YOUR CODE HERE


# Exercise 34: Process Multiple Files
# TODO: Read all CSV files from a directory, combine them, and write as single Delta table

# YOUR CODE HERE


# Exercise 35: File Format Conversion
# TODO: Convert all CSV files in a directory to Parquet format

# YOUR CODE HERE


# =========================================================
# PART 9: CLEANUP OPERATIONS
# =========================================================

# Exercise 36: Remove Single File
# TODO: Remove a specific file from /FileStore/my-workspace/

# YOUR CODE HERE


# Exercise 37: Remove Directory
# TODO: Remove the entire /FileStore/my-workspace/ directory

# YOUR CODE HERE


# Exercise 38: Clean Temporary Files
# TODO: Remove all files in /tmp/ that are older than 1 day (simulation)

# YOUR CODE HERE


# =========================================================
# PART 10: ADVANCED SCENARIOS
# =========================================================

# Exercise 39: Backup Files
# TODO: Create a backup of all files in /FileStore/day4-exercises/ to /FileStore/backup/

# YOUR CODE HERE


# Exercise 40: File Comparison
# TODO: Compare two CSV files and identify differences

# YOUR CODE HERE


# =========================================================
# REFLECTION QUESTIONS
# =========================================================
# Answer these in a markdown cell:
#
# 1. What is the difference between dbfs:/ and /dbfs/ paths?
# 2. When should you use FileStore vs other DBFS locations?
# 3. What are the benefits of using mount points?
# 4. How does DBFS relate to cloud storage?
# 5. What happens to files in /tmp/ when cluster terminates?
# 6. How do you make a file publicly accessible?
# 7. What is the default location for managed tables?
# 8. How do you recursively copy a directory?
# 9. What file formats can Spark read from DBFS?
# 10. Why is Delta Lake preferred over raw files?


# =========================================================
# BONUS CHALLENGES
# =========================================================

# Bonus 1: File Monitoring System
# TODO: Create a function that monitors a directory and reports:
# - Total number of files
# - Total size
# - File types distribution
# - Largest file

def monitor_directory(path):
    # YOUR CODE HERE
    pass


# Bonus 2: Automated Backup System
# TODO: Create a function that backs up files with timestamp

def backup_with_timestamp(source_path, backup_path):
    # YOUR CODE HERE
    pass


# Bonus 3: File Organization System
# TODO: Create a function that organizes files by extension into subdirectories

def organize_by_extension(source_path):
    # YOUR CODE HERE
    pass


# Bonus 4: Data Lake Structure
# TODO: Create a standard data lake structure (bronze/silver/gold)

def create_data_lake_structure(base_path):
    # YOUR CODE HERE
    pass


# Bonus 5: File Validation System
# TODO: Create a function that validates CSV files (checks headers, row count, etc.)

def validate_csv_file(file_path, expected_columns):
    # YOUR CODE HERE
    pass


# =========================================================
# CLEANUP (Optional)
# =========================================================

# Uncomment to clean up after exercises
# dbutils.fs.rm("/FileStore/day4-exercises/", recurse=True)
# dbutils.fs.rm("/FileStore/my-workspace/", recurse=True)
# dbutils.fs.rm("/FileStore/backup/", recurse=True)


# =========================================================
# KEY LEARNINGS CHECKLIST
# =========================================================
# Mark these as you complete them:
# [ ] Navigated DBFS using dbutils.fs.ls()
# [ ] Created directories with dbutils.fs.mkdirs()
# [ ] Copied files with dbutils.fs.cp()
# [ ] Moved files with dbutils.fs.mv()
# [ ] Removed files with dbutils.fs.rm()
# [ ] Read files with dbutils.fs.head()
# [ ] Understood dbfs:/ vs /dbfs/ paths
# [ ] Worked with FileStore
# [ ] Read/wrote various file formats with Spark
# [ ] Built file processing pipelines
# [ ] Implemented file utility functions
# [ ] Cleaned up temporary files
