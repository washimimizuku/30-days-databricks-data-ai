# Day 10: Python DataFrames Basics - Exercises
# =========================================================
# Complete these exercises to master PySpark DataFrame basics

# =========================================================
# PART 1: CREATING DATAFRAMES
# =========================================================

# Exercise 1: Create DataFrame from List of Tuples
# TODO: Create a DataFrame with columns: id, name, age
# Data: (1, "Alice", 30), (2, "Bob", 25), (3, "Charlie", 35)

# YOUR CODE HERE


# Exercise 2: Create DataFrame from List of Rows
# TODO: Create a DataFrame using Row objects with: id, product, price
# Data: id=101, product="Laptop", price=999.99 (add 2 more products)

# YOUR CODE HERE


# Exercise 3: Create DataFrame with Explicit Schema
# TODO: Create a DataFrame with explicit schema (id: int, name: string, salary: double)
# Use StructType and StructField

# YOUR CODE HERE


# Exercise 4: Create DataFrame from Dictionary
# TODO: Create a DataFrame from a dictionary with keys: name, age, city

# YOUR CODE HERE


# Exercise 5: Create Empty DataFrame
# TODO: Create an empty DataFrame with schema: id (int), name (string)

# YOUR CODE HERE


# =========================================================
# PART 2: READING DATA
# =========================================================

# Exercise 6: Read CSV with Header
# TODO: Read employees.csv with header and inferred schema

# YOUR CODE HERE


# Exercise 7: Read CSV with Explicit Schema
# TODO: Read employees.csv with explicit schema definition

# YOUR CODE HERE


# Exercise 8: Read JSON File
# TODO: Read products.json file

# YOUR CODE HERE


# Exercise 9: Read Multiple CSV Files
# TODO: Read all CSV files from /FileStore/day10-exercises/ using wildcard

# YOUR CODE HERE


# Exercise 10: Read CSV with Options
# TODO: Read employees.csv with custom delimiter, quote, and null value options

# YOUR CODE HERE


# =========================================================
# PART 3: INSPECTING DATAFRAMES
# =========================================================

# Exercise 11: Display DataFrame Schema
# TODO: Read employees.csv and print its schema

# YOUR CODE HERE


# Exercise 12: Show First Rows
# TODO: Display first 5 rows of employees DataFrame

# YOUR CODE HERE


# Exercise 13: Get DataFrame Dimensions
# TODO: Print the number of rows and columns in employees DataFrame

# YOUR CODE HERE


# Exercise 14: Get Column Names and Types
# TODO: Print all column names and their data types

# YOUR CODE HERE


# Exercise 15: Display Summary Statistics
# TODO: Show summary statistics for employees DataFrame

# YOUR CODE HERE


# =========================================================
# PART 4: SELECTING COLUMNS
# =========================================================

# Exercise 16: Select Single Column
# TODO: Select only the 'name' column from employees

# YOUR CODE HERE


# Exercise 17: Select Multiple Columns
# TODO: Select 'name', 'department', and 'salary' columns

# YOUR CODE HERE


# Exercise 18: Select All Columns
# TODO: Select all columns using "*"

# YOUR CODE HERE


# Exercise 19: Select with Column Expressions
# TODO: Select name and salary+1000 as 'new_salary'

# YOUR CODE HERE


# Exercise 20: Select Distinct Values
# TODO: Select distinct departments from employees

# YOUR CODE HERE


# =========================================================
# PART 5: FILTERING DATA
# =========================================================

# Exercise 21: Filter with Single Condition
# TODO: Filter employees with salary > 80000

# YOUR CODE HERE


# Exercise 22: Filter with Multiple Conditions (AND)
# TODO: Filter employees in Engineering department with salary > 90000

# YOUR CODE HERE


# Exercise 23: Filter with Multiple Conditions (OR)
# TODO: Filter employees in Engineering OR Sales department

# YOUR CODE HERE


# Exercise 24: Filter with NOT Condition
# TODO: Filter employees NOT in Engineering department

# YOUR CODE HERE


# Exercise 25: Filter with String Operations
# TODO: Filter employees whose name starts with 'A'

# YOUR CODE HERE


# Exercise 26: Filter with IN Clause
# TODO: Filter employees in cities: Seattle, New York, Chicago

# YOUR CODE HERE


# Exercise 27: Filter with BETWEEN
# TODO: Filter employees with salary between 75000 and 90000

# YOUR CODE HERE


# Exercise 28: Filter Null Values
# TODO: Filter rows where city is not null

# YOUR CODE HERE


# =========================================================
# PART 6: ADDING AND MODIFYING COLUMNS
# =========================================================

# Exercise 29: Add New Column with Literal Value
# TODO: Add a column 'country' with value 'USA' for all rows

# YOUR CODE HERE


# Exercise 30: Add Calculated Column
# TODO: Add a column 'annual_bonus' as 10% of salary

# YOUR CODE HERE


# Exercise 31: Add Conditional Column
# TODO: Add 'salary_grade' column: 'High' if salary > 85000, else 'Standard'

# YOUR CODE HERE


# Exercise 32: Add Multiple Conditional Column
# TODO: Add 'experience_level' column:
# 'Senior' if hire_date before 2020, 'Mid' if 2020-2021, 'Junior' if after 2021

# YOUR CODE HERE


# Exercise 33: Rename Column
# TODO: Rename 'name' column to 'employee_name'

# YOUR CODE HERE


# Exercise 34: Modify Existing Column
# TODO: Increase all salaries by 5%

# YOUR CODE HERE


# Exercise 35: Drop Columns
# TODO: Drop 'city' and 'hire_date' columns

# YOUR CODE HERE


# =========================================================
# PART 7: AGGREGATIONS
# =========================================================

# Exercise 36: Count Total Rows
# TODO: Count total number of employees

# YOUR CODE HERE


# Exercise 37: Calculate Average
# TODO: Calculate average salary

# YOUR CODE HERE


# Exercise 38: Find Min and Max
# TODO: Find minimum and maximum salary

# YOUR CODE HERE


# Exercise 39: Multiple Aggregations
# TODO: Calculate count, avg, min, max salary in one query

# YOUR CODE HERE


# Exercise 40: Count Distinct
# TODO: Count distinct departments

# YOUR CODE HERE


# =========================================================
# PART 8: GROUPING DATA
# =========================================================

# Exercise 41: Group By Single Column
# TODO: Count employees by department

# YOUR CODE HERE


# Exercise 42: Group By with Average
# TODO: Calculate average salary by department

# YOUR CODE HERE


# Exercise 43: Group By with Multiple Aggregations
# TODO: For each department, show: count, avg_salary, max_salary

# YOUR CODE HERE


# Exercise 44: Group By Multiple Columns
# TODO: Count employees by department and city

# YOUR CODE HERE


# Exercise 45: Group By with Filter
# TODO: Show departments with average salary > 80000

# YOUR CODE HERE


# =========================================================
# PART 9: SORTING DATA
# =========================================================

# Exercise 46: Sort Ascending
# TODO: Sort employees by salary (ascending)

# YOUR CODE HERE


# Exercise 47: Sort Descending
# TODO: Sort employees by salary (descending)

# YOUR CODE HERE


# Exercise 48: Sort by Multiple Columns
# TODO: Sort by department (asc) then salary (desc)

# YOUR CODE HERE


# Exercise 49: Sort with Null Handling
# TODO: Sort by city with nulls first

# YOUR CODE HERE


# =========================================================
# PART 10: REMOVING DUPLICATES
# =========================================================

# Exercise 50: Remove All Duplicates
# TODO: Remove duplicate rows from employees DataFrame

# YOUR CODE HERE


# Exercise 51: Remove Duplicates by Column
# TODO: Remove duplicate departments (keep first occurrence)

# YOUR CODE HERE


# Exercise 52: Remove Duplicates by Multiple Columns
# TODO: Remove duplicates based on department and city

# YOUR CODE HERE


# =========================================================
# PART 11: HANDLING NULL VALUES
# =========================================================

# Exercise 53: Read Customers with Nulls
# TODO: Read customers.csv and display it

# YOUR CODE HERE


# Exercise 54: Drop Rows with Any Null
# TODO: Drop rows that have any null value

# YOUR CODE HERE


# Exercise 55: Drop Rows with All Nulls
# TODO: Drop rows where all values are null

# YOUR CODE HERE


# Exercise 56: Drop Nulls in Specific Columns
# TODO: Drop rows where email or phone is null

# YOUR CODE HERE


# Exercise 57: Fill Nulls with Default Values
# TODO: Fill null emails with 'no-email@company.com' and null phones with '000-0000'

# YOUR CODE HERE


# Exercise 58: Fill Nulls with Column-Specific Values
# TODO: Fill nulls: email='unknown@email.com', phone='N/A', city='Unknown', state='XX'

# YOUR CODE HERE


# =========================================================
# PART 12: WRITING DATA
# =========================================================

# Exercise 59: Write DataFrame as CSV
# TODO: Write employees DataFrame to /FileStore/day10-output/employees_output.csv

# YOUR CODE HERE


# Exercise 60: Write DataFrame as JSON
# TODO: Write employees DataFrame to /FileStore/day10-output/employees_output.json

# YOUR CODE HERE


# Exercise 61: Write DataFrame as Parquet
# TODO: Write employees DataFrame to /FileStore/day10-output/employees_output.parquet

# YOUR CODE HERE


# Exercise 62: Write DataFrame as Delta
# TODO: Write employees DataFrame to /FileStore/day10-output/employees_delta

# YOUR CODE HERE


# Exercise 63: Write with Overwrite Mode
# TODO: Write employees DataFrame with mode='overwrite'

# YOUR CODE HERE


# Exercise 64: Write with Append Mode
# TODO: Append new data to existing employees_output

# YOUR CODE HERE


# Exercise 65: Write with Partitioning
# TODO: Write employees partitioned by department

# YOUR CODE HERE


# Exercise 66: Write as Table
# TODO: Write employees DataFrame as table 'day10_db.employees_table'

# YOUR CODE HERE


# =========================================================
# PART 13: SQL INTEGRATION
# =========================================================

# Exercise 67: Create Temporary View
# TODO: Create a temporary view 'employees_view' from employees DataFrame

# YOUR CODE HERE


# Exercise 68: Query Temporary View
# TODO: Query employees_view using SQL to get Engineering employees

# YOUR CODE HERE


# Exercise 69: Create Global Temporary View
# TODO: Create a global temporary view 'global_employees'

# YOUR CODE HERE


# Exercise 70: Complex SQL Query
# TODO: Use SQL to find average salary by department, ordered by avg_salary desc

# YOUR CODE HERE


# Exercise 71: Join Using SQL
# TODO: Create views for employees and departments, then join them with SQL

# YOUR CODE HERE


# =========================================================
# PART 14: DATAFRAME TRANSFORMATIONS CHAIN
# =========================================================

# Exercise 72: Chain Multiple Transformations
# TODO: Read employees, filter salary > 80000, select name and salary, order by salary desc

# YOUR CODE HERE


# Exercise 73: Complex Transformation Pipeline
# TODO: Read employees, add bonus column (10% of salary), filter bonus > 8000,
#       group by department, show avg bonus by department

# YOUR CODE HERE


# Exercise 74: Data Cleaning Pipeline
# TODO: Read customers, drop nulls, fill remaining nulls, remove duplicates,
#       select specific columns, sort by name

# YOUR CODE HERE


# =========================================================
# PART 15: WORKING WITH DATES
# =========================================================

# Exercise 75: Parse Date Column
# TODO: Read employees and convert hire_date string to date type

# YOUR CODE HERE


# Exercise 76: Extract Year from Date
# TODO: Add a column 'hire_year' extracted from hire_date

# YOUR CODE HERE


# Exercise 77: Calculate Date Difference
# TODO: Calculate days since hire_date (use current_date())

# YOUR CODE HERE


# Exercise 78: Filter by Date Range
# TODO: Filter employees hired in 2020

# YOUR CODE HERE


# =========================================================
# PART 16: STRING OPERATIONS
# =========================================================

# Exercise 79: Convert to Uppercase
# TODO: Convert all names to uppercase

# YOUR CODE HERE


# Exercise 80: Concatenate Strings
# TODO: Create 'full_info' column: "Name: {name}, Dept: {department}"

# YOUR CODE HERE


# Exercise 81: Extract Substring
# TODO: Extract first 3 characters of name as 'name_code'

# YOUR CODE HERE


# Exercise 82: String Length
# TODO: Add column 'name_length' with length of name

# YOUR CODE HERE


# Exercise 83: Trim Whitespace
# TODO: Trim whitespace from all string columns

# YOUR CODE HERE


# =========================================================
# PART 17: NUMERIC OPERATIONS
# =========================================================

# Exercise 84: Round Numbers
# TODO: Round salary to nearest thousand

# YOUR CODE HERE


# Exercise 85: Absolute Value
# TODO: Calculate absolute difference between salary and 85000

# YOUR CODE HERE


# Exercise 86: Mathematical Operations
# TODO: Calculate: (salary * 1.05) + 1000 as 'projected_salary'

# YOUR CODE HERE


# =========================================================
# PART 18: WORKING WITH ARRAYS AND COLLECTIONS
# =========================================================

# Exercise 87: Create Array Column
# TODO: Create an array column 'skills' with sample skills for each employee

# YOUR CODE HERE


# Exercise 88: Array Contains
# TODO: Filter employees whose skills array contains 'Python'

# YOUR CODE HERE


# Exercise 89: Array Size
# TODO: Add column 'skill_count' with size of skills array

# YOUR CODE HERE


# =========================================================
# PART 19: PERFORMANCE AND OPTIMIZATION
# =========================================================

# Exercise 90: Cache DataFrame
# TODO: Cache the employees DataFrame and verify it's cached

# YOUR CODE HERE


# Exercise 91: Unpersist DataFrame
# TODO: Unpersist the cached DataFrame

# YOUR CODE HERE


# Exercise 92: Repartition DataFrame
# TODO: Repartition employees DataFrame into 4 partitions

# YOUR CODE HERE


# Exercise 93: Coalesce Partitions
# TODO: Coalesce employees DataFrame to 1 partition

# YOUR CODE HERE


# =========================================================
# PART 20: ADVANCED SCENARIOS
# =========================================================

# Exercise 94: Pivot Table
# TODO: Create a pivot table showing count of employees by department and city

# YOUR CODE HERE


# Exercise 95: Unpivot Data
# TODO: Transform wide format to long format (melt operation)

# YOUR CODE HERE


# Exercise 96: Sample Data
# TODO: Take a random 50% sample of employees

# YOUR CODE HERE


# Exercise 97: Union DataFrames
# TODO: Create two employee DataFrames and union them

# YOUR CODE HERE


# Exercise 98: Intersect DataFrames
# TODO: Find common rows between two DataFrames

# YOUR CODE HERE


# Exercise 99: Except (Difference)
# TODO: Find rows in df1 that are not in df2

# YOUR CODE HERE


# Exercise 100: Complete ETL Pipeline
# TODO: Build a complete pipeline:
# 1. Read employees and sales data
# 2. Transform both (add calculated columns)
# 3. Join them
# 4. Aggregate results
# 5. Write to Delta table

# YOUR CODE HERE


# =========================================================
# REFLECTION QUESTIONS
# =========================================================
# Answer these in comments or markdown cell:
#
# 1. What is the difference between a transformation and an action?
# 2. Why is lazy evaluation beneficial in Spark?
# 3. When should you use inferSchema vs explicit schema?
# 4. What are the advantages of Delta Lake over Parquet?
# 5. How do you handle null values in DataFrames?
# 6. What is the difference between filter() and where()?
# 7. Why use col() function instead of string column names?
# 8. What happens when you call collect() on a large DataFrame?
# 9. What is the difference between cache() and persist()?
# 10. When should you use repartition() vs coalesce()?


# =========================================================
# BONUS CHALLENGES
# =========================================================

# Bonus 1: Create a Data Quality Report
# TODO: Create a function that generates a data quality report showing:
# - Total rows
# - Null count per column
# - Distinct count per column
# - Min/max for numeric columns

def data_quality_report(df):
    # YOUR CODE HERE
    pass


# Bonus 2: Dynamic Column Selection
# TODO: Create a function that selects columns based on data type

def select_columns_by_type(df, data_type):
    # YOUR CODE HERE
    pass


# Bonus 3: Outlier Detection
# TODO: Create a function that identifies outliers in numeric columns using IQR method

def detect_outliers(df, column_name):
    # YOUR CODE HERE
    pass


# Bonus 4: Data Profiling
# TODO: Create a comprehensive data profiling function

def profile_dataframe(df):
    # YOUR CODE HERE
    pass


# Bonus 5: Custom Aggregation Function
# TODO: Create a custom aggregation that calculates median salary by department

def median_by_group(df, group_col, agg_col):
    # YOUR CODE HERE
    pass


# =========================================================
# CLEANUP (Optional)
# =========================================================

# Uncomment to clean up after exercises
# dbutils.fs.rm("/FileStore/day10-exercises/", recurse=True)
# dbutils.fs.rm("/FileStore/day10-output/", recurse=True)
# spark.sql("DROP DATABASE IF EXISTS day10_db CASCADE")


# =========================================================
# KEY LEARNINGS CHECKLIST
# =========================================================
# Mark these as you complete them:
# [ ] Created DataFrames from various sources
# [ ] Read data in multiple formats (CSV, JSON, Parquet, Delta)
# [ ] Defined and worked with schemas
# [ ] Selected and filtered data
# [ ] Added and modified columns
# [ ] Performed aggregations and grouping
# [ ] Sorted and removed duplicates
# [ ] Handled null values
# [ ] Wrote data in different formats
# [ ] Integrated DataFrames with SQL
# [ ] Chained multiple transformations
# [ ] Worked with dates and strings
# [ ] Understood lazy evaluation
# [ ] Applied performance optimizations
