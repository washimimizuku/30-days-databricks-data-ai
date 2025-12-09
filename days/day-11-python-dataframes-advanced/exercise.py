# Day 11: Python DataFrames Advanced - Exercises
# =========================================================
# Complete these exercises to master advanced DataFrame operations

from pyspark.sql.functions import col, lit, when, broadcast
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead, sum, avg
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType, IntegerType, DoubleType

# Load datasets
df_employees = spark.read.csv("/FileStore/day11-exercises/employees.csv", header=True, inferSchema=True)
df_departments = spark.read.csv("/FileStore/day11-exercises/departments.csv", header=True, inferSchema=True)
df_sales = spark.read.csv("/FileStore/day11-exercises/sales.csv", header=True, inferSchema=True)
df_products = spark.read.csv("/FileStore/day11-exercises/products.csv", header=True, inferSchema=True)
df_orders = spark.read.csv("/FileStore/day11-exercises/orders.csv", header=True, inferSchema=True)
df_customers = spark.read.csv("/FileStore/day11-exercises/customers.csv", header=True, inferSchema=True)
df_timeseries = spark.read.csv("/FileStore/day11-exercises/timeseries.csv", header=True, inferSchema=True)

# =========================================================
# PART 1: INNER JOINS
# =========================================================

# Exercise 1: Simple Inner Join
# TODO: Join employees with departments on department name

# YOUR CODE HERE


# Exercise 2: Inner Join with Column Selection
# TODO: Join employees and departments, select emp_id, name, dept_name, location

# YOUR CODE HERE


# Exercise 3: Inner Join with Filter
# TODO: Join employees and departments, filter for Engineering department only

# YOUR CODE HERE


# Exercise 4: Multiple Column Join
# TODO: Create sample data and join on multiple columns

# YOUR CODE HERE


# Exercise 5: Inner Join with Aggregation
# TODO: Join employees and departments, count employees per location

# YOUR CODE HERE


# =========================================================
# PART 2: LEFT AND RIGHT JOINS
# =========================================================

# Exercise 6: Left Join
# TODO: Left join employees with departments (show all employees)

# YOUR CODE HERE


# Exercise 7: Right Join
# TODO: Right join employees with departments (show all departments)

# YOUR CODE HERE


# Exercise 8: Left Join with Null Check
# TODO: Left join employees with departments, find employees without department match

# YOUR CODE HERE


# Exercise 9: Left Join with Coalesce
# TODO: Left join and use coalesce to handle nulls

# YOUR CODE HERE


# Exercise 10: Right Join with Aggregation
# TODO: Right join and count employees per department (including empty departments)

# YOUR CODE HERE


# =========================================================
# PART 3: OUTER AND CROSS JOINS
# =========================================================

# Exercise 11: Full Outer Join
# TODO: Full outer join employees and departments

# YOUR CODE HERE


# Exercise 12: Cross Join
# TODO: Cross join employees with products (Cartesian product)

# YOUR CODE HERE


# Exercise 13: Cross Join with Filter
# TODO: Cross join and filter to simulate specific matching

# YOUR CODE HERE


# =========================================================
# PART 4: SEMI AND ANTI JOINS
# =========================================================

# Exercise 14: Left Semi Join
# TODO: Find employees who have made sales (using left semi join)

# YOUR CODE HERE


# Exercise 15: Left Anti Join
# TODO: Find employees who have NOT made sales (using left anti join)

# YOUR CODE HERE


# Exercise 16: Semi Join with Condition
# TODO: Find employees in departments with budget > 200000

# YOUR CODE HERE


# Exercise 17: Anti Join for Data Quality
# TODO: Find orders for non-existent products

# YOUR CODE HERE


# =========================================================
# PART 5: COMPLEX JOINS
# =========================================================

# Exercise 18: Self Join
# TODO: Join employees with themselves to find employee-manager pairs

# YOUR CODE HERE


# Exercise 19: Multiple Joins
# TODO: Join employees, departments, and sales in one query

# YOUR CODE HERE


# Exercise 20: Join with Different Column Names
# TODO: Join orders with products (order.product_id = products.product_id)

# YOUR CODE HERE


# Exercise 21: Join with Complex Condition
# TODO: Join with date range condition

# YOUR CODE HERE


# Exercise 22: Broadcast Join
# TODO: Use broadcast hint to join large employees with small departments

# YOUR CODE HERE


# =========================================================
# PART 6: WINDOW FUNCTIONS - RANKING
# =========================================================

# Exercise 23: Row Number
# TODO: Add row number to employees ordered by salary within each department

# YOUR CODE HERE


# Exercise 24: Rank
# TODO: Rank employees by salary within department (with gaps for ties)

# YOUR CODE HERE


# Exercise 25: Dense Rank
# TODO: Dense rank employees by salary within department (no gaps)

# YOUR CODE HERE


# Exercise 26: Ntile
# TODO: Divide employees into 4 salary quartiles within each department

# YOUR CODE HERE


# Exercise 27: Top N per Group
# TODO: Find top 2 highest paid employees in each department

# YOUR CODE HERE


# =========================================================
# PART 7: WINDOW FUNCTIONS - ANALYTIC
# =========================================================

# Exercise 28: Lag Function
# TODO: Add previous employee's salary in the same department

# YOUR CODE HERE


# Exercise 29: Lead Function
# TODO: Add next employee's salary in the same department

# YOUR CODE HERE


# Exercise 30: Salary Difference
# TODO: Calculate difference between current and previous salary

# YOUR CODE HERE


# Exercise 31: First and Last
# TODO: Add first and last salary in each department

# YOUR CODE HERE


# Exercise 32: Lag with Default
# TODO: Use lag with default value for first row

# YOUR CODE HERE


# =========================================================
# PART 8: WINDOW FUNCTIONS - AGGREGATES
# =========================================================

# Exercise 33: Running Total
# TODO: Calculate running total of sales by employee

# YOUR CODE HERE


# Exercise 34: Cumulative Average
# TODO: Calculate cumulative average salary by department

# YOUR CODE HERE


# Exercise 35: Moving Average
# TODO: Calculate 3-day moving average of sales

# YOUR CODE HERE


# Exercise 36: Running Max
# TODO: Calculate running maximum salary by department

# YOUR CODE HERE


# Exercise 37: Percentage of Total
# TODO: Calculate each employee's salary as percentage of department total

# YOUR CODE HERE


# =========================================================
# PART 9: WINDOW FRAMES
# =========================================================

# Exercise 38: Rows Between
# TODO: Calculate sum of current row and 2 preceding rows

# YOUR CODE HERE


# Exercise 39: Range Between
# TODO: Use range-based window frame

# YOUR CODE HERE


# Exercise 40: Unbounded Preceding
# TODO: Calculate running total from start to current row

# YOUR CODE HERE


# Exercise 41: Unbounded Following
# TODO: Calculate total from current row to end

# YOUR CODE HERE


# =========================================================
# PART 10: USER-DEFINED FUNCTIONS (UDFs)
# =========================================================

# Exercise 42: Simple UDF
# TODO: Create UDF to categorize salary (Low < 80k, Medium 80-100k, High > 100k)

# YOUR CODE HERE


# Exercise 43: Lambda UDF
# TODO: Create lambda UDF to calculate bonus (10% of salary)

# YOUR CODE HERE


# Exercise 44: UDF with Multiple Inputs
# TODO: Create UDF that takes name and department, returns formatted string

# YOUR CODE HERE


# Exercise 45: UDF with Conditional Logic
# TODO: Create UDF to determine experience level based on hire date

# YOUR CODE HERE


# Exercise 46: Register UDF for SQL
# TODO: Register UDF and use it in SQL query

# YOUR CODE HERE


# =========================================================
# PART 11: PANDAS UDFs
# =========================================================

# Exercise 47: Scalar Pandas UDF
# TODO: Create Pandas UDF to calculate tax (20% of salary)

# YOUR CODE HERE


# Exercise 48: Pandas UDF with Series
# TODO: Create Pandas UDF for complex calculation

# YOUR CODE HERE


# Exercise 49: Grouped Map Pandas UDF
# TODO: Create grouped map UDF to normalize salaries within department

# YOUR CODE HERE


# =========================================================
# PART 12: BROADCAST VARIABLES
# =========================================================

# Exercise 50: Create Broadcast Variable
# TODO: Create broadcast variable with department code mapping

# YOUR CODE HERE


# Exercise 51: Use Broadcast in UDF
# TODO: Use broadcast variable in UDF to map values

# YOUR CODE HERE


# Exercise 52: Broadcast Join
# TODO: Perform broadcast join with small lookup table

# YOUR CODE HERE


# =========================================================
# PART 13: ARRAY OPERATIONS
# =========================================================

# Exercise 53: Create Array Column
# TODO: Create array of skills for each employee

# YOUR CODE HERE


# Exercise 54: Array Contains
# TODO: Filter employees who have 'Python' skill

# YOUR CODE HERE


# Exercise 55: Array Size
# TODO: Count number of skills per employee

# YOUR CODE HERE


# Exercise 56: Explode Array
# TODO: Explode skills array to one row per skill

# YOUR CODE HERE


# Exercise 57: Collect List
# TODO: Aggregate employee names into array by department

# YOUR CODE HERE


# Exercise 58: Array Distinct
# TODO: Remove duplicates from array column

# YOUR CODE HERE


# =========================================================
# PART 14: STRUCT OPERATIONS
# =========================================================

# Exercise 59: Create Struct
# TODO: Create struct column for employee address

# YOUR CODE HERE


# Exercise 60: Access Struct Fields
# TODO: Select specific fields from struct

# YOUR CODE HERE


# Exercise 61: Expand Struct
# TODO: Expand all struct fields into separate columns

# YOUR CODE HERE


# =========================================================
# PART 15: MAP OPERATIONS
# =========================================================

# Exercise 62: Create Map
# TODO: Create map column with employee attributes

# YOUR CODE HERE


# Exercise 63: Map Keys and Values
# TODO: Extract keys and values from map

# YOUR CODE HERE


# Exercise 64: Explode Map
# TODO: Explode map into key-value pairs

# YOUR CODE HERE


# =========================================================
# PART 16: PIVOT AND UNPIVOT
# =========================================================

# Exercise 65: Pivot Table
# TODO: Pivot sales data by region and product

# YOUR CODE HERE


# Exercise 66: Pivot with Aggregation
# TODO: Pivot and calculate sum of sales

# YOUR CODE HERE


# Exercise 67: Unpivot Data
# TODO: Convert wide format to long format using stack

# YOUR CODE HERE


# =========================================================
# PART 17: CONDITIONAL AGGREGATIONS
# =========================================================

# Exercise 68: Count with Condition
# TODO: Count high earners (>90k) and regular earners by department

# YOUR CODE HERE


# Exercise 69: Sum with Condition
# TODO: Calculate total salary for employees hired after 2020

# YOUR CODE HERE


# Exercise 70: Multiple Conditional Aggregations
# TODO: Calculate various metrics with conditions

# YOUR CODE HERE


# =========================================================
# PART 18: PERFORMANCE OPTIMIZATION
# =========================================================

# Exercise 71: Cache DataFrame
# TODO: Cache frequently used DataFrame

# YOUR CODE HERE


# Exercise 72: Repartition
# TODO: Repartition DataFrame by department

# YOUR CODE HERE


# Exercise 73: Coalesce
# TODO: Reduce number of partitions before writing

# YOUR CODE HERE


# Exercise 74: Persist with Storage Level
# TODO: Persist DataFrame with specific storage level

# YOUR CODE HERE


# =========================================================
# PART 19: COMPLEX TRANSFORMATIONS
# =========================================================

# Exercise 75: Chained Transformations
# TODO: Chain multiple operations (join, filter, aggregate, sort)

# YOUR CODE HERE


# Exercise 76: Nested Aggregations
# TODO: Perform aggregation on aggregated results

# YOUR CODE HERE


# Exercise 77: Complex Window Calculation
# TODO: Combine multiple window functions

# YOUR CODE HERE


# Exercise 78: Multi-level Grouping
# TODO: Group by multiple levels and aggregate

# YOUR CODE HERE


# =========================================================
# PART 20: REAL-WORLD SCENARIOS
# =========================================================

# Exercise 79: Customer Lifetime Value
# TODO: Calculate total order value per customer using joins and aggregations

# YOUR CODE HERE


# Exercise 80: Employee Hierarchy
# TODO: Build employee hierarchy showing all levels using self-joins

# YOUR CODE HERE


# =========================================================
# REFLECTION QUESTIONS
# =========================================================
# Answer these in comments or markdown cell:
#
# 1. When should you use left semi join vs inner join?
# 2. What is the difference between rank() and dense_rank()?
# 3. Why are Pandas UDFs faster than standard UDFs?
# 4. When should you use broadcast joins?
# 5. What is the difference between rowsBetween and rangeBetween?
# 6. How do you handle duplicate column names after joins?
# 7. What are the performance implications of cross joins?
# 8. When should you cache a DataFrame?
# 9. What is the difference between repartition and coalesce?
# 10. How do window functions differ from group by aggregations?


# =========================================================
# BONUS CHALLENGES
# =========================================================

# Bonus 1: Advanced Join Pattern
# TODO: Implement a complex multi-table join with filters and aggregations

def complex_join_analysis():
    # YOUR CODE HERE
    pass


# Bonus 2: Custom Window Function
# TODO: Create a custom ranking system using window functions

def custom_ranking():
    # YOUR CODE HERE
    pass


# Bonus 3: Performance Comparison
# TODO: Compare performance of standard UDF vs Pandas UDF

def compare_udf_performance():
    # YOUR CODE HERE
    pass


# Bonus 4: Data Quality Check
# TODO: Use anti joins to find data quality issues

def data_quality_check():
    # YOUR CODE HERE
    pass


# Bonus 5: Time Series Analysis
# TODO: Perform time series analysis using window functions

def time_series_analysis():
    # YOUR CODE HERE
    pass


# =========================================================
# CLEANUP (Optional)
# =========================================================

# Uncomment to clean up after exercises
# dbutils.fs.rm("/FileStore/day11-exercises/", recurse=True)
# spark.sql("DROP DATABASE IF EXISTS day11_db CASCADE")


# =========================================================
# KEY LEARNINGS CHECKLIST
# =========================================================
# Mark these as you complete them:
# [ ] Performed all types of joins (inner, left, right, outer, semi, anti, cross)
# [ ] Used window functions for ranking and analytics
# [ ] Created and used standard UDFs
# [ ] Created and used Pandas UDFs
# [ ] Worked with broadcast variables
# [ ] Manipulated arrays, structs, and maps
# [ ] Performed pivot and unpivot operations
# [ ] Implemented conditional aggregations
# [ ] Optimized DataFrame operations
# [ ] Built complex multi-step transformations
