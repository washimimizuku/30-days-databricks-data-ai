-- Day 1: Databricks Architecture & Workspace - Exercises
-- =========================================================
-- Note: These exercises focus on workspace navigation and basic SQL
-- You'll need a cluster starting from Day 2 to run most queries

-- Exercise 1: Markdown Documentation
-- Create a markdown cell with the following content:
-- %md
-- # Day 1 Exercises
-- ## Databricks Architecture
-- 
-- **Control Plane**: Managed by Databricks
-- **Data Plane**: Runs in your cloud account

-- Exercise 2: Simple SELECT Statement
-- Write a query that returns your name and today's date
-- TODO: Replace 'Your Name' with your actual name

SELECT 
    'Your Name' as student_name,
    current_date() as today;

-- Exercise 3: Basic Calculations
-- Write a query that performs basic arithmetic

SELECT 
    10 + 5 as addition,
    10 - 5 as subtraction,
    10 * 5 as multiplication,
    10 / 5 as division;

-- Exercise 4: String Functions
-- Write a query that demonstrates string manipulation

SELECT 
    'databricks' as original,
    upper('databricks') as uppercase,
    length('databricks') as string_length,
    concat('Hello ', 'Databricks!') as concatenated;

-- Exercise 5: Date Functions
-- Write a query that works with dates

SELECT 
    current_date() as today,
    current_timestamp() as now,
    date_add(current_date(), 7) as next_week,
    date_sub(current_date(), 7) as last_week;

-- Exercise 6: Create a Simple Dataset
-- Create an inline dataset using VALUES

SELECT * FROM VALUES
    (1, 'Alice', 'Data Engineer', 95000),
    (2, 'Bob', 'Data Analyst', 75000),
    (3, 'Charlie', 'Data Scientist', 105000),
    (4, 'Diana', 'ML Engineer', 110000)
AS employees(id, name, role, salary);

-- Exercise 7: Filter Data
-- Using the dataset from Exercise 6, filter for high earners
-- TODO: Write a query that shows only employees with salary > 90000

-- YOUR CODE HERE


-- Exercise 8: Aggregate Functions
-- Using the dataset from Exercise 6, calculate statistics
-- TODO: Write a query that shows:
-- - Total number of employees
-- - Average salary
-- - Minimum salary
-- - Maximum salary

-- YOUR CODE HERE


-- Exercise 9: Magic Commands Practice
-- In separate cells, try these magic commands:

-- %md
-- # This is a markdown cell
-- This cell contains **formatted text**

-- %python
-- print("Hello from Python!")
-- print(f"Spark version: {spark.version}")

-- %sql
-- SELECT "Hello from SQL!" as greeting

-- Exercise 10: Workspace Organization Challenge
-- Create the following folder structure in your workspace:
-- 
-- 30-days-bootcamp/
-- ├── week-1/
-- │   ├── day-01/
-- │   ├── day-02/
-- │   └── ...
-- ├── week-2/
-- ├── week-3/
-- └── week-4/
--
-- Then create a notebook in day-01 folder with today's exercises

-- Exercise 11: Explore Databricks Datasets
-- List available sample datasets (requires cluster)
-- Uncomment and run when you have a cluster:

-- %python
-- dbutils.fs.ls("/databricks-datasets/")

-- Exercise 12: System Information
-- Get information about your Databricks environment
-- Uncomment and run when you have a cluster:

-- SELECT 
--     current_database() as current_db,
--     current_user() as current_user,
--     current_version() as spark_version;

-- =========================================================
-- REFLECTION QUESTIONS
-- =========================================================
-- Answer these in a markdown cell:
--
-- 1. What is the difference between control plane and data plane?
-- 2. What are the main components of the Databricks workspace?
-- 3. What magic commands are available in Databricks notebooks?
-- 4. What are the limitations of Community Edition?
-- 5. How do you organize notebooks in the workspace?

-- =========================================================
-- BONUS CHALLENGES
-- =========================================================

-- Bonus 1: Create a more complex inline dataset
-- Create a dataset with at least 10 rows and 5 columns
-- Include different data types (strings, numbers, dates)

-- YOUR CODE HERE


-- Bonus 2: Nested SELECT
-- Write a query that uses a subquery to find employees
-- with above-average salaries

-- YOUR CODE HERE


-- Bonus 3: CASE Statement
-- Write a query that categorizes employees by salary range:
-- - 'Entry': < 80000
-- - 'Mid': 80000-100000
-- - 'Senior': > 100000

-- YOUR CODE HERE


-- =========================================================
-- NOTES
-- =========================================================
-- - Save this notebook in your workspace
-- - You can run these queries once you create a cluster (Day 2)
-- - Practice using keyboard shortcuts:
--   - Shift + Enter: Run cell and move to next
--   - Ctrl/Cmd + Enter: Run cell and stay
--   - B: Create new cell below
--   - A: Create new cell above
--   - DD: Delete cell
