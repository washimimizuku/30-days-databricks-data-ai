-- Day 1: Databricks Architecture & Workspace - Solutions
-- =========================================================

-- Exercise 1: Markdown Documentation
%md
# Day 1 Exercises
## Databricks Architecture

**Control Plane**: Managed by Databricks
**Data Plane**: Runs in your cloud account

-- Exercise 2: Simple SELECT Statement
SELECT 
    'John Doe' as student_name,
    current_date() as today;

-- Exercise 3: Basic Calculations
SELECT 
    10 + 5 as addition,
    10 - 5 as subtraction,
    10 * 5 as multiplication,
    10 / 5 as division;

-- Exercise 4: String Functions
SELECT 
    'databricks' as original,
    upper('databricks') as uppercase,
    length('databricks') as string_length,
    concat('Hello ', 'Databricks!') as concatenated;

-- Exercise 5: Date Functions
SELECT 
    current_date() as today,
    current_timestamp() as now,
    date_add(current_date(), 7) as next_week,
    date_sub(current_date(), 7) as last_week;

-- Exercise 6: Create a Simple Dataset
SELECT * FROM VALUES
    (1, 'Alice', 'Data Engineer', 95000),
    (2, 'Bob', 'Data Analyst', 75000),
    (3, 'Charlie', 'Data Scientist', 105000),
    (4, 'Diana', 'ML Engineer', 110000)
AS employees(id, name, role, salary);

-- Exercise 7: Filter Data
SELECT * FROM VALUES
    (1, 'Alice', 'Data Engineer', 95000),
    (2, 'Bob', 'Data Analyst', 75000),
    (3, 'Charlie', 'Data Scientist', 105000),
    (4, 'Diana', 'ML Engineer', 110000)
AS employees(id, name, role, salary)
WHERE salary > 90000;

-- Exercise 8: Aggregate Functions
SELECT 
    COUNT(*) as total_employees,
    AVG(salary) as average_salary,
    MIN(salary) as minimum_salary,
    MAX(salary) as maximum_salary
FROM VALUES
    (1, 'Alice', 'Data Engineer', 95000),
    (2, 'Bob', 'Data Analyst', 75000),
    (3, 'Charlie', 'Data Scientist', 105000),
    (4, 'Diana', 'ML Engineer', 110000)
AS employees(id, name, role, salary);

-- Exercise 9: Magic Commands Practice

-- Markdown cell:
%md
# This is a markdown cell
This cell contains **formatted text**

-- Python cell:
%python
print("Hello from Python!")
print(f"Spark version: {spark.version}")

-- SQL cell:
%sql
SELECT "Hello from SQL!" as greeting

-- Exercise 10: Workspace Organization
-- This is done through the UI:
-- 1. Navigate to Workspace
-- 2. Right-click your user folder
-- 3. Create → Folder
-- 4. Repeat for each folder in the structure

-- Exercise 11: Explore Databricks Datasets
%python
display(dbutils.fs.ls("/databricks-datasets/"))

-- Exercise 12: System Information
SELECT 
    current_database() as current_db,
    current_user() as current_user,
    current_version() as spark_version;

-- =========================================================
-- REFLECTION ANSWERS
-- =========================================================
%md
### Reflection Answers

1. **Control Plane vs Data Plane**:
   - Control Plane: Managed by Databricks, hosts UI, notebooks, metadata
   - Data Plane: Runs in your cloud, contains clusters and data

2. **Main Workspace Components**:
   - Workspace: File browser for notebooks and folders
   - Repos: Git integration
   - Compute: Cluster management
   - Workflows: Job orchestration
   - Data: Database and table browser

3. **Magic Commands**:
   - %md: Markdown
   - %sql: SQL
   - %python: Python
   - %scala: Scala
   - %r: R
   - %sh: Shell commands
   - %fs: File system commands

4. **Community Edition Limitations**:
   - Single-node clusters only
   - Single user
   - No RBAC
   - No Unity Catalog
   - Limited compute resources

5. **Organizing Notebooks**:
   - Use folders to group related notebooks
   - Follow a naming convention (e.g., day-XX-topic)
   - Use Repos for version control
   - Keep personal work in user folder
   - Use Shared folder for team collaboration

-- =========================================================
-- BONUS SOLUTIONS
-- =========================================================

-- Bonus 1: Complex inline dataset
SELECT * FROM VALUES
    (1, 'Alice', 'Data Engineer', 95000, '2020-01-15'),
    (2, 'Bob', 'Data Analyst', 75000, '2021-03-20'),
    (3, 'Charlie', 'Data Scientist', 105000, '2019-06-10'),
    (4, 'Diana', 'ML Engineer', 110000, '2020-09-05'),
    (5, 'Eve', 'Data Engineer', 98000, '2021-01-12'),
    (6, 'Frank', 'Data Analyst', 72000, '2022-02-28'),
    (7, 'Grace', 'Data Scientist', 108000, '2019-11-15'),
    (8, 'Henry', 'ML Engineer', 115000, '2020-07-22'),
    (9, 'Iris', 'Data Engineer', 92000, '2021-05-18'),
    (10, 'Jack', 'Data Analyst', 78000, '2022-04-10')
AS employees(id, name, role, salary, hire_date);

-- Bonus 2: Nested SELECT (Subquery)
WITH employee_data AS (
    SELECT * FROM VALUES
        (1, 'Alice', 'Data Engineer', 95000),
        (2, 'Bob', 'Data Analyst', 75000),
        (3, 'Charlie', 'Data Scientist', 105000),
        (4, 'Diana', 'ML Engineer', 110000)
    AS employees(id, name, role, salary)
)
SELECT 
    id,
    name,
    role,
    salary
FROM employee_data
WHERE salary > (SELECT AVG(salary) FROM employee_data);

-- Bonus 3: CASE Statement
SELECT 
    id,
    name,
    role,
    salary,
    CASE 
        WHEN salary < 80000 THEN 'Entry'
        WHEN salary BETWEEN 80000 AND 100000 THEN 'Mid'
        ELSE 'Senior'
    END as salary_category
FROM VALUES
    (1, 'Alice', 'Data Engineer', 95000),
    (2, 'Bob', 'Data Analyst', 75000),
    (3, 'Charlie', 'Data Scientist', 105000),
    (4, 'Diana', 'ML Engineer', 110000)
AS employees(id, name, role, salary);

-- =========================================================
-- ADDITIONAL EXAMPLES
-- =========================================================

-- Example 1: Multiple CTEs
WITH 
    employee_data AS (
        SELECT * FROM VALUES
            (1, 'Alice', 'Engineering', 95000),
            (2, 'Bob', 'Analytics', 75000),
            (3, 'Charlie', 'Engineering', 105000),
            (4, 'Diana', 'Engineering', 110000)
        AS t(id, name, department, salary)
    ),
    dept_stats AS (
        SELECT 
            department,
            COUNT(*) as employee_count,
            AVG(salary) as avg_salary
        FROM employee_data
        GROUP BY department
    )
SELECT 
    e.name,
    e.department,
    e.salary,
    d.avg_salary as dept_avg_salary,
    e.salary - d.avg_salary as salary_diff_from_avg
FROM employee_data e
JOIN dept_stats d ON e.department = d.department;

-- Example 2: Window Functions Preview (covered in detail later)
SELECT 
    id,
    name,
    salary,
    ROW_NUMBER() OVER (ORDER BY salary DESC) as salary_rank,
    AVG(salary) OVER () as overall_avg_salary
FROM VALUES
    (1, 'Alice', 95000),
    (2, 'Bob', 75000),
    (3, 'Charlie', 105000),
    (4, 'Diana', 110000)
AS employees(id, name, salary);

-- =========================================================
-- KEY LEARNINGS
-- =========================================================
%md
## Key Learnings from Day 1

1. **Architecture Understanding**:
   - Control plane handles management
   - Data plane handles processing
   - Separation provides security and scalability

2. **Workspace Navigation**:
   - Organized folder structure is essential
   - Use naming conventions
   - Leverage Repos for version control

3. **Notebook Basics**:
   - Multiple languages in one notebook
   - Magic commands for language switching
   - Markdown for documentation

4. **SQL Fundamentals**:
   - SELECT statements
   - Inline datasets with VALUES
   - Basic functions (string, date, aggregate)
   - Filtering and grouping

5. **Best Practices**:
   - Document your code with markdown
   - Organize notebooks logically
   - Use descriptive names
   - Save work frequently

**Next**: Tomorrow we'll create clusters and start running more complex queries!
