# Day 6: Spark SQL Basics

## Learning Objectives

By the end of today, you will:
- Master fundamental SQL SELECT statements
- Understand and use different types of JOINs
- Perform aggregations with GROUP BY
- Write complex queries using CTEs (Common Table Expressions)
- Use subqueries effectively
- Apply set operations (UNION, INTERSECT, EXCEPT)
- Filter and sort data efficiently
- Understand SQL execution order

## Topics Covered

### 1. SELECT Statement Fundamentals

**Basic SELECT**:
```sql
-- Select all columns
SELECT * FROM employees;

-- Select specific columns
SELECT employee_id, name, salary FROM employees;

-- Column aliases
SELECT 
    employee_id AS id,
    name AS employee_name,
    salary * 12 AS annual_salary
FROM employees;

-- DISTINCT values
SELECT DISTINCT department FROM employees;
```

**WHERE Clause**:
```sql
-- Basic filtering
SELECT * FROM employees WHERE salary > 50000;

-- Multiple conditions
SELECT * FROM employees 
WHERE salary > 50000 AND department = 'Engineering';

-- IN operator
SELECT * FROM employees 
WHERE department IN ('Engineering', 'Sales', 'Marketing');

-- LIKE pattern matching
SELECT * FROM employees WHERE name LIKE 'J%';

-- BETWEEN operator
SELECT * FROM employees WHERE salary BETWEEN 50000 AND 100000;

-- NULL handling
SELECT * FROM employees WHERE manager_id IS NULL;
SELECT * FROM employees WHERE manager_id IS NOT NULL;
```

**ORDER BY**:
```sql
-- Ascending order (default)
SELECT * FROM employees ORDER BY salary;

-- Descending order
SELECT * FROM employees ORDER BY salary DESC;

-- Multiple columns
SELECT * FROM employees 
ORDER BY department ASC, salary DESC;

-- Order by alias
SELECT 
    name,
    salary * 12 AS annual_salary
FROM employees
ORDER BY annual_salary DESC;
```

**LIMIT**:
```sql
-- Top 10 highest paid employees
SELECT * FROM employees 
ORDER BY salary DESC 
LIMIT 10;

-- Pagination (LIMIT with OFFSET)
SELECT * FROM employees 
ORDER BY employee_id 
LIMIT 10 OFFSET 20;  -- Skip first 20, get next 10
```

### 2. JOIN Operations

**Types of JOINs**:

**INNER JOIN** (returns only matching rows):
```sql
SELECT 
    e.employee_id,
    e.name,
    d.department_name
FROM employees e
INNER JOIN departments d ON e.department_id = d.department_id;
```

**LEFT JOIN** (all rows from left table):
```sql
SELECT 
    e.employee_id,
    e.name,
    d.department_name
FROM employees e
LEFT JOIN departments d ON e.department_id = d.department_id;
-- Returns all employees, even those without a department
```

**RIGHT JOIN** (all rows from right table):
```sql
SELECT 
    e.employee_id,
    e.name,
    d.department_name
FROM employees e
RIGHT JOIN departments d ON e.department_id = d.department_id;
-- Returns all departments, even those without employees
```

**FULL OUTER JOIN** (all rows from both tables):
```sql
SELECT 
    e.employee_id,
    e.name,
    d.department_name
FROM employees e
FULL OUTER JOIN departments d ON e.department_id = d.department_id;
-- Returns all employees and all departments
```

**CROSS JOIN** (Cartesian product):
```sql
SELECT 
    e.name,
    d.department_name
FROM employees e
CROSS JOIN departments d;
-- Every employee paired with every department
```

**SELF JOIN**:
```sql
-- Find employees and their managers
SELECT 
    e.name AS employee_name,
    m.name AS manager_name
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.employee_id;
```

**Multiple JOINs**:
```sql
SELECT 
    e.name,
    d.department_name,
    l.city,
    l.country
FROM employees e
INNER JOIN departments d ON e.department_id = d.department_id
INNER JOIN locations l ON d.location_id = l.location_id;
```

### 3. Aggregation Functions

**Common Aggregate Functions**:
```sql
SELECT 
    COUNT(*) AS total_employees,
    COUNT(DISTINCT department_id) AS unique_departments,
    SUM(salary) AS total_payroll,
    AVG(salary) AS average_salary,
    MIN(salary) AS minimum_salary,
    MAX(salary) AS maximum_salary,
    STDDEV(salary) AS salary_std_dev
FROM employees;
```

**GROUP BY**:
```sql
-- Group by single column
SELECT 
    department_id,
    COUNT(*) AS employee_count,
    AVG(salary) AS avg_salary
FROM employees
GROUP BY department_id;

-- Group by multiple columns
SELECT 
    department_id,
    job_title,
    COUNT(*) AS employee_count,
    AVG(salary) AS avg_salary
FROM employees
GROUP BY department_id, job_title;
```

**HAVING Clause** (filter after aggregation):
```sql
-- Departments with more than 5 employees
SELECT 
    department_id,
    COUNT(*) AS employee_count
FROM employees
GROUP BY department_id
HAVING COUNT(*) > 5;

-- Departments with average salary > 60000
SELECT 
    department_id,
    AVG(salary) AS avg_salary
FROM employees
GROUP BY department_id
HAVING AVG(salary) > 60000;
```

**WHERE vs HAVING**:
```sql
-- WHERE filters before grouping, HAVING filters after
SELECT 
    department_id,
    AVG(salary) AS avg_salary
FROM employees
WHERE hire_date >= '2020-01-01'  -- Filter rows before grouping
GROUP BY department_id
HAVING AVG(salary) > 60000;      -- Filter groups after aggregation
```

### 4. Subqueries

**Scalar Subquery** (returns single value):
```sql
-- Employees earning more than average
SELECT name, salary
FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees);
```

**Column Subquery** (returns single column):
```sql
-- Employees in departments located in New York
SELECT name, department_id
FROM employees
WHERE department_id IN (
    SELECT department_id 
    FROM departments 
    WHERE location = 'New York'
);
```

**Table Subquery** (returns multiple rows and columns):
```sql
-- Department statistics
SELECT 
    e.department_id,
    e.name,
    e.salary,
    ds.avg_salary
FROM employees e
INNER JOIN (
    SELECT 
        department_id,
        AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department_id
) ds ON e.department_id = ds.department_id;
```

**Correlated Subquery**:
```sql
-- Employees earning more than their department average
SELECT 
    e1.name,
    e1.salary,
    e1.department_id
FROM employees e1
WHERE e1.salary > (
    SELECT AVG(e2.salary)
    FROM employees e2
    WHERE e2.department_id = e1.department_id
);
```

**EXISTS**:
```sql
-- Departments that have employees
SELECT department_name
FROM departments d
WHERE EXISTS (
    SELECT 1 
    FROM employees e 
    WHERE e.department_id = d.department_id
);
```

### 5. Common Table Expressions (CTEs)

**Basic CTE**:
```sql
WITH high_earners AS (
    SELECT * 
    FROM employees 
    WHERE salary > 80000
)
SELECT 
    department_id,
    COUNT(*) AS high_earner_count
FROM high_earners
GROUP BY department_id;
```

**Multiple CTEs**:
```sql
WITH 
    dept_stats AS (
        SELECT 
            department_id,
            AVG(salary) AS avg_salary,
            COUNT(*) AS employee_count
        FROM employees
        GROUP BY department_id
    ),
    high_performing_depts AS (
        SELECT department_id
        FROM dept_stats
        WHERE avg_salary > 70000 AND employee_count > 5
    )
SELECT 
    e.name,
    e.salary,
    e.department_id
FROM employees e
INNER JOIN high_performing_depts hpd 
    ON e.department_id = hpd.department_id;
```

**Recursive CTE** (organizational hierarchy):
```sql
WITH RECURSIVE employee_hierarchy AS (
    -- Base case: top-level managers
    SELECT 
        employee_id,
        name,
        manager_id,
        1 AS level
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- Recursive case: employees reporting to previous level
    SELECT 
        e.employee_id,
        e.name,
        e.manager_id,
        eh.level + 1
    FROM employees e
    INNER JOIN employee_hierarchy eh ON e.manager_id = eh.employee_id
)
SELECT * FROM employee_hierarchy ORDER BY level, name;
```

**CTE vs Subquery**:
- CTEs are more readable
- CTEs can be referenced multiple times
- CTEs can be recursive
- Subqueries are inline and can't be reused

### 6. Set Operations

**UNION** (combines results, removes duplicates):
```sql
SELECT employee_id, name FROM employees WHERE department_id = 1
UNION
SELECT employee_id, name FROM employees WHERE salary > 80000;
```

**UNION ALL** (combines results, keeps duplicates):
```sql
SELECT employee_id, name FROM employees WHERE department_id = 1
UNION ALL
SELECT employee_id, name FROM employees WHERE salary > 80000;
-- Faster than UNION because it doesn't remove duplicates
```

**INTERSECT** (returns common rows):
```sql
SELECT employee_id FROM employees WHERE department_id = 1
INTERSECT
SELECT employee_id FROM employees WHERE salary > 80000;
-- Employees in department 1 AND earning > 80000
```

**EXCEPT** (returns rows in first query but not in second):
```sql
SELECT employee_id FROM employees WHERE department_id = 1
EXCEPT
SELECT employee_id FROM employees WHERE salary > 80000;
-- Employees in department 1 BUT NOT earning > 80000
```

### 7. SQL Execution Order

**Written Order**:
```sql
SELECT column_list
FROM table
WHERE condition
GROUP BY column_list
HAVING condition
ORDER BY column_list
LIMIT n;
```

**Execution Order**:
1. **FROM** - Identify tables
2. **WHERE** - Filter rows
3. **GROUP BY** - Group rows
4. **HAVING** - Filter groups
5. **SELECT** - Select columns
6. **DISTINCT** - Remove duplicates
7. **ORDER BY** - Sort results
8. **LIMIT** - Limit results

**Why This Matters**:
```sql
-- This works because WHERE executes before SELECT
SELECT salary * 12 AS annual_salary
FROM employees
WHERE salary > 50000;  -- Can't use annual_salary here

-- This works because ORDER BY executes after SELECT
SELECT salary * 12 AS annual_salary
FROM employees
ORDER BY annual_salary;  -- Can use annual_salary here
```

### 8. Advanced Filtering

**CASE Expressions**:
```sql
SELECT 
    name,
    salary,
    CASE 
        WHEN salary < 50000 THEN 'Low'
        WHEN salary BETWEEN 50000 AND 80000 THEN 'Medium'
        WHEN salary > 80000 THEN 'High'
        ELSE 'Unknown'
    END AS salary_category
FROM employees;

-- Searched CASE
SELECT 
    name,
    CASE 
        WHEN department_id = 1 THEN 'Engineering'
        WHEN department_id = 2 THEN 'Sales'
        ELSE 'Other'
    END AS department_name
FROM employees;
```

**COALESCE** (return first non-null value):
```sql
SELECT 
    name,
    COALESCE(phone, email, 'No contact') AS contact_info
FROM employees;
```

**NULLIF** (return NULL if values are equal):
```sql
SELECT 
    name,
    NULLIF(department_id, 0) AS department_id
FROM employees;
-- Returns NULL if department_id is 0
```

### 9. String Functions in SQL

```sql
-- Concatenation
SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM employees;

-- String length
SELECT name, LENGTH(name) AS name_length FROM employees;

-- Substring
SELECT SUBSTRING(name, 1, 3) AS initials FROM employees;

-- Upper/Lower case
SELECT UPPER(name), LOWER(email) FROM employees;

-- Trim whitespace
SELECT TRIM(name) FROM employees;

-- Replace
SELECT REPLACE(email, '@old.com', '@new.com') FROM employees;
```

### 10. Date Functions in SQL

```sql
-- Current date/time
SELECT CURRENT_DATE(), CURRENT_TIMESTAMP();

-- Date arithmetic
SELECT 
    hire_date,
    DATE_ADD(hire_date, 90) AS probation_end,
    DATE_SUB(CURRENT_DATE(), hire_date) AS days_employed
FROM employees;

-- Date parts
SELECT 
    hire_date,
    YEAR(hire_date) AS hire_year,
    MONTH(hire_date) AS hire_month,
    DAY(hire_date) AS hire_day,
    DAYOFWEEK(hire_date) AS day_of_week
FROM employees;

-- Date formatting
SELECT DATE_FORMAT(hire_date, 'yyyy-MM-dd') FROM employees;

-- Date difference
SELECT DATEDIFF(CURRENT_DATE(), hire_date) AS days_employed FROM employees;
```

## Hands-On Exercises

See `exercise.sql` for practical exercises covering:
- Basic SELECT statements with filtering and sorting
- All types of JOINs
- Aggregations with GROUP BY and HAVING
- Subqueries (scalar, column, table, correlated)
- CTEs for complex queries
- Set operations
- CASE expressions
- String and date functions

## Key Takeaways

1. **SELECT is the foundation** of all SQL queries
2. **JOINs combine data** from multiple tables (INNER, LEFT, RIGHT, FULL)
3. **GROUP BY aggregates data** by categories
4. **HAVING filters aggregated results** (WHERE filters before aggregation)
5. **CTEs improve readability** and can be reused in the same query
6. **Subqueries enable complex logic** but CTEs are often more readable
7. **Set operations combine query results** (UNION, INTERSECT, EXCEPT)
8. **SQL execution order** differs from written order
9. **CASE expressions** enable conditional logic
10. **Understanding execution order** helps write correct queries

## Exam Tips

- **Know JOIN types** and when to use each (INNER vs LEFT vs RIGHT)
- **Understand WHERE vs HAVING** (before vs after aggregation)
- **Master CTEs** - they appear frequently on the exam
- **Know set operations** (UNION removes duplicates, UNION ALL doesn't)
- **Understand subquery types** (scalar, column, table, correlated)
- **Remember execution order** (FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY)
- **Know aggregate functions** (COUNT, SUM, AVG, MIN, MAX)
- **Understand DISTINCT** (removes duplicates)
- **Know CASE expressions** for conditional logic
- **Practice complex queries** with multiple JOINs and CTEs

## Common Exam Questions

1. What's the difference between INNER JOIN and LEFT JOIN?
2. When do you use WHERE vs HAVING?
3. What does UNION do vs UNION ALL?
4. How do you find duplicates in a table?
5. What's the difference between a subquery and a CTE?
6. In what order does SQL execute clauses?
7. How do you find the top N records?
8. What does INTERSECT return?
9. How do you handle NULL values in comparisons?
10. What's the difference between COUNT(*) and COUNT(column)?

## Best Practices

1. **Use table aliases** for readability (especially with JOINs)
2. **Prefer CTEs over nested subqueries** for complex queries
3. **Use INNER JOIN explicitly** instead of implicit joins
4. **Filter early** with WHERE to reduce data processed
5. **Use UNION ALL** instead of UNION when duplicates don't matter (faster)
6. **Avoid SELECT *** in production (specify columns)
7. **Use meaningful aliases** for calculated columns
8. **Comment complex queries** for maintainability
9. **Test subqueries independently** before combining
10. **Use EXPLAIN** to understand query execution

## Additional Resources

- [Databricks SQL Reference](https://docs.databricks.com/sql/language-manual/index.html)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [SQL Functions Reference](https://docs.databricks.com/sql/language-manual/sql-ref-functions.html)

## Next Steps

Tomorrow: [Day 7 - Week 1 Review](../day-07-week-1-review/README.md)

