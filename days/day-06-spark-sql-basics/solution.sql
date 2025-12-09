-- Day 6: Spark SQL Basics - Solutions
-- =========================================================

USE day6_sql_practice;

-- =========================================================
-- PART 1: BASIC SELECT STATEMENTS
-- =========================================================

-- Exercise 1: Select All Employees
SELECT * FROM employees;

-- Exercise 2: Select Specific Columns
SELECT 
    employee_id, 
    first_name, 
    last_name, 
    salary 
FROM employees;

-- Exercise 3: Column Aliases
SELECT 
    CONCAT(first_name, ' ', last_name) AS full_name,
    salary * 12 AS annual_salary
FROM employees;

-- Exercise 4: DISTINCT Values
SELECT DISTINCT job_title 
FROM employees
ORDER BY job_title;

-- Exercise 5: WHERE Clause
SELECT * 
FROM employees 
WHERE salary > 80000;

-- Exercise 6: Multiple Conditions
SELECT * 
FROM employees 
WHERE department_id = 1 AND salary > 70000;

-- Exercise 7: IN Operator
SELECT * 
FROM employees 
WHERE job_title IN ('Engineer', 'Senior Engineer', 'Junior Engineer');

-- Exercise 8: LIKE Pattern Matching
SELECT * 
FROM employees 
WHERE first_name LIKE 'J%';

-- Exercise 9: BETWEEN Operator
SELECT * 
FROM employees 
WHERE hire_date BETWEEN '2020-01-01' AND '2020-12-31';

-- Exercise 10: NULL Handling
SELECT * 
FROM employees 
WHERE manager_id IS NULL;

-- Exercise 11: ORDER BY
SELECT * 
FROM employees 
ORDER BY salary DESC;

-- Exercise 12: Multiple Column Sorting
SELECT * 
FROM employees 
ORDER BY department_id ASC, salary DESC;

-- Exercise 13: LIMIT
SELECT * 
FROM employees 
ORDER BY salary DESC 
LIMIT 5;

-- =========================================================
-- PART 2: JOIN OPERATIONS
-- =========================================================

-- Exercise 14: INNER JOIN
SELECT 
    e.employee_id,
    e.first_name,
    e.last_name,
    d.department_name
FROM employees e
INNER JOIN departments d ON e.department_id = d.department_id;

-- Exercise 15: LEFT JOIN
SELECT 
    d.department_name,
    COUNT(e.employee_id) AS employee_count
FROM departments d
LEFT JOIN employees e ON d.department_id = e.department_id
GROUP BY d.department_name;

-- Exercise 16: Multiple JOINs
SELECT 
    e.first_name,
    e.last_name,
    d.department_name,
    l.city
FROM employees e
INNER JOIN departments d ON e.department_id = d.department_id
INNER JOIN locations l ON d.location_id = l.location_id;

-- Exercise 17: SELF JOIN
SELECT 
    e.first_name || ' ' || e.last_name AS employee_name,
    m.first_name || ' ' || m.last_name AS manager_name
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.employee_id;

-- Exercise 18: JOIN with WHERE
SELECT 
    e.first_name,
    e.last_name,
    e.job_title,
    d.department_name
FROM employees e
INNER JOIN departments d ON e.department_id = d.department_id
WHERE d.department_name = 'Engineering';

-- Exercise 19: Customer Orders JOIN
SELECT 
    c.customer_name,
    o.order_id,
    o.order_total
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id;

-- Exercise 20: Complex JOIN
SELECT 
    o.order_id,
    c.customer_name,
    e.first_name || ' ' || e.last_name AS sales_rep_name,
    o.order_total
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
INNER JOIN employees e ON o.employee_id = e.employee_id;

-- =========================================================
-- PART 3: AGGREGATION FUNCTIONS
-- =========================================================

-- Exercise 21: Basic Aggregations
SELECT 
    COUNT(*) AS total_employees,
    AVG(salary) AS average_salary,
    MIN(salary) AS min_salary,
    MAX(salary) AS max_salary
FROM employees;

-- Exercise 22: GROUP BY Single Column
SELECT 
    department_id,
    COUNT(*) AS employee_count
FROM employees
GROUP BY department_id;

-- Exercise 23: GROUP BY with Multiple Aggregates
SELECT 
    department_id,
    COUNT(*) AS employee_count,
    AVG(salary) AS avg_salary,
    SUM(salary) AS total_payroll
FROM employees
GROUP BY department_id;

-- Exercise 24: GROUP BY Multiple Columns
SELECT 
    department_id,
    job_title,
    COUNT(*) AS employee_count
FROM employees
GROUP BY department_id, job_title
ORDER BY department_id, job_title;

-- Exercise 25: HAVING Clause
SELECT 
    department_id,
    COUNT(*) AS employee_count
FROM employees
GROUP BY department_id
HAVING COUNT(*) > 3;

-- Exercise 26: HAVING with Aggregate Condition
SELECT 
    department_id,
    AVG(salary) AS avg_salary
FROM employees
GROUP BY department_id
HAVING AVG(salary) > 75000;

-- Exercise 27: WHERE and HAVING Together
SELECT 
    department_id,
    COUNT(*) AS employee_count,
    AVG(salary) AS avg_salary
FROM employees
WHERE hire_date > '2020-06-01'
GROUP BY department_id
HAVING AVG(salary) > 70000;

-- Exercise 28: Customer Order Statistics
SELECT 
    c.customer_id,
    c.customer_name,
    COUNT(o.order_id) AS total_orders,
    SUM(o.order_total) AS total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY total_spent DESC;

-- =========================================================
-- PART 4: SUBQUERIES
-- =========================================================

-- Exercise 29: Scalar Subquery
SELECT 
    first_name,
    last_name,
    salary
FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees);

-- Exercise 30: IN Subquery
SELECT 
    first_name,
    last_name,
    department_id
FROM employees
WHERE department_id IN (
    SELECT d.department_id
    FROM departments d
    INNER JOIN locations l ON d.location_id = l.location_id
    WHERE l.city = 'San Francisco'
);

-- Exercise 31: NOT IN Subquery
SELECT 
    customer_id,
    customer_name
FROM customers
WHERE customer_id NOT IN (
    SELECT DISTINCT customer_id FROM orders
);

-- Exercise 32: Correlated Subquery
SELECT 
    e1.first_name,
    e1.last_name,
    e1.salary,
    e1.department_id
FROM employees e1
WHERE e1.salary > (
    SELECT AVG(e2.salary)
    FROM employees e2
    WHERE e2.department_id = e1.department_id
);

-- Exercise 33: EXISTS
SELECT 
    department_id,
    department_name
FROM departments d
WHERE EXISTS (
    SELECT 1 
    FROM employees e 
    WHERE e.department_id = d.department_id
);

-- Exercise 34: Subquery in SELECT
SELECT 
    e.first_name,
    e.last_name,
    e.salary,
    e.department_id,
    (SELECT AVG(salary) 
     FROM employees e2 
     WHERE e2.department_id = e.department_id) AS dept_avg_salary
FROM employees e;

-- =========================================================
-- PART 5: COMMON TABLE EXPRESSIONS (CTEs)
-- =========================================================

-- Exercise 35: Basic CTE
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

-- Exercise 36: Multiple CTEs
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
        WHERE avg_salary > 75000
    )
SELECT 
    e.first_name,
    e.last_name,
    e.salary,
    e.department_id
FROM employees e
INNER JOIN high_performing_depts hpd ON e.department_id = hpd.department_id
ORDER BY e.salary DESC;

-- Exercise 37: CTE with JOIN
WITH customer_totals AS (
    SELECT 
        customer_id,
        COUNT(order_id) AS order_count,
        SUM(order_total) AS total_spent
    FROM orders
    GROUP BY customer_id
)
SELECT 
    c.customer_name,
    c.city,
    ct.order_count,
    ct.total_spent
FROM customers c
LEFT JOIN customer_totals ct ON c.customer_id = ct.customer_id
ORDER BY ct.total_spent DESC NULLS LAST;

-- Exercise 38: Nested CTE
WITH 
    employee_dept_info AS (
        SELECT 
            e.employee_id,
            e.first_name,
            e.last_name,
            e.salary,
            e.department_id,
            d.department_name
        FROM employees e
        INNER JOIN departments d ON e.department_id = d.department_id
    ),
    ranked_employees AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) AS salary_rank
        FROM employee_dept_info
    )
SELECT * 
FROM ranked_employees
WHERE salary_rank <= 3
ORDER BY department_id, salary_rank;

-- =========================================================
-- PART 6: SET OPERATIONS
-- =========================================================

-- Exercise 39: UNION
SELECT employee_id, first_name, last_name 
FROM employees 
WHERE department_id = 1
UNION
SELECT employee_id, first_name, last_name 
FROM employees 
WHERE salary > 85000;

-- Exercise 40: UNION ALL
SELECT employee_id, first_name, last_name 
FROM employees 
WHERE department_id = 1
UNION ALL
SELECT employee_id, first_name, last_name 
FROM employees 
WHERE salary > 85000;

-- Exercise 41: INTERSECT
SELECT employee_id, first_name, last_name 
FROM employees 
WHERE department_id = 1
INTERSECT
SELECT employee_id, first_name, last_name 
FROM employees 
WHERE salary > 80000;

-- Exercise 42: EXCEPT
SELECT employee_id, first_name, last_name 
FROM employees 
WHERE department_id = 1
EXCEPT
SELECT employee_id, first_name, last_name 
FROM employees 
WHERE salary > 80000;

-- =========================================================
-- PART 7: ADVANCED QUERIES
-- =========================================================

-- Exercise 43: CASE Expression
SELECT 
    first_name,
    last_name,
    salary,
    CASE 
        WHEN salary < 60000 THEN 'Entry'
        WHEN salary BETWEEN 60000 AND 85000 THEN 'Mid'
        ELSE 'Senior'
    END AS salary_category
FROM employees;

-- Exercise 44: COALESCE
SELECT 
    first_name,
    last_name,
    COALESCE(phone, email, 'No contact') AS contact_info
FROM employees;

-- Exercise 45: Complex Aggregation
WITH total_payroll AS (
    SELECT SUM(salary) AS total FROM employees
)
SELECT 
    d.department_name,
    SUM(e.salary) AS dept_payroll,
    ROUND(SUM(e.salary) * 100.0 / tp.total, 2) AS percentage_of_total
FROM employees e
INNER JOIN departments d ON e.department_id = d.department_id
CROSS JOIN total_payroll tp
GROUP BY d.department_name, tp.total
ORDER BY percentage_of_total DESC;

-- Exercise 46: Date Functions
SELECT 
    first_name,
    last_name,
    hire_date,
    DATEDIFF(CURRENT_DATE(), hire_date) AS days_employed,
    ROUND(DATEDIFF(CURRENT_DATE(), hire_date) / 365.25, 1) AS years_of_service
FROM employees
ORDER BY years_of_service DESC;

-- Exercise 47: String Functions
SELECT 
    first_name,
    last_name,
    LOWER(CONCAT(first_name, '.', last_name, '@company.com')) AS generated_email
FROM employees;

-- Exercise 48: Ranking Query
SELECT 
    first_name,
    last_name,
    department_id,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) AS salary_rank,
    RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS salary_rank_with_ties
FROM employees
ORDER BY department_id, salary_rank;

-- Exercise 49: Sales Performance
WITH sales_rep_performance AS (
    SELECT 
        e.employee_id,
        e.first_name,
        e.last_name,
        COUNT(o.order_id) AS total_orders,
        SUM(o.order_total) AS total_revenue
    FROM employees e
    INNER JOIN orders o ON e.employee_id = o.employee_id
    WHERE e.job_title LIKE '%Sales%'
    GROUP BY e.employee_id, e.first_name, e.last_name
)
SELECT *
FROM sales_rep_performance
ORDER BY total_revenue DESC
LIMIT 3;

-- Exercise 50: Product Analysis
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.stock_quantity
FROM products p
WHERE p.product_id NOT IN (
    SELECT DISTINCT product_id FROM order_items
);

-- =========================================================
-- REFLECTION ANSWERS
-- =========================================================
%md
### Reflection Answers

1. **INNER JOIN vs LEFT JOIN**:
   - INNER JOIN: Returns only rows with matches in both tables
   - LEFT JOIN: Returns all rows from left table, with NULLs for non-matches from right table

2. **WHERE vs HAVING**:
   - WHERE: Filters rows before aggregation
   - HAVING: Filters groups after aggregation
   - Use WHERE for row-level filtering, HAVING for aggregate filtering

3. **UNION vs UNION ALL**:
   - UNION: Combines results and removes duplicates (slower)
   - UNION ALL: Combines results and keeps duplicates (faster)

4. **CTEs vs Subqueries**:
   - CTEs are more readable and maintainable
   - CTEs can be referenced multiple times in the same query
   - CTEs can be recursive
   - Subqueries are inline and can't be reused

5. **SQL Execution Order**:
   - FROM → WHERE → GROUP BY → HAVING → SELECT → DISTINCT → ORDER BY → LIMIT

6. **Find records not in another table**:
   - Use NOT IN, NOT EXISTS, or LEFT JOIN with NULL check
   - Example: `WHERE id NOT IN (SELECT id FROM other_table)`

7. **COUNT(*) vs COUNT(column)**:
   - COUNT(*): Counts all rows including NULLs
   - COUNT(column): Counts non-NULL values in that column

8. **Correlated Subqueries**:
   - Reference columns from outer query
   - Execute once for each row in outer query
   - Can be slower than JOINs but sometimes more readable

9. **CASE Expressions**:
   - Conditional logic in SQL
   - Categorize data, handle NULL values, create derived columns
   - Can be used in SELECT, WHERE, ORDER BY

10. **Table Aliases**:
    - Shorter query syntax
    - Required for self-joins
    - Clarify column sources in multi-table queries
    - Improve readability

-- =========================================================
-- BONUS SOLUTIONS
-- =========================================================

-- Bonus 1: Employee Hierarchy Report
SELECT 
    e.first_name || ' ' || e.last_name AS employee_name,
    m.first_name || ' ' || m.last_name AS manager_name,
    d.department_name,
    l.city,
    e.salary,
    ROW_NUMBER() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS dept_salary_rank
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.employee_id
INNER JOIN departments d ON e.department_id = d.department_id
INNER JOIN locations l ON d.location_id = l.location_id
ORDER BY d.department_name, dept_salary_rank;

-- Bonus 2: Sales Analysis Dashboard
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        COUNT(o.order_id) AS total_orders,
        SUM(o.order_total) AS total_revenue,
        AVG(o.order_total) AS avg_order_value,
        COUNT(DISTINCT oi.product_id) AS unique_products,
        MAX(o.order_date) AS last_order_date,
        DATEDIFF(CURRENT_DATE(), MAX(o.order_date)) AS days_since_last_order
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    GROUP BY c.customer_id, c.customer_name
)
SELECT 
    customer_name,
    total_orders,
    total_revenue,
    ROUND(avg_order_value, 2) AS avg_order_value,
    unique_products,
    last_order_date,
    days_since_last_order
FROM customer_metrics
ORDER BY total_revenue DESC;

-- Bonus 3: Department Budget Analysis
SELECT 
    d.department_name,
    d.budget,
    COALESCE(SUM(e.salary), 0) AS actual_payroll,
    d.budget - COALESCE(SUM(e.salary), 0) AS remaining_budget,
    ROUND(COALESCE(SUM(e.salary), 0) * 100.0 / d.budget, 2) AS budget_used_percentage
FROM departments d
LEFT JOIN employees e ON d.department_id = e.department_id
GROUP BY d.department_id, d.department_name, d.budget
ORDER BY budget_used_percentage DESC;

-- Bonus 4: Product Inventory Alert
SELECT 
    p.product_id,
    p.product_name,
    p.stock_quantity,
    COUNT(DISTINCT oi.order_id) AS recent_orders
FROM products p
INNER JOIN order_items oi ON p.product_id = oi.product_id
INNER JOIN orders o ON oi.order_id = o.order_id
WHERE p.stock_quantity < 50
GROUP BY p.product_id, p.product_name, p.stock_quantity
ORDER BY p.stock_quantity ASC;

-- Bonus 5: Employee Performance Metrics
WITH employee_performance AS (
    SELECT 
        e.employee_id,
        e.first_name || ' ' || e.last_name AS employee_name,
        e.department_id,
        COUNT(o.order_id) AS orders_handled,
        COALESCE(SUM(o.order_total), 0) AS revenue_generated,
        COALESCE(AVG(o.order_total), 0) AS avg_order_value
    FROM employees e
    LEFT JOIN orders o ON e.employee_id = o.employee_id
    GROUP BY e.employee_id, e.first_name, e.last_name, e.department_id
)
SELECT 
    employee_name,
    orders_handled,
    ROUND(revenue_generated, 2) AS revenue_generated,
    ROUND(avg_order_value, 2) AS avg_order_value,
    RANK() OVER (PARTITION BY department_id ORDER BY revenue_generated DESC) AS dept_rank
FROM employee_performance
WHERE orders_handled > 0
ORDER BY revenue_generated DESC;

-- Bonus 6: Recursive CTE (Organizational Hierarchy)
WITH RECURSIVE employee_hierarchy AS (
    -- Base case: top-level managers
    SELECT 
        employee_id,
        first_name || ' ' || last_name AS employee_name,
        job_title,
        manager_id,
        1 AS level,
        CAST(first_name || ' ' || last_name AS STRING) AS hierarchy_path
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- Recursive case
    SELECT 
        e.employee_id,
        e.first_name || ' ' || e.last_name AS employee_name,
        e.job_title,
        e.manager_id,
        eh.level + 1,
        CAST(eh.hierarchy_path || ' > ' || e.first_name || ' ' || e.last_name AS STRING)
    FROM employees e
    INNER JOIN employee_hierarchy eh ON e.manager_id = eh.employee_id
)
SELECT 
    REPEAT('  ', level - 1) || employee_name AS indented_name,
    job_title,
    level,
    hierarchy_path
FROM employee_hierarchy
ORDER BY hierarchy_path;

-- Bonus 7: Complex Business Query
WITH customer_order_months AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT DATE_FORMAT(order_date, 'yyyy-MM')) AS distinct_months,
        SUM(order_total) AS total_spending
    FROM orders
    GROUP BY customer_id
)
SELECT 
    c.customer_name,
    com.distinct_months,
    com.total_spending
FROM customers c
INNER JOIN customer_order_months com ON c.customer_id = com.customer_id
WHERE com.distinct_months >= 2 AND com.total_spending > 30000
ORDER BY com.total_spending DESC;

-- Bonus 8: Gap Analysis
WITH RECURSIVE date_range AS (
    SELECT MIN(order_date) AS date_value FROM orders
    UNION ALL
    SELECT DATE_ADD(date_value, 1)
    FROM date_range
    WHERE date_value < (SELECT MAX(order_date) FROM orders)
)
SELECT dr.date_value AS missing_date
FROM date_range dr
LEFT JOIN orders o ON dr.date_value = o.order_date
WHERE o.order_id IS NULL
ORDER BY dr.date_value;

-- =========================================================
-- KEY LEARNINGS SUMMARY
-- =========================================================
%md
## Key Learnings from Day 6

### SELECT Fundamentals
- Mastered basic SELECT with WHERE, ORDER BY, LIMIT
- Used column aliases for readability
- Applied DISTINCT to remove duplicates
- Filtered with multiple conditions (AND, OR, IN, LIKE, BETWEEN)

### JOIN Operations
- INNER JOIN: Only matching rows
- LEFT JOIN: All from left + matches from right
- RIGHT JOIN: All from right + matches from left
- FULL OUTER JOIN: All from both tables
- SELF JOIN: Join table to itself
- Multiple JOINs: Combine 3+ tables

### Aggregations
- Used COUNT, SUM, AVG, MIN, MAX
- Grouped data with GROUP BY
- Filtered groups with HAVING
- Combined WHERE (row filter) and HAVING (group filter)

### Subqueries
- Scalar: Single value
- Column: Single column, multiple rows
- Table: Multiple columns and rows
- Correlated: Reference outer query
- EXISTS: Check for existence

### CTEs
- More readable than nested subqueries
- Can be referenced multiple times
- Support recursion
- Improve query maintainability

### Set Operations
- UNION: Combine and deduplicate
- UNION ALL: Combine with duplicates (faster)
- INTERSECT: Common rows
- EXCEPT: Rows in first but not second

### Advanced Concepts
- CASE expressions for conditional logic
- Window functions for ranking
- Date and string functions
- Complex multi-table queries

**Next**: Day 7 will review all of Week 1 with a comprehensive project!
