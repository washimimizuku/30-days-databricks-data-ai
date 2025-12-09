-- Day 6: Spark SQL Basics - Exercises
-- =========================================================

-- Make sure you've completed setup.md first!
USE day6_sql_practice;

-- =========================================================
-- PART 1: BASIC SELECT STATEMENTS
-- =========================================================

-- Exercise 1: Select All Employees
-- TODO: Select all columns from employees table

-- YOUR CODE HERE


-- Exercise 2: Select Specific Columns
-- TODO: Select only employee_id, first_name, last_name, and salary

-- YOUR CODE HERE


-- Exercise 3: Column Aliases
-- TODO: Select first_name and last_name as full_name (concatenated),
-- and salary * 12 as annual_salary

-- YOUR CODE HERE


-- Exercise 4: DISTINCT Values
-- TODO: Find all unique job titles

-- YOUR CODE HERE


-- Exercise 5: WHERE Clause
-- TODO: Find all employees with salary greater than 80000

-- YOUR CODE HERE


-- Exercise 6: Multiple Conditions
-- TODO: Find employees in department_id 1 with salary > 70000

-- YOUR CODE HERE


-- Exercise 7: IN Operator
-- TODO: Find employees with job titles 'Engineer', 'Senior Engineer', or 'Junior Engineer'

-- YOUR CODE HERE


-- Exercise 8: LIKE Pattern Matching
-- TODO: Find employees whose first name starts with 'J'

-- YOUR CODE HERE


-- Exercise 9: BETWEEN Operator
-- TODO: Find employees hired between '2020-01-01' and '2020-12-31'

-- YOUR CODE HERE


-- Exercise 10: NULL Handling
-- TODO: Find all employees who are top-level managers (manager_id IS NULL)

-- YOUR CODE HERE


-- Exercise 11: ORDER BY
-- TODO: List all employees ordered by salary descending

-- YOUR CODE HERE


-- Exercise 12: Multiple Column Sorting
-- TODO: Order employees by department_id ascending, then salary descending

-- YOUR CODE HERE


-- Exercise 13: LIMIT
-- TODO: Find the top 5 highest paid employees

-- YOUR CODE HERE


-- =========================================================
-- PART 2: JOIN OPERATIONS
-- =========================================================

-- Exercise 14: INNER JOIN
-- TODO: Join employees and departments to show employee names with department names

-- YOUR CODE HERE


-- Exercise 15: LEFT JOIN
-- TODO: Show all departments and count of employees (include departments with 0 employees)

-- YOUR CODE HERE


-- Exercise 16: Multiple JOINs
-- TODO: Join employees, departments, and locations to show employee name, 
-- department name, and city

-- YOUR CODE HERE


-- Exercise 17: SELF JOIN
-- TODO: Show each employee with their manager's name

-- YOUR CODE HERE


-- Exercise 18: JOIN with WHERE
-- TODO: Find all employees in the 'Engineering' department with their department info

-- YOUR CODE HERE


-- Exercise 19: Customer Orders JOIN
-- TODO: Join customers and orders to show customer names with their order totals

-- YOUR CODE HERE


-- Exercise 20: Complex JOIN
-- TODO: Join orders, customers, and employees to show order_id, customer_name,
-- and sales rep name (employee who handled the order)

-- YOUR CODE HERE


-- =========================================================
-- PART 3: AGGREGATION FUNCTIONS
-- =========================================================

-- Exercise 21: Basic Aggregations
-- TODO: Calculate total employees, average salary, min salary, and max salary

-- YOUR CODE HERE


-- Exercise 22: GROUP BY Single Column
-- TODO: Count employees by department_id

-- YOUR CODE HERE


-- Exercise 23: GROUP BY with Multiple Aggregates
-- TODO: For each department, show count of employees, average salary, and total payroll

-- YOUR CODE HERE


-- Exercise 24: GROUP BY Multiple Columns
-- TODO: Count employees by department_id and job_title

-- YOUR CODE HERE


-- Exercise 25: HAVING Clause
-- TODO: Find departments with more than 3 employees

-- YOUR CODE HERE


-- Exercise 26: HAVING with Aggregate Condition
-- TODO: Find departments where average salary is greater than 75000

-- YOUR CODE HERE


-- Exercise 27: WHERE and HAVING Together
-- TODO: For employees hired after 2020-06-01, find departments with 
-- average salary > 70000

-- YOUR CODE HERE


-- Exercise 28: Customer Order Statistics
-- TODO: For each customer, calculate total orders and total amount spent

-- YOUR CODE HERE


-- =========================================================
-- PART 4: SUBQUERIES
-- =========================================================

-- Exercise 29: Scalar Subquery
-- TODO: Find employees earning more than the average salary

-- YOUR CODE HERE


-- Exercise 30: IN Subquery
-- TODO: Find employees working in departments located in 'San Francisco'

-- YOUR CODE HERE


-- Exercise 31: NOT IN Subquery
-- TODO: Find customers who have never placed an order

-- YOUR CODE HERE


-- Exercise 32: Correlated Subquery
-- TODO: Find employees earning more than the average salary in their department

-- YOUR CODE HERE


-- Exercise 33: EXISTS
-- TODO: Find departments that have at least one employee

-- YOUR CODE HERE


-- Exercise 34: Subquery in SELECT
-- TODO: Show each employee with the average salary of their department

-- YOUR CODE HERE


-- =========================================================
-- PART 5: COMMON TABLE EXPRESSIONS (CTEs)
-- =========================================================

-- Exercise 35: Basic CTE
-- TODO: Create a CTE for high earners (salary > 80000) and count them by department

-- YOUR CODE HERE


-- Exercise 36: Multiple CTEs
-- TODO: Create two CTEs: one for department stats (avg salary, count),
-- another for high-performing departments (avg > 75000), then join with employees

-- YOUR CODE HERE


-- Exercise 37: CTE with JOIN
-- TODO: Create a CTE for customer order totals, then join with customers table

-- YOUR CODE HERE


-- Exercise 38: Nested CTE
-- TODO: Create a CTE for employee salaries with department info,
-- then another CTE to rank them by salary within department

-- YOUR CODE HERE


-- =========================================================
-- PART 6: SET OPERATIONS
-- =========================================================

-- Exercise 39: UNION
-- TODO: Combine employees from department 1 and employees earning > 85000
-- (remove duplicates)

-- YOUR CODE HERE


-- Exercise 40: UNION ALL
-- TODO: Same as above but keep duplicates

-- YOUR CODE HERE


-- Exercise 41: INTERSECT
-- TODO: Find employees who are both in department 1 AND earning > 80000

-- YOUR CODE HERE


-- Exercise 42: EXCEPT
-- TODO: Find employees in department 1 who are NOT earning > 80000

-- YOUR CODE HERE


-- =========================================================
-- PART 7: ADVANCED QUERIES
-- =========================================================

-- Exercise 43: CASE Expression
-- TODO: Categorize employees as 'Entry' (<60000), 'Mid' (60000-85000), 
-- or 'Senior' (>85000) based on salary

-- YOUR CODE HERE


-- Exercise 44: COALESCE
-- TODO: Show employee contact info, preferring phone over email, 
-- with 'No contact' as default

-- YOUR CODE HERE


-- Exercise 45: Complex Aggregation
-- TODO: Calculate the percentage of total payroll each department represents

-- YOUR CODE HERE


-- Exercise 46: Date Functions
-- TODO: Calculate years of service for each employee (from hire_date to current_date)

-- YOUR CODE HERE


-- Exercise 47: String Functions
-- TODO: Create email addresses by concatenating first_name, last_name, and '@company.com'
-- (lowercase, no spaces)

-- YOUR CODE HERE


-- Exercise 48: Ranking Query
-- TODO: Rank employees by salary within each department (use ROW_NUMBER or RANK)

-- YOUR CODE HERE


-- Exercise 49: Sales Performance
-- TODO: Find the top 3 sales reps by total order value

-- YOUR CODE HERE


-- Exercise 50: Product Analysis
-- TODO: Find products that have never been ordered

-- YOUR CODE HERE


-- =========================================================
-- REFLECTION QUESTIONS
-- =========================================================
-- Answer these in a markdown cell:
--
-- 1. What's the difference between INNER JOIN and LEFT JOIN?
-- 2. When should you use WHERE vs HAVING?
-- 3. What does UNION do vs UNION ALL?
-- 4. What's the advantage of CTEs over subqueries?
-- 5. In what order does SQL execute clauses?
-- 6. How do you find records in one table but not in another?
-- 7. What's the difference between COUNT(*) and COUNT(column)?
-- 8. How do correlated subqueries work?
-- 9. When should you use CASE expressions?
-- 10. What's the purpose of table aliases?


-- =========================================================
-- BONUS CHALLENGES
-- =========================================================

-- Bonus 1: Employee Hierarchy Report
-- TODO: Create a report showing each employee with their manager's name,
-- department name, location city, and salary rank within department

-- YOUR CODE HERE


-- Bonus 2: Sales Analysis Dashboard
-- TODO: Create a comprehensive sales report showing:
-- - Total orders and revenue by customer
-- - Average order value
-- - Number of different products ordered
-- - Days since last order

-- YOUR CODE HERE


-- Bonus 3: Department Budget Analysis
-- TODO: Compare actual payroll vs budget for each department,
-- showing percentage used and remaining budget

-- YOUR CODE HERE


-- Bonus 4: Product Inventory Alert
-- TODO: Find products with stock below 50 units that have been ordered
-- in the last 30 days (simulate with any orders)

-- YOUR CODE HERE


-- Bonus 5: Employee Performance Metrics
-- TODO: Create a report showing for each employee:
-- - Number of orders handled
-- - Total revenue generated
-- - Average order value
-- - Rank among peers in same department

-- YOUR CODE HERE


-- Bonus 6: Recursive CTE (Advanced)
-- TODO: Create an organizational hierarchy showing all employees
-- under each manager with their level in the hierarchy

-- YOUR CODE HERE


-- Bonus 7: Complex Business Query
-- TODO: Find customers who have placed orders in at least 2 different months
-- and have total spending > 30000

-- YOUR CODE HERE


-- Bonus 8: Gap Analysis
-- TODO: Find date gaps in orders (dates where no orders were placed)
-- between the first and last order date

-- YOUR CODE HERE


-- =========================================================
-- CLEANUP (Optional)
-- =========================================================

-- Uncomment to clean up
-- DROP DATABASE IF EXISTS day6_sql_practice CASCADE;


-- =========================================================
-- KEY LEARNINGS CHECKLIST
-- =========================================================
-- [ ] Mastered basic SELECT statements
-- [ ] Understood all JOIN types
-- [ ] Used GROUP BY and HAVING effectively
-- [ ] Created and used subqueries
-- [ ] Wrote complex queries with CTEs
-- [ ] Applied set operations
-- [ ] Used CASE expressions
-- [ ] Worked with date and string functions
-- [ ] Understood SQL execution order
-- [ ] Combined multiple concepts in complex queries

