# Day 6 Quiz: Spark SQL Basics

Test your knowledge of Spark SQL fundamentals, JOINs, aggregations, and advanced queries.

---

## Question 1
**What is the difference between INNER JOIN and LEFT JOIN?**

A) INNER JOIN returns all rows from both tables; LEFT JOIN returns only matching rows  
B) INNER JOIN returns only matching rows; LEFT JOIN returns all rows from the left table  
C) They are the same, just different syntax  
D) INNER JOIN is faster than LEFT JOIN  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) INNER JOIN returns only matching rows; LEFT JOIN returns all rows from the left table**

Explanation: INNER JOIN returns only rows where there's a match in both tables. LEFT JOIN returns all rows from the left table and matching rows from the right table, with NULLs for non-matches.
</details>

---

## Question 2
**When should you use HAVING instead of WHERE?**

A) HAVING is used to filter rows before aggregation  
B) HAVING is used to filter groups after aggregation  
C) HAVING and WHERE are interchangeable  
D) HAVING is only used with subqueries  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) HAVING is used to filter groups after aggregation**

Explanation: WHERE filters individual rows before GROUP BY, while HAVING filters groups after aggregation. Use HAVING when you need to filter based on aggregate functions like COUNT, SUM, AVG.
</details>

---

## Question 3
**What does UNION do compared to UNION ALL?**

A) UNION keeps duplicates; UNION ALL removes them  
B) UNION removes duplicates; UNION ALL keeps them  
C) They are the same  
D) UNION is only for numeric columns  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) UNION removes duplicates; UNION ALL keeps them**

Explanation: UNION combines results from multiple queries and removes duplicate rows, while UNION ALL keeps all rows including duplicates. UNION ALL is faster because it doesn't need to check for duplicates.
</details>

---

## Question 4
**In what order does SQL execute clauses?**

A) SELECT → FROM → WHERE → GROUP BY → ORDER BY  
B) FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY  
C) WHERE → FROM → SELECT → GROUP BY → ORDER BY  
D) FROM → SELECT → WHERE → GROUP BY → ORDER BY  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY**

Explanation: SQL execution order is: FROM (identify tables) → WHERE (filter rows) → GROUP BY (group rows) → HAVING (filter groups) → SELECT (select columns) → DISTINCT → ORDER BY (sort) → LIMIT.
</details>

---

## Question 5
**What is the purpose of a Common Table Expression (CTE)?**

A) To create permanent tables  
B) To create temporary named result sets for use in a query  
C) To replace all subqueries  
D) To improve query performance automatically  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To create temporary named result sets for use in a query**

Explanation: CTEs (WITH clause) create temporary named result sets that exist only for the duration of the query. They improve readability, can be referenced multiple times, and support recursion.
</details>

---

## Question 6
**Which query finds employees earning more than their department average?**

A) Use a simple WHERE clause  
B) Use a correlated subquery  
C) Use UNION  
D) Use INTERSECT  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Use a correlated subquery**

Explanation: A correlated subquery references the outer query and executes once per row. Example:
```sql
SELECT * FROM employees e1
WHERE salary > (SELECT AVG(salary) FROM employees e2 WHERE e2.dept_id = e1.dept_id)
```
</details>

---

## Question 7
**What does the INTERSECT operator return?**

A) All rows from both queries  
B) Rows that appear in the first query but not the second  
C) Rows that appear in both queries  
D) Rows with NULL values  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Rows that appear in both queries**

Explanation: INTERSECT returns only rows that exist in both query results. It's equivalent to an INNER JOIN but works on entire result sets rather than specific join conditions.
</details>

---

## Question 8
**What is the difference between COUNT(*) and COUNT(column_name)?**

A) They are identical  
B) COUNT(*) counts all rows; COUNT(column_name) counts non-NULL values  
C) COUNT(column_name) is faster  
D) COUNT(*) only works with numeric columns  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) COUNT(*) counts all rows; COUNT(column_name) counts non-NULL values**

Explanation: COUNT(*) counts all rows including those with NULL values. COUNT(column_name) counts only rows where that specific column is not NULL.
</details>

---

## Question 9
**Which clause would you use to get the top 10 highest salaries?**

A) WHERE salary > 10  
B) HAVING COUNT(*) = 10  
C) ORDER BY salary DESC LIMIT 10  
D) GROUP BY salary LIMIT 10  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) ORDER BY salary DESC LIMIT 10**

Explanation: To get the top N records, use ORDER BY to sort in descending order, then LIMIT to restrict the number of results. ORDER BY must come before LIMIT.
</details>

---

## Question 10
**What does a SELF JOIN do?**

A) Joins a table to itself  
B) Joins all tables in the database  
C) Creates a copy of the table  
D) Optimizes join performance  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) Joins a table to itself**

Explanation: A SELF JOIN joins a table to itself, typically used for hierarchical data like employee-manager relationships. You must use different aliases for each reference to the table.
</details>

---

## Question 11
**Which function returns the first non-NULL value from a list?**

A) IFNULL  
B) COALESCE  
C) NULLIF  
D) ISNULL  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) COALESCE**

Explanation: COALESCE returns the first non-NULL value from a list of expressions. Example: COALESCE(phone, email, 'No contact') returns phone if not NULL, otherwise email, otherwise 'No contact'.
</details>

---

## Question 12
**What is a correlated subquery?**

A) A subquery that runs once for the entire query  
B) A subquery that references columns from the outer query  
C) A subquery in the FROM clause  
D) A subquery that uses UNION  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) A subquery that references columns from the outer query**

Explanation: A correlated subquery references columns from the outer query and executes once for each row processed by the outer query. It creates a dependency between inner and outer queries.
</details>

---

## Question 13
**Which operator finds rows in the first query but NOT in the second?**

A) UNION  
B) INTERSECT  
C) EXCEPT  
D) MINUS  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) EXCEPT**

Explanation: EXCEPT (also called MINUS in some databases) returns rows from the first query that don't appear in the second query. It's useful for finding differences between datasets.
</details>

---

## Question 14
**What is the purpose of the DISTINCT keyword?**

A) To sort results  
B) To remove duplicate rows  
C) To filter NULL values  
D) To join tables  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To remove duplicate rows**

Explanation: DISTINCT removes duplicate rows from the result set, returning only unique rows. It applies to all selected columns. Example: SELECT DISTINCT department FROM employees.
</details>

---

## Question 15
**When using GROUP BY, which aggregate functions can you use?**

A) Only COUNT  
B) COUNT, SUM, AVG, MIN, MAX  
C) Only SUM and AVG  
D) No aggregate functions allowed  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) COUNT, SUM, AVG, MIN, MAX**

Explanation: GROUP BY works with all aggregate functions including COUNT, SUM, AVG, MIN, MAX, STDDEV, VARIANCE, etc. These functions compute values across groups of rows.
</details>

---

## Scoring Guide

- **13-15 correct**: Excellent! You have a strong understanding of Spark SQL.
- **10-12 correct**: Good job! Review the questions you missed.
- **7-9 correct**: Fair. Review the Day 6 materials and try again.
- **Below 7**: Review the Day 6 README and practice more SQL queries.

---

## Key Concepts to Remember

1. **JOIN Types**: INNER (matching only), LEFT (all from left), RIGHT (all from right), FULL (all from both)
2. **WHERE vs HAVING**: WHERE filters rows before aggregation, HAVING filters groups after
3. **UNION vs UNION ALL**: UNION removes duplicates, UNION ALL keeps them (faster)
4. **Execution Order**: FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY → LIMIT
5. **CTEs**: Temporary named result sets using WITH clause
6. **Subqueries**: Scalar (single value), Column (single column), Table (multiple columns), Correlated (references outer query)
7. **Set Operations**: UNION (combine), INTERSECT (common), EXCEPT (difference)
8. **Aggregates**: COUNT, SUM, AVG, MIN, MAX work with GROUP BY
9. **COUNT(*) vs COUNT(col)**: COUNT(*) includes NULLs, COUNT(col) doesn't
10. **DISTINCT**: Removes duplicate rows

---

## Common Exam Scenarios

### Scenario 1: Finding Top Performers
**Question**: Write a query to find the top 5 employees by salary in each department.

**Answer**: Use window functions with PARTITION BY:
```sql
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) as rank
    FROM employees
)
SELECT * FROM ranked WHERE rank <= 5;
```

### Scenario 2: Identifying Gaps
**Question**: Find customers who have never placed an order.

**Answer**: Use NOT IN or LEFT JOIN with NULL check:
```sql
-- Method 1: NOT IN
SELECT * FROM customers WHERE customer_id NOT IN (SELECT customer_id FROM orders);

-- Method 2: LEFT JOIN
SELECT c.* FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id WHERE o.order_id IS NULL;
```

### Scenario 3: Department Statistics
**Question**: Show departments with average salary > 70000 and more than 3 employees.

**Answer**: Use GROUP BY with HAVING:
```sql
SELECT dept_id, COUNT(*) as emp_count, AVG(salary) as avg_sal
FROM employees
GROUP BY dept_id
HAVING AVG(salary) > 70000 AND COUNT(*) > 3;
```

### Scenario 4: Hierarchical Data
**Question**: Show each employee with their manager's name.

**Answer**: Use SELF JOIN:
```sql
SELECT e.name as employee, m.name as manager
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.employee_id;
```

---

## Practice Questions

### Question A
What's the difference between a subquery and a CTE?

<details>
<summary>Click to reveal answer</summary>

**Answer**: 
- **Subquery**: Inline query within another query, can't be referenced multiple times
- **CTE**: Named temporary result set using WITH clause, can be referenced multiple times, more readable, supports recursion

CTEs are generally preferred for complex queries because they improve readability and maintainability.
</details>

### Question B
How do you find duplicate records in a table?

<details>
<summary>Click to reveal answer</summary>

**Answer**: Use GROUP BY with HAVING COUNT(*) > 1:
```sql
SELECT column1, column2, COUNT(*)
FROM table_name
GROUP BY column1, column2
HAVING COUNT(*) > 1;
```

This groups by the columns that define uniqueness and filters for groups with more than one row.
</details>

### Question C
What's the difference between INNER JOIN and CROSS JOIN?

<details>
<summary>Click to reveal answer</summary>

**Answer**:
- **INNER JOIN**: Returns rows where there's a match based on the join condition (ON clause)
- **CROSS JOIN**: Returns the Cartesian product (every row from first table paired with every row from second table), no join condition

CROSS JOIN is rarely used in practice and can produce very large result sets.
</details>

---

## SQL Best Practices

1. **Use explicit JOIN syntax** (INNER JOIN) instead of implicit joins (comma-separated tables)
2. **Always use table aliases** in multi-table queries for clarity
3. **Prefer CTEs over nested subqueries** for complex queries
4. **Use UNION ALL instead of UNION** when duplicates don't matter (faster)
5. **Filter early with WHERE** to reduce data processed
6. **Avoid SELECT *** in production (specify columns)
7. **Use meaningful aliases** for calculated columns
8. **Comment complex queries** for maintainability
9. **Test subqueries independently** before combining
10. **Use EXPLAIN** to understand query execution plans

---

## Next Steps

- Review any questions you got wrong
- Re-read the relevant sections in the Day 6 README
- Practice writing complex queries with multiple JOINs
- Experiment with CTEs and subqueries
- Try the bonus challenges in exercise.sql
- Move on to Day 7: Week 1 Review

Good luck! 🚀
