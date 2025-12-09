# Day 8: Advanced SQL - Window Functions

## Welcome to Week 2!

Congratulations on completing Week 1! This week, we'll dive deeper into advanced SQL techniques and Python DataFrames. Today's focus is on **Window Functions** - one of the most powerful features in SQL for analytics and data engineering.

## Learning Objectives

By the end of today, you will:
- Understand what window functions are and when to use them
- Master ranking functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE)
- Use analytical functions (LAG, LEAD, FIRST_VALUE, LAST_VALUE)
- Calculate running totals and moving averages
- Understand PARTITION BY and ORDER BY in window context
- Work with window frames (ROWS BETWEEN, RANGE BETWEEN)
- Apply window functions to real-world analytics problems

## What Are Window Functions?

**Window functions** perform calculations across a set of rows that are related to the current row, without collapsing the result set like GROUP BY does.

**Key Differences from GROUP BY**:
- **GROUP BY**: Collapses rows into groups, returns one row per group
- **Window Functions**: Keeps all rows, adds calculated columns

**Example**:
```sql
-- GROUP BY: Returns 3 rows (one per department)
SELECT department, AVG(salary) as avg_salary
FROM employees
GROUP BY department;

-- Window Function: Returns all rows with avg_salary added
SELECT 
    employee_name,
    department,
    salary,
    AVG(salary) OVER (PARTITION BY department) as avg_salary
FROM employees;
```

## Topics Covered

### 1. Window Function Syntax

**Basic Structure**:
```sql
function_name() OVER (
    [PARTITION BY partition_expression]
    [ORDER BY sort_expression]
    [ROWS/RANGE frame_specification]
)
```

**Components**:
- **function_name**: The window function (ROW_NUMBER, SUM, AVG, etc.)
- **PARTITION BY**: Divides rows into partitions (like GROUP BY but doesn't collapse)
- **ORDER BY**: Defines order within each partition
- **Frame**: Specifies which rows to include in calculation

### 2. Ranking Functions

**ROW_NUMBER()**: Assigns unique sequential integers
```sql
SELECT 
    employee_name,
    department,
    salary,
    ROW_NUMBER() OVER (ORDER BY salary DESC) as overall_rank,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
FROM employees;
```

**RANK()**: Assigns rank with gaps for ties
```sql
-- If two employees have same salary, both get rank 1, next gets rank 3
SELECT 
    employee_name,
    salary,
    RANK() OVER (ORDER BY salary DESC) as salary_rank
FROM employees;
-- Result: 1, 2, 2, 4, 5 (gap at 3)
```

**DENSE_RANK()**: Assigns rank without gaps
```sql
-- Same as RANK but no gaps
SELECT 
    employee_name,
    salary,
    DENSE_RANK() OVER (ORDER BY salary DESC) as salary_rank
FROM employees;
-- Result: 1, 2, 2, 3, 4 (no gap)
```

**NTILE(n)**: Divides rows into n buckets
```sql
-- Divide employees into 4 quartiles by salary
SELECT 
    employee_name,
    salary,
    NTILE(4) OVER (ORDER BY salary) as salary_quartile
FROM employees;
```

**Comparison**:
| Salary | ROW_NUMBER | RANK | DENSE_RANK |
|--------|------------|------|------------|
| 100    | 1          | 1    | 1          |
| 90     | 2          | 2    | 2          |
| 90     | 3          | 2    | 2          |
| 80     | 4          | 4    | 3          |
| 70     | 5          | 5    | 4          |

### 3. Analytical Functions

**LAG()**: Access previous row's value
```sql
SELECT 
    order_date,
    amount,
    LAG(amount) OVER (ORDER BY order_date) as previous_amount,
    amount - LAG(amount) OVER (ORDER BY order_date) as change
FROM orders;
```

**LEAD()**: Access next row's value
```sql
SELECT 
    order_date,
    amount,
    LEAD(amount) OVER (ORDER BY order_date) as next_amount,
    LEAD(amount) OVER (ORDER BY order_date) - amount as change_to_next
FROM orders;
```

**LAG/LEAD with offset and default**:
```sql
-- LAG(column, offset, default_value)
SELECT 
    order_date,
    amount,
    LAG(amount, 1, 0) OVER (ORDER BY order_date) as prev_1,
    LAG(amount, 2, 0) OVER (ORDER BY order_date) as prev_2,
    LEAD(amount, 1, 0) OVER (ORDER BY order_date) as next_1
FROM orders;
```

**FIRST_VALUE()**: First value in window
```sql
SELECT 
    employee_name,
    department,
    salary,
    FIRST_VALUE(salary) OVER (
        PARTITION BY department 
        ORDER BY salary DESC
    ) as highest_salary_in_dept
FROM employees;
```

**LAST_VALUE()**: Last value in window (requires frame specification)
```sql
SELECT 
    employee_name,
    department,
    salary,
    LAST_VALUE(salary) OVER (
        PARTITION BY department 
        ORDER BY salary DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as lowest_salary_in_dept
FROM employees;
```

### 4. Aggregate Functions as Window Functions

**Running Totals**:
```sql
SELECT 
    order_date,
    amount,
    SUM(amount) OVER (ORDER BY order_date) as running_total
FROM orders;
```

**Running Average**:
```sql
SELECT 
    order_date,
    amount,
    AVG(amount) OVER (ORDER BY order_date) as running_avg
FROM orders;
```

**Cumulative Count**:
```sql
SELECT 
    order_date,
    amount,
    COUNT(*) OVER (ORDER BY order_date) as cumulative_count
FROM orders;
```

**Partition-wise Aggregates**:
```sql
SELECT 
    employee_name,
    department,
    salary,
    SUM(salary) OVER (PARTITION BY department) as dept_total_salary,
    AVG(salary) OVER (PARTITION BY department) as dept_avg_salary,
    COUNT(*) OVER (PARTITION BY department) as dept_employee_count
FROM employees;
```

### 5. Window Frames

**Frame Specification**: Defines which rows to include in calculation

**Syntax**:
```sql
ROWS BETWEEN frame_start AND frame_end
RANGE BETWEEN frame_start AND frame_end
```

**Frame Boundaries**:
- `UNBOUNDED PRECEDING`: Start of partition
- `n PRECEDING`: n rows before current
- `CURRENT ROW`: Current row
- `n FOLLOWING`: n rows after current
- `UNBOUNDED FOLLOWING`: End of partition

**Moving Average (3-day)**:
```sql
SELECT 
    order_date,
    amount,
    AVG(amount) OVER (
        ORDER BY order_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as moving_avg_3day
FROM orders;
```

**Centered Moving Average**:
```sql
SELECT 
    order_date,
    amount,
    AVG(amount) OVER (
        ORDER BY order_date
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ) as centered_moving_avg
FROM orders;
```

**Year-to-Date Total**:
```sql
SELECT 
    order_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY YEAR(order_date)
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as ytd_total
FROM orders;
```

**ROWS vs RANGE**:
- **ROWS**: Physical rows (count-based)
- **RANGE**: Logical range (value-based, handles ties)

```sql
-- ROWS: Exactly 2 preceding rows
ROWS BETWEEN 2 PRECEDING AND CURRENT ROW

-- RANGE: All rows with values within range
RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND CURRENT ROW
```

### 6. Common Use Cases

**Top N per Group**:
```sql
WITH ranked_products AS (
    SELECT 
        category,
        product_name,
        sales,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) as rank
    FROM products
)
SELECT *
FROM ranked_products
WHERE rank <= 3;
```

**Percentage of Total**:
```sql
SELECT 
    department,
    employee_name,
    salary,
    SUM(salary) OVER (PARTITION BY department) as dept_total,
    ROUND(salary * 100.0 / SUM(salary) OVER (PARTITION BY department), 2) as pct_of_dept
FROM employees;
```

**Gap Analysis**:
```sql
SELECT 
    order_date,
    LAG(order_date) OVER (ORDER BY order_date) as prev_order_date,
    DATEDIFF(order_date, LAG(order_date) OVER (ORDER BY order_date)) as days_since_last_order
FROM orders;
```

**Running Difference**:
```sql
SELECT 
    month,
    revenue,
    revenue - LAG(revenue) OVER (ORDER BY month) as month_over_month_change,
    ROUND((revenue - LAG(revenue) OVER (ORDER BY month)) * 100.0 / LAG(revenue) OVER (ORDER BY month), 2) as pct_change
FROM monthly_revenue;
```

**Cohort Analysis**:
```sql
WITH first_purchase AS (
    SELECT 
        customer_id,
        MIN(order_date) as first_order_date
    FROM orders
    GROUP BY customer_id
)
SELECT 
    o.customer_id,
    o.order_date,
    fp.first_order_date,
    DATEDIFF(o.order_date, fp.first_order_date) as days_since_first_purchase,
    ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.order_date) as order_number
FROM orders o
INNER JOIN first_purchase fp ON o.customer_id = fp.customer_id;
```

### 7. Performance Considerations

**Best Practices**:
1. **Use PARTITION BY** to limit window size
2. **Avoid unnecessary ORDER BY** in window clause
3. **Reuse window definitions** with WINDOW clause
4. **Consider materialized views** for complex calculations
5. **Index partition and order columns**

**Window Clause (Reusable)**:
```sql
SELECT 
    employee_name,
    department,
    salary,
    ROW_NUMBER() OVER w as row_num,
    RANK() OVER w as rank,
    AVG(salary) OVER w as avg_salary
FROM employees
WINDOW w AS (PARTITION BY department ORDER BY salary DESC);
```

### 8. Advanced Patterns

**Conditional Aggregation**:
```sql
SELECT 
    order_date,
    amount,
    SUM(CASE WHEN amount > 1000 THEN amount ELSE 0 END) 
        OVER (ORDER BY order_date) as running_total_large_orders
FROM orders;
```

**Multiple Window Functions**:
```sql
SELECT 
    employee_name,
    department,
    salary,
    -- Ranking within department
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
    -- Overall ranking
    RANK() OVER (ORDER BY salary DESC) as overall_rank,
    -- Salary vs department average
    salary - AVG(salary) OVER (PARTITION BY department) as diff_from_dept_avg,
    -- Percentile
    PERCENT_RANK() OVER (ORDER BY salary) as salary_percentile
FROM employees;
```

**Lead/Lag with Multiple Offsets**:
```sql
SELECT 
    order_date,
    amount,
    LAG(amount, 1) OVER (ORDER BY order_date) as prev_1,
    LAG(amount, 7) OVER (ORDER BY order_date) as prev_7,
    LAG(amount, 30) OVER (ORDER BY order_date) as prev_30,
    -- Compare to same day last week
    amount - LAG(amount, 7) OVER (ORDER BY order_date) as week_over_week_change
FROM daily_orders;
```

## Hands-On Exercises

See `exercise.sql` for practical exercises covering:
- Ranking functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE)
- Analytical functions (LAG, LEAD, FIRST_VALUE, LAST_VALUE)
- Running totals and cumulative calculations
- Moving averages with different window frames
- Top N per group queries
- Gap analysis and time-series calculations
- Complex multi-window queries

## Key Takeaways

1. **Window functions don't collapse rows** like GROUP BY
2. **PARTITION BY divides data** into groups for calculation
3. **ORDER BY determines** calculation order within partitions
4. **ROW_NUMBER is always unique**, RANK has gaps, DENSE_RANK doesn't
5. **LAG/LEAD access** previous/next rows
6. **Window frames control** which rows are included in calculation
7. **ROWS is physical**, RANGE is logical
8. **Reuse window definitions** with WINDOW clause for performance
9. **Window functions are evaluated** after WHERE but before ORDER BY
10. **Use CTEs with window functions** for complex queries

## Exam Tips

- **Know the difference** between ROW_NUMBER, RANK, and DENSE_RANK
- **Understand LAG and LEAD** with offset and default parameters
- **Master running totals** (SUM OVER ORDER BY)
- **Know window frame syntax** (ROWS BETWEEN)
- **Understand PARTITION BY** vs GROUP BY
- **Know when to use** FIRST_VALUE vs LAST_VALUE
- **Remember frame defaults**: Without frame specification, default is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
- **Practice Top N per group** pattern (very common on exam)
- **Understand NTILE** for bucketing/quartiles
- **Know PERCENT_RANK** and CUME_DIST** for percentile calculations

## Common Exam Questions

1. How do you find the top 3 products per category?
2. What's the difference between RANK and DENSE_RANK?
3. How do you calculate a running total?
4. How do you calculate month-over-month change?
5. What does LAG(column, 2, 0) do?
6. How do you calculate a 7-day moving average?
7. What's the default window frame?
8. How do you find gaps between orders?
9. What does NTILE(4) do?
10. How do you calculate percentage of total by group?

## Additional Resources

- [Databricks Window Functions](https://docs.databricks.com/sql/language-manual/sql-ref-window-functions.html)
- [Spark SQL Window Functions](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html)
- [Window Function Examples](https://docs.databricks.com/sql/language-manual/sql-ref-window-functions-examples.html)

## Next Steps

Tomorrow: [Day 9 - Advanced SQL: Higher-Order Functions](../day-09-advanced-sql-higher-order-functions/README.md)

