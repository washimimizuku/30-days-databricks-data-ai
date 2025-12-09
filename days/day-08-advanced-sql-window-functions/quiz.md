# Day 8 Quiz: Advanced SQL - Window Functions

Test your knowledge of window functions, ranking, and analytical queries.

---

## Question 1
**What is the main difference between window functions and GROUP BY?**

A) Window functions are faster  
B) Window functions don't collapse rows like GROUP BY does  
C) GROUP BY is more powerful  
D) They are the same  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Window functions don't collapse rows like GROUP BY does**

Explanation: GROUP BY collapses rows into groups and returns one row per group. Window functions perform calculations across rows but keep all original rows in the result set.
</details>

---

## Question 2
**What's the difference between RANK() and DENSE_RANK()?**

A) RANK is faster  
B) RANK has gaps for ties, DENSE_RANK doesn't  
C) DENSE_RANK is more accurate  
D) They are identical  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) RANK has gaps for ties, DENSE_RANK doesn't**

Explanation: RANK assigns ranks with gaps (1, 2, 2, 4), while DENSE_RANK has no gaps (1, 2, 2, 3). Both handle ties the same way, but RANK skips numbers after ties.
</details>

---

## Question 3
**What does LAG(column, 2, 0) do?**

A) Returns the value 2 rows ahead  
B) Returns the value 2 rows before, defaulting to 0 if not available  
C) Returns the value 2 columns to the left  
D) Delays execution by 2 seconds  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Returns the value 2 rows before, defaulting to 0 if not available**

Explanation: LAG(column, offset, default) accesses a previous row. The offset specifies how many rows back (default 1), and the default value is returned when there's no previous row.
</details>

---

## Question 4
**How do you calculate a running total?**

A) SUM(column) GROUP BY column  
B) SUM(column) OVER (ORDER BY column)  
C) RUNNING_SUM(column)  
D) CUMULATIVE(column)  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) SUM(column) OVER (ORDER BY column)**

Explanation: A running total is calculated using SUM as a window function with ORDER BY. This accumulates the sum from the first row to the current row.
</details>

---

## Question 5
**What is the purpose of PARTITION BY in a window function?**

A) To create table partitions  
B) To divide rows into groups for separate window calculations  
C) To filter rows  
D) To sort results  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To divide rows into groups for separate window calculations**

Explanation: PARTITION BY divides the result set into partitions, and the window function is applied separately to each partition. It's similar to GROUP BY but doesn't collapse rows.
</details>

---

## Question 6
**What does NTILE(4) do?**

A) Returns the 4th row  
B) Divides rows into 4 equal buckets  
C) Returns rows where value equals 4  
D) Calculates 4-day moving average  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Divides rows into 4 equal buckets**

Explanation: NTILE(n) divides the ordered result set into n approximately equal groups, assigning a bucket number (1 to n) to each row. Useful for quartiles, deciles, etc.
</details>

---

## Question 7
**What is the default window frame if not specified?**

A) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW  
B) RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW  
C) ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING  
D) No default frame  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW**

Explanation: When ORDER BY is specified but no frame, the default is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW. This includes all rows from the start of the partition to the current row.
</details>

---

## Question 8
**How do you calculate a 3-day moving average?**

A) AVG(column) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)  
B) AVG(column) OVER (ORDER BY date ROWS 3)  
C) MOVING_AVG(column, 3)  
D) AVG(column) GROUP BY date LIMIT 3  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) AVG(column) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)**

Explanation: A 3-day moving average includes the current row and 2 preceding rows (total of 3). ROWS BETWEEN 2 PRECEDING AND CURRENT ROW specifies this window frame.
</details>

---

## Question 9
**What's the difference between ROWS and RANGE in window frames?**

A) ROWS is faster  
B) ROWS is physical (count-based), RANGE is logical (value-based)  
C) RANGE is more accurate  
D) They are identical  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) ROWS is physical (count-based), RANGE is logical (value-based)**

Explanation: ROWS counts physical rows (e.g., 3 preceding rows). RANGE considers values and includes all rows with the same value, handling ties differently.
</details>

---

## Question 10
**Why does LAST_VALUE often need explicit frame specification?**

A) It's a bug  
B) Default frame only goes to current row, not end of partition  
C) It's required by SQL standard  
D) For performance reasons  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Default frame only goes to current row, not end of partition**

Explanation: The default frame (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) only includes rows up to the current row. To get the actual last value, you need: ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING.
</details>

---

## Question 11
**How do you find the top 3 products per category?**

A) Use LIMIT 3 with GROUP BY category  
B) Use ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) and filter WHERE rank <= 3  
C) Use TOP 3 BY category  
D) Use MAX() with LIMIT 3  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Use ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) and filter WHERE rank <= 3**

Explanation: The "top N per group" pattern uses ROW_NUMBER with PARTITION BY to rank within each group, then filters in a CTE or subquery to keep only top N ranks.
</details>

---

## Question 12
**What does LEAD() do?**

A) Accesses the previous row's value  
B) Accesses the next row's value  
C) Returns the first value in the partition  
D) Returns the last value in the partition  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Accesses the next row's value**

Explanation: LEAD() is the opposite of LAG(). It accesses values from subsequent rows. LEAD(column, 1) gets the next row's value, LEAD(column, 2) gets the value 2 rows ahead.
</details>

---

## Question 13
**Can you use multiple window functions in the same SELECT?**

A) No, only one per query  
B) Yes, and you can reuse window definitions with WINDOW clause  
C) Yes, but they must use the same PARTITION BY  
D) No, it causes errors  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Yes, and you can reuse window definitions with WINDOW clause**

Explanation: You can use multiple window functions in one query. The WINDOW clause lets you define a window once and reuse it: WINDOW w AS (PARTITION BY dept ORDER BY salary).
</details>

---

## Question 14
**What does PERCENT_RANK() return?**

A) The percentage value of the column  
B) The relative rank as a percentage (0 to 1)  
C) The top percentage of values  
D) The rank divided by 100  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) The relative rank as a percentage (0 to 1)**

Explanation: PERCENT_RANK() returns the relative rank of a row within a partition, calculated as (rank - 1) / (total rows - 1). Values range from 0 to 1.
</details>

---

## Question 15
**When calculating month-over-month growth, which function is most useful?**

A) RANK()  
B) LAG()  
C) NTILE()  
D) FIRST_VALUE()  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) LAG()**

Explanation: LAG() accesses the previous month's value, allowing you to calculate: (current - LAG(current)) / LAG(current) * 100 for month-over-month growth percentage.
</details>

---

## Scoring Guide

- **13-15 correct**: Excellent! You've mastered window functions.
- **10-12 correct**: Good job! Review the questions you missed.
- **7-9 correct**: Fair. Review the Day 8 materials and practice more.
- **Below 7**: Review the Day 8 README and complete more exercises.

---

## Key Concepts to Remember

1. **Window functions don't collapse rows** (unlike GROUP BY)
2. **ROW_NUMBER**: Always unique
3. **RANK**: Has gaps for ties (1, 2, 2, 4)
4. **DENSE_RANK**: No gaps (1, 2, 2, 3)
5. **LAG/LEAD**: Access previous/next rows
6. **PARTITION BY**: Divides data for separate calculations
7. **ORDER BY**: Determines calculation order
8. **Window frames**: ROWS (physical) vs RANGE (logical)
9. **Default frame**: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
10. **LAST_VALUE**: Needs explicit frame to end of partition

---

## Common Exam Scenarios

### Scenario 1: Top N per Group
**Question**: Find the top 3 highest-paid employees in each department.

**Answer**:
```sql
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn
    FROM employees
)
SELECT * FROM ranked WHERE rn <= 3;
```

### Scenario 2: Running Total
**Question**: Calculate cumulative sales by date.

**Answer**:
```sql
SELECT 
    sale_date,
    amount,
    SUM(amount) OVER (ORDER BY sale_date) as running_total
FROM sales;
```

### Scenario 3: Month-over-Month Change
**Question**: Calculate month-over-month revenue growth.

**Answer**:
```sql
SELECT 
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY month) as prev_month,
    (revenue - LAG(revenue) OVER (ORDER BY month)) * 100.0 / 
        LAG(revenue) OVER (ORDER BY month) as growth_pct
FROM monthly_revenue;
```

### Scenario 4: Moving Average
**Question**: Calculate 7-day moving average of stock prices.

**Answer**:
```sql
SELECT 
    date,
    price,
    AVG(price) OVER (
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as ma_7day
FROM stock_prices;
```

---

## Practice Tips

1. **Start with simple ranking** (ROW_NUMBER, RANK)
2. **Practice LAG/LEAD** for time-series analysis
3. **Master running totals** (very common on exam)
4. **Understand window frames** (ROWS BETWEEN)
5. **Practice top N per group** (frequent exam pattern)
6. **Learn PARTITION BY** vs GROUP BY differences
7. **Experiment with moving averages**
8. **Try complex multi-window queries**

---

## Common Mistakes to Avoid

- Forgetting ORDER BY in ranking functions
- Using wrong frame for LAST_VALUE
- Confusing ROWS and RANGE
- Not using PARTITION BY when needed
- Mixing up LAG and LEAD
- Forgetting default values in LAG/LEAD
- Using GROUP BY when window function is needed

---

## Next Steps

- Review any questions you got wrong
- Re-read relevant sections in Day 8 README
- Practice window functions with real datasets
- Complete bonus exercises
- Move on to Day 9: Advanced SQL - Higher-Order Functions

Good luck! 🚀
