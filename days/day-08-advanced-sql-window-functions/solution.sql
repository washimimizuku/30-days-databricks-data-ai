-- Day 8: Advanced SQL - Window Functions - Solutions
-- =========================================================

USE day8_window_functions;

-- =========================================================
-- PART 1: RANKING FUNCTIONS
-- =========================================================

-- Exercise 1: ROW_NUMBER - Basic Ranking
SELECT 
    sale_id,
    product_name,
    total_amount,
    ROW_NUMBER() OVER (ORDER BY total_amount DESC) as rank
FROM sales;

-- Exercise 2: ROW_NUMBER - Ranking within Groups
SELECT 
    category,
    product_name,
    total_amount,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY total_amount DESC) as rank_in_category
FROM sales;

-- Exercise 3: RANK vs DENSE_RANK
SELECT 
    employee_name,
    salary,
    RANK() OVER (ORDER BY salary DESC) as rank_with_gaps,
    DENSE_RANK() OVER (ORDER BY salary DESC) as dense_rank_no_gaps
FROM employees;

-- Exercise 4: NTILE - Quartiles
SELECT 
    employee_name,
    salary,
    NTILE(4) OVER (ORDER BY salary) as salary_quartile
FROM employees;

-- Exercise 5: Top N per Group
WITH ranked_sales AS (
    SELECT 
        category,
        product_name,
        total_amount,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY total_amount DESC) as rank
    FROM sales
)
SELECT *
FROM ranked_sales
WHERE rank <= 3;

-- =========================================================
-- PART 2: ANALYTICAL FUNCTIONS (LAG/LEAD)
-- =========================================================

-- Exercise 6: LAG - Previous Value
SELECT 
    sale_date,
    total_amount,
    LAG(total_amount) OVER (ORDER BY sale_date) as previous_amount
FROM sales
ORDER BY sale_date;

-- Exercise 7: LEAD - Next Value
SELECT 
    sale_date,
    total_amount,
    LEAD(total_amount) OVER (ORDER BY sale_date) as next_amount
FROM sales
ORDER BY sale_date;

-- Exercise 8: LAG with Offset
SELECT 
    ticker,
    trade_date,
    close_price,
    LAG(close_price, 2) OVER (PARTITION BY ticker ORDER BY trade_date) as price_2days_ago
FROM stock_prices
ORDER BY ticker, trade_date;

-- Exercise 9: Calculate Change
SELECT 
    ticker,
    trade_date,
    close_price,
    LAG(close_price) OVER (PARTITION BY ticker ORDER BY trade_date) as prev_close,
    close_price - LAG(close_price) OVER (PARTITION BY ticker ORDER BY trade_date) as price_change,
    ROUND((close_price - LAG(close_price) OVER (PARTITION BY ticker ORDER BY trade_date)) * 100.0 / 
          LAG(close_price) OVER (PARTITION BY ticker ORDER BY trade_date), 2) as pct_change
FROM stock_prices
ORDER BY ticker, trade_date;

-- Exercise 10: Gap Analysis
SELECT 
    customer_id,
    customer_name,
    order_date,
    LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_order_date,
    DATEDIFF(order_date, LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date)) as days_since_last_order
FROM customer_orders
ORDER BY customer_id, order_date;

-- =========================================================
-- PART 3: RUNNING TOTALS AND CUMULATIVE CALCULATIONS
-- =========================================================

-- Exercise 11: Running Total
SELECT 
    sale_date,
    total_amount,
    SUM(total_amount) OVER (ORDER BY sale_date) as running_total
FROM sales
ORDER BY sale_date;

-- Exercise 12: Running Total by Group
SELECT 
    category,
    sale_date,
    total_amount,
    SUM(total_amount) OVER (PARTITION BY category ORDER BY sale_date) as category_running_total
FROM sales
ORDER BY category, sale_date;

-- Exercise 13: Running Average
SELECT 
    sale_date,
    total_amount,
    AVG(total_amount) OVER (ORDER BY sale_date) as running_avg
FROM sales
ORDER BY sale_date;

-- Exercise 14: Cumulative Count
SELECT 
    order_date,
    order_amount,
    COUNT(*) OVER (ORDER BY order_date) as cumulative_order_count
FROM customer_orders
ORDER BY order_date;

-- Exercise 15: Year-to-Date Total
SELECT 
    month_date,
    region,
    revenue,
    SUM(revenue) OVER (
        PARTITION BY region, YEAR(month_date)
        ORDER BY month_date
    ) as ytd_revenue
FROM monthly_revenue
ORDER BY region, month_date;

-- =========================================================
-- PART 4: MOVING AVERAGES AND WINDOW FRAMES
-- =========================================================

-- Exercise 16: 3-Day Moving Average
SELECT 
    ticker,
    trade_date,
    close_price,
    AVG(close_price) OVER (
        PARTITION BY ticker
        ORDER BY trade_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as moving_avg_3day
FROM stock_prices
ORDER BY ticker, trade_date;

-- Exercise 17: Centered Moving Average
SELECT 
    ticker,
    trade_date,
    close_price,
    AVG(close_price) OVER (
        PARTITION BY ticker
        ORDER BY trade_date
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ) as centered_moving_avg
FROM stock_prices
ORDER BY ticker, trade_date;

-- Exercise 18: Custom Window Frame
SELECT 
    product_name,
    sale_date,
    total_amount,
    SUM(total_amount) OVER (
        PARTITION BY product_name
        ORDER BY sale_date
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as last_5_sales_sum
FROM sales
ORDER BY product_name, sale_date;

-- Exercise 19: Moving Sum
SELECT 
    sale_date,
    total_amount,
    SUM(total_amount) OVER (
        ORDER BY sale_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_sum_7day
FROM sales
ORDER BY sale_date;

-- Exercise 20: Expanding Window
SELECT 
    sale_date,
    total_amount,
    AVG(total_amount) OVER (
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as expanding_avg
FROM sales
ORDER BY sale_date;

-- =========================================================
-- PART 5: FIRST_VALUE AND LAST_VALUE
-- =========================================================

-- Exercise 21: FIRST_VALUE
SELECT 
    employee_name,
    department,
    salary,
    FIRST_VALUE(salary) OVER (
        PARTITION BY department 
        ORDER BY salary DESC
    ) as highest_salary_in_dept,
    salary - FIRST_VALUE(salary) OVER (
        PARTITION BY department 
        ORDER BY salary DESC
    ) as diff_from_highest
FROM employees;

-- Exercise 22: LAST_VALUE
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

-- Exercise 23: First and Last Order
SELECT 
    customer_id,
    customer_name,
    order_date,
    FIRST_VALUE(order_date) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
    ) as first_order_date,
    LAST_VALUE(order_date) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as last_order_date
FROM customer_orders;

-- Exercise 24: Price Range
SELECT 
    ticker,
    trade_date,
    close_price,
    FIRST_VALUE(close_price) OVER (
        PARTITION BY ticker 
        ORDER BY close_price DESC
    ) as highest_price,
    LAST_VALUE(close_price) OVER (
        PARTITION BY ticker 
        ORDER BY close_price DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as lowest_price
FROM stock_prices;

-- Exercise 25: Department Comparison
SELECT 
    employee_name,
    department,
    salary,
    FIRST_VALUE(salary) OVER (PARTITION BY department ORDER BY salary DESC) as dept_highest,
    LAST_VALUE(salary) OVER (
        PARTITION BY department 
        ORDER BY salary DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as dept_lowest,
    salary - FIRST_VALUE(salary) OVER (PARTITION BY department ORDER BY salary DESC) as diff_from_highest,
    salary - LAST_VALUE(salary) OVER (
        PARTITION BY department 
        ORDER BY salary DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as diff_from_lowest
FROM employees;

-- =========================================================
-- PART 6: AGGREGATE FUNCTIONS AS WINDOW FUNCTIONS
-- =========================================================

-- Exercise 26: Percentage of Total
SELECT 
    product_name,
    total_amount,
    SUM(total_amount) OVER () as total_sales,
    ROUND(total_amount * 100.0 / SUM(total_amount) OVER (), 2) as pct_of_total
FROM sales;

-- Exercise 27: Percentage of Group Total
SELECT 
    category,
    product_name,
    total_amount,
    SUM(total_amount) OVER (PARTITION BY category) as category_total,
    ROUND(total_amount * 100.0 / SUM(total_amount) OVER (PARTITION BY category), 2) as pct_of_category
FROM sales;

-- Exercise 28: Deviation from Average
SELECT 
    employee_name,
    department,
    salary,
    AVG(salary) OVER (PARTITION BY department) as dept_avg_salary,
    salary - AVG(salary) OVER (PARTITION BY department) as deviation_from_avg,
    ROUND((salary - AVG(salary) OVER (PARTITION BY department)) * 100.0 / 
          AVG(salary) OVER (PARTITION BY department), 2) as pct_deviation
FROM employees;

-- Exercise 29: Count and Sum by Group
SELECT 
    category,
    product_name,
    total_amount,
    COUNT(*) OVER (PARTITION BY category) as category_sale_count,
    SUM(total_amount) OVER (PARTITION BY category) as category_total_sales
FROM sales;

-- Exercise 30: Multiple Aggregates
SELECT 
    employee_name,
    department,
    salary,
    MIN(salary) OVER (PARTITION BY department) as dept_min,
    MAX(salary) OVER (PARTITION BY department) as dept_max,
    AVG(salary) OVER (PARTITION BY department) as dept_avg,
    COUNT(*) OVER (PARTITION BY department) as dept_count
FROM employees;

-- =========================================================
-- PART 7: COMPLEX WINDOW QUERIES
-- =========================================================

-- Exercise 31: Multiple Rankings
SELECT 
    category,
    product_name,
    total_amount,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY total_amount DESC) as category_rank,
    ROW_NUMBER() OVER (ORDER BY total_amount DESC) as overall_rank
FROM sales;

-- Exercise 32: Conditional Window Function
SELECT 
    sale_date,
    total_amount,
    SUM(CASE WHEN total_amount > 1000 THEN total_amount ELSE 0 END) 
        OVER (ORDER BY sale_date) as running_total_large_sales
FROM sales
ORDER BY sale_date;

-- Exercise 33: Window Function with CASE
SELECT 
    employee_name,
    performance_score,
    RANK() OVER (ORDER BY performance_score DESC) as performance_rank,
    CASE 
        WHEN RANK() OVER (ORDER BY performance_score DESC) <= 5 THEN 'Top'
        WHEN RANK() OVER (ORDER BY performance_score DESC) <= 10 THEN 'Middle'
        ELSE 'Bottom'
    END as performance_tier
FROM employees;

-- Exercise 34: Cohort Analysis
WITH first_orders AS (
    SELECT 
        customer_id,
        MIN(order_date) as first_order_date
    FROM customer_orders
    GROUP BY customer_id
)
SELECT 
    co.customer_id,
    co.customer_name,
    co.order_date,
    fo.first_order_date,
    ROW_NUMBER() OVER (PARTITION BY co.customer_id ORDER BY co.order_date) as order_number,
    DATEDIFF(co.order_date, fo.first_order_date) as days_since_first_order
FROM customer_orders co
INNER JOIN first_orders fo ON co.customer_id = fo.customer_id
ORDER BY co.customer_id, co.order_date;

-- Exercise 35: Percentile Calculation
SELECT 
    employee_name,
    salary,
    PERCENT_RANK() OVER (ORDER BY salary) as salary_percentile,
    CUME_DIST() OVER (ORDER BY salary) as cumulative_distribution
FROM employees;

-- =========================================================
-- PART 8: TIME-SERIES ANALYSIS
-- =========================================================

-- Exercise 36: Month-over-Month Growth
SELECT 
    month_date,
    region,
    revenue,
    LAG(revenue) OVER (PARTITION BY region ORDER BY month_date) as prev_month_revenue,
    revenue - LAG(revenue) OVER (PARTITION BY region ORDER BY month_date) as mom_change,
    ROUND((revenue - LAG(revenue) OVER (PARTITION BY region ORDER BY month_date)) * 100.0 / 
          LAG(revenue) OVER (PARTITION BY region ORDER BY month_date), 2) as mom_growth_pct
FROM monthly_revenue
ORDER BY region, month_date;

-- Exercise 37: Week-over-Week Comparison
SELECT 
    sale_date,
    SUM(total_amount) as daily_sales,
    LAG(SUM(total_amount), 7) OVER (ORDER BY sale_date) as same_day_last_week,
    SUM(total_amount) - LAG(SUM(total_amount), 7) OVER (ORDER BY sale_date) as wow_change
FROM sales
GROUP BY sale_date
ORDER BY sale_date;

-- Exercise 38: Same Day Last Week
SELECT 
    ticker,
    trade_date,
    close_price,
    LAG(close_price, 7) OVER (PARTITION BY ticker ORDER BY trade_date) as price_last_week,
    close_price - LAG(close_price, 7) OVER (PARTITION BY ticker ORDER BY trade_date) as weekly_change
FROM stock_prices
ORDER BY ticker, trade_date;

-- Exercise 39: Trend Analysis
SELECT 
    month_date,
    region,
    revenue,
    LAG(revenue) OVER (PARTITION BY region ORDER BY month_date) as prev_revenue,
    CASE 
        WHEN revenue > LAG(revenue) OVER (PARTITION BY region ORDER BY month_date) THEN 'Increasing'
        WHEN revenue < LAG(revenue) OVER (PARTITION BY region ORDER BY month_date) THEN 'Decreasing'
        ELSE 'Stable'
    END as trend
FROM monthly_revenue
ORDER BY region, month_date;

-- Exercise 40: Moving Average Crossover
WITH moving_averages AS (
    SELECT 
        ticker,
        trade_date,
        close_price,
        AVG(close_price) OVER (
            PARTITION BY ticker 
            ORDER BY trade_date 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as ma_3day,
        AVG(close_price) OVER (
            PARTITION BY ticker 
            ORDER BY trade_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as ma_7day
    FROM stock_prices
)
SELECT 
    ticker,
    trade_date,
    close_price,
    ma_3day,
    ma_7day,
    CASE 
        WHEN ma_3day > ma_7day AND LAG(ma_3day) OVER (PARTITION BY ticker ORDER BY trade_date) <= LAG(ma_7day) OVER (PARTITION BY ticker ORDER BY trade_date) 
        THEN 'Golden Cross'
        WHEN ma_3day < ma_7day AND LAG(ma_3day) OVER (PARTITION BY ticker ORDER BY trade_date) >= LAG(ma_7day) OVER (PARTITION BY ticker ORDER BY trade_date) 
        THEN 'Death Cross'
        ELSE NULL
    END as signal
FROM moving_averages
ORDER BY ticker, trade_date;

-- =========================================================
-- PART 9: ADVANCED PATTERNS
-- =========================================================

-- Exercise 41: Dense Rank with Ties
WITH ranked_employees AS (
    SELECT 
        employee_name,
        department,
        salary,
        DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) as salary_rank
    FROM employees
)
SELECT *
FROM ranked_employees
WHERE salary_rank <= 3;

-- Exercise 42: Running Difference
WITH first_sale AS (
    SELECT MIN(sale_date) as first_date, MIN(total_amount) as first_amount FROM sales
)
SELECT 
    s.sale_date,
    s.total_amount,
    fs.first_amount,
    s.total_amount - fs.first_amount as diff_from_first
FROM sales s
CROSS JOIN first_sale fs
ORDER BY s.sale_date;

-- Exercise 43: Cumulative Distribution
SELECT 
    employee_name,
    salary,
    CUME_DIST() OVER (ORDER BY salary) as cumulative_dist,
    ROUND(CUME_DIST() OVER (ORDER BY salary) * 100, 2) as percentile
FROM employees;

-- Exercise 44: Window with Multiple Partitions
SELECT 
    region,
    category,
    product_name,
    total_amount,
    ROW_NUMBER() OVER (PARTITION BY region, category ORDER BY total_amount DESC) as rank_in_region_category
FROM sales;

-- Exercise 45: Reusable Window Definition
SELECT 
    employee_name,
    department,
    salary,
    ROW_NUMBER() OVER w as row_num,
    RANK() OVER w as rank,
    DENSE_RANK() OVER w as dense_rank,
    AVG(salary) OVER w as avg_salary
FROM employees
WINDOW w AS (PARTITION BY department ORDER BY salary DESC);

-- =========================================================
-- PART 10: REAL-WORLD SCENARIOS
-- =========================================================

-- Exercise 46: Sales Performance Dashboard
SELECT 
    sales_rep,
    SUM(total_amount) as total_sales,
    RANK() OVER (ORDER BY SUM(total_amount) DESC) as rep_rank,
    ROUND(SUM(total_amount) * 100.0 / SUM(SUM(total_amount)) OVER (), 2) as pct_of_total,
    SUM(total_amount) - AVG(SUM(total_amount)) OVER () as diff_from_avg
FROM sales
GROUP BY sales_rep
ORDER BY total_sales DESC;

-- Exercise 47: Stock Analysis Report
SELECT 
    ticker,
    trade_date,
    close_price,
    AVG(close_price) OVER (
        PARTITION BY ticker 
        ORDER BY trade_date 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as ma_5day,
    close_price - LAG(close_price, 5) OVER (PARTITION BY ticker ORDER BY trade_date) as change_5days,
    RANK() OVER (PARTITION BY trade_date ORDER BY close_price DESC) as price_rank
FROM stock_prices
ORDER BY ticker, trade_date;

-- Exercise 48: Employee Compensation Analysis
SELECT 
    employee_name,
    department,
    salary,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
    NTILE(4) OVER (ORDER BY salary) as salary_quartile,
    salary - PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) OVER (PARTITION BY department) as diff_from_median
FROM employees;

-- Exercise 49: Customer Lifetime Value
WITH customer_metrics AS (
    SELECT 
        customer_id,
        customer_name,
        COUNT(*) as total_orders,
        SUM(order_amount) as total_spent,
        AVG(order_amount) as avg_order_value,
        MIN(order_date) as first_order_date,
        DATEDIFF(CURRENT_DATE(), MIN(order_date)) as days_since_first
    FROM customer_orders
    GROUP BY customer_id, customer_name
)
SELECT 
    *,
    RANK() OVER (ORDER BY total_spent DESC) as customer_rank
FROM customer_metrics
ORDER BY total_spent DESC;

-- Exercise 50: Revenue Trend Report
SELECT 
    month_date,
    region,
    revenue,
    SUM(revenue) OVER (PARTITION BY region ORDER BY month_date) as running_total,
    revenue - LAG(revenue) OVER (PARTITION BY region ORDER BY month_date) as mom_change,
    AVG(revenue) OVER (
        PARTITION BY region 
        ORDER BY month_date 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as ma_3month,
    RANK() OVER (PARTITION BY region ORDER BY revenue DESC) as revenue_rank
FROM monthly_revenue
ORDER BY region, month_date;

-- =========================================================
-- KEY LEARNINGS SUMMARY
-- =========================================================
%md
## Key Learnings from Day 8

### Ranking Functions
- ROW_NUMBER: Always unique, sequential
- RANK: Gaps for ties (1, 2, 2, 4)
- DENSE_RANK: No gaps (1, 2, 2, 3)
- NTILE: Divide into buckets

### Analytical Functions
- LAG: Access previous row
- LEAD: Access next row
- FIRST_VALUE: First in window
- LAST_VALUE: Last in window (needs frame spec)

### Running Calculations
- Running totals: SUM() OVER (ORDER BY)
- Running averages: AVG() OVER (ORDER BY)
- Cumulative counts: COUNT() OVER (ORDER BY)

### Window Frames
- ROWS: Physical rows
- RANGE: Logical range
- BETWEEN: Define start and end
- Common: ROWS BETWEEN n PRECEDING AND CURRENT ROW

### Best Practices
- Use PARTITION BY to limit window size
- Reuse window definitions with WINDOW clause
- Understand default frame behavior
- Consider performance for large datasets

**Next**: Day 9 will cover Higher-Order Functions for complex data transformations!

