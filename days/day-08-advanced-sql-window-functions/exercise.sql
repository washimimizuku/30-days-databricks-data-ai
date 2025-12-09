-- Day 8: Advanced SQL - Window Functions - Exercises
-- =========================================================

USE day8_window_functions;

-- =========================================================
-- PART 1: RANKING FUNCTIONS
-- =========================================================

-- Exercise 1: ROW_NUMBER - Basic Ranking
-- TODO: Rank all sales by total_amount (highest first)

-- YOUR CODE HERE


-- Exercise 2: ROW_NUMBER - Ranking within Groups
-- TODO: Rank sales within each category by total_amount

-- YOUR CODE HERE


-- Exercise 3: RANK vs DENSE_RANK
-- TODO: Show the difference between RANK and DENSE_RANK for employee salaries

-- YOUR CODE HERE


-- Exercise 4: NTILE - Quartiles
-- TODO: Divide employees into 4 salary quartiles

-- YOUR CODE HERE


-- Exercise 5: Top N per Group
-- TODO: Find the top 3 products by sales amount in each category

-- YOUR CODE HERE


-- =========================================================
-- PART 2: ANALYTICAL FUNCTIONS (LAG/LEAD)
-- =========================================================

-- Exercise 6: LAG - Previous Value
-- TODO: For each sale, show the previous sale amount (ordered by date)

-- YOUR CODE HERE


-- Exercise 7: LEAD - Next Value
-- TODO: For each sale, show the next sale amount

-- YOUR CODE HERE


-- Exercise 8: LAG with Offset
-- TODO: Show current stock price and price from 2 days ago

-- YOUR CODE HERE


-- Exercise 9: Calculate Change
-- TODO: Calculate day-over-day change in stock prices

-- YOUR CODE HERE


-- Exercise 10: Gap Analysis
-- TODO: Calculate days between consecutive orders for each customer

-- YOUR CODE HERE


-- =========================================================
-- PART 3: RUNNING TOTALS AND CUMULATIVE CALCULATIONS
-- =========================================================

-- Exercise 11: Running Total
-- TODO: Calculate running total of sales by date

-- YOUR CODE HERE


-- Exercise 12: Running Total by Group
-- TODO: Calculate running total of sales by category

-- YOUR CODE HERE


-- Exercise 13: Running Average
-- TODO: Calculate running average of sales amounts

-- YOUR CODE HERE


-- Exercise 14: Cumulative Count
-- TODO: Count cumulative number of orders by date

-- YOUR CODE HERE


-- Exercise 15: Year-to-Date Total
-- TODO: Calculate YTD revenue for each region

-- YOUR CODE HERE


-- =========================================================
-- PART 4: MOVING AVERAGES AND WINDOW FRAMES
-- =========================================================

-- Exercise 16: 3-Day Moving Average
-- TODO: Calculate 3-day moving average of stock closing prices

-- YOUR CODE HERE


-- Exercise 17: Centered Moving Average
-- TODO: Calculate centered 3-day moving average (1 before, current, 1 after)

-- YOUR CODE HERE


-- Exercise 18: Custom Window Frame
-- TODO: Calculate sum of last 5 sales for each product

-- YOUR CODE HERE


-- Exercise 19: Moving Sum
-- TODO: Calculate 7-day moving sum of sales

-- YOUR CODE HERE


-- Exercise 20: Expanding Window
-- TODO: Calculate expanding average (from start to current row)

-- YOUR CODE HERE


-- =========================================================
-- PART 5: FIRST_VALUE AND LAST_VALUE
-- =========================================================

-- Exercise 21: FIRST_VALUE
-- TODO: Show each employee's salary compared to highest in their department

-- YOUR CODE HERE


-- Exercise 22: LAST_VALUE
-- TODO: Show each employee's salary compared to lowest in their department

-- YOUR CODE HERE


-- Exercise 23: First and Last Order
-- TODO: For each customer, show their first and last order dates

-- YOUR CODE HERE


-- Exercise 24: Price Range
-- TODO: Show highest and lowest stock price for each ticker

-- YOUR CODE HERE


-- Exercise 25: Department Comparison
-- TODO: Compare each employee's salary to both highest and lowest in department

-- YOUR CODE HERE


-- =========================================================
-- PART 6: AGGREGATE FUNCTIONS AS WINDOW FUNCTIONS
-- =========================================================

-- Exercise 26: Percentage of Total
-- TODO: Calculate each sale as percentage of total sales

-- YOUR CODE HERE


-- Exercise 27: Percentage of Group Total
-- TODO: Calculate each sale as percentage of category total

-- YOUR CODE HERE


-- Exercise 28: Deviation from Average
-- TODO: Show how much each employee's salary deviates from department average

-- YOUR CODE HERE


-- Exercise 29: Count and Sum by Group
-- TODO: For each sale, show total sales count and sum for that category

-- YOUR CODE HERE


-- Exercise 30: Multiple Aggregates
-- TODO: Show min, max, avg, and count of salaries by department for each employee

-- YOUR CODE HERE


-- =========================================================
-- PART 7: COMPLEX WINDOW QUERIES
-- =========================================================

-- Exercise 31: Multiple Rankings
-- TODO: Show both category rank and overall rank for each product

-- YOUR CODE HERE


-- Exercise 32: Conditional Window Function
-- TODO: Calculate running total of only large sales (>1000)

-- YOUR CODE HERE


-- Exercise 33: Window Function with CASE
-- TODO: Rank employees by performance score, showing 'Top', 'Middle', 'Bottom' tier

-- YOUR CODE HERE


-- Exercise 34: Cohort Analysis
-- TODO: For each customer, show order number and days since first order

-- YOUR CODE HERE


-- Exercise 35: Percentile Calculation
-- TODO: Calculate salary percentile for each employee

-- YOUR CODE HERE


-- =========================================================
-- PART 8: TIME-SERIES ANALYSIS
-- =========================================================

-- Exercise 36: Month-over-Month Growth
-- TODO: Calculate month-over-month revenue growth percentage

-- YOUR CODE HERE


-- Exercise 37: Week-over-Week Comparison
-- TODO: Compare each week's sales to previous week

-- YOUR CODE HERE


-- Exercise 38: Same Day Last Week
-- TODO: Compare stock prices to same day previous week

-- YOUR CODE HERE


-- Exercise 39: Trend Analysis
-- TODO: Identify if revenue is increasing, decreasing, or stable

-- YOUR CODE HERE


-- Exercise 40: Moving Average Crossover
-- TODO: Identify when 3-day MA crosses above 7-day MA (stock signal)

-- YOUR CODE HERE


-- =========================================================
-- PART 9: ADVANCED PATTERNS
-- =========================================================

-- Exercise 41: Dense Rank with Ties
-- TODO: Find all employees tied for top 3 salaries in each department

-- YOUR CODE HERE


-- Exercise 42: Running Difference
-- TODO: Calculate running difference from first sale amount

-- YOUR CODE HERE


-- Exercise 43: Cumulative Distribution
-- TODO: Calculate cumulative distribution of salaries

-- YOUR CODE HERE


-- Exercise 44: Window with Multiple Partitions
-- TODO: Rank sales by amount within region and category

-- YOUR CODE HERE


-- Exercise 45: Reusable Window Definition
-- TODO: Use WINDOW clause to define reusable window for multiple functions

-- YOUR CODE HERE


-- =========================================================
-- PART 10: REAL-WORLD SCENARIOS
-- =========================================================

-- Exercise 46: Sales Performance Dashboard
-- TODO: Create a dashboard showing:
-- - Sales rep name
-- - Total sales
-- - Rank among all reps
-- - Percentage of total sales
-- - Difference from average

-- YOUR CODE HERE


-- Exercise 47: Stock Analysis Report
-- TODO: For each stock, show:
-- - Current price
-- - 5-day moving average
-- - Price change from 5 days ago
-- - Rank by current price

-- YOUR CODE HERE


-- Exercise 48: Employee Compensation Analysis
-- TODO: Show:
-- - Employee name, department, salary
-- - Department rank
-- - Salary quartile
-- - Difference from department median

-- YOUR CODE HERE


-- Exercise 49: Customer Lifetime Value
-- TODO: Calculate:
-- - Customer name
-- - Total orders
-- - Total spent
-- - Average order value
-- - Days since first order
-- - Customer rank by total spent

-- YOUR CODE HERE


-- Exercise 50: Revenue Trend Report
-- TODO: Create comprehensive revenue report with:
-- - Month, region, revenue
-- - Running total
-- - Month-over-month change
-- - 3-month moving average
-- - Rank within region

-- YOUR CODE HERE


-- =========================================================
-- REFLECTION QUESTIONS
-- =========================================================
-- Answer these in a markdown cell:
--
-- 1. What's the difference between ROW_NUMBER, RANK, and DENSE_RANK?
-- 2. When would you use LAG vs LEAD?
-- 3. What's the default window frame if not specified?
-- 4. How do you calculate a running total?
-- 5. What's the difference between ROWS and RANGE?
-- 6. Why does LAST_VALUE need explicit frame specification?
-- 7. How do you find top N per group?
-- 8. What's the purpose of PARTITION BY?
-- 9. How do you calculate month-over-month growth?
-- 10. When should you use NTILE?


-- =========================================================
-- BONUS CHALLENGES
-- =========================================================

-- Bonus 1: Advanced Cohort Analysis
-- TODO: Create a cohort analysis showing:
-- - Customer cohort (month of first purchase)
-- - Number of customers in cohort
-- - Average orders per customer
-- - Average revenue per customer
-- - Retention rate (customers with 2+ orders)

-- YOUR CODE HERE


-- Bonus 2: Stock Trading Signals
-- TODO: Generate trading signals:
-- - Calculate 5-day and 20-day moving averages
-- - Identify golden cross (5-day crosses above 20-day)
-- - Identify death cross (5-day crosses below 20-day)
-- - Calculate RSI (Relative Strength Index)

-- YOUR CODE HERE


-- Bonus 3: Sales Funnel Analysis
-- TODO: Analyze sales funnel:
-- - Stage of sale (Pending, Processing, Completed)
-- - Count at each stage
-- - Conversion rate to next stage
-- - Average time in each stage
-- - Drop-off rate

-- YOUR CODE HERE


-- Bonus 4: Employee Career Progression
-- TODO: Track employee progression:
-- - Years at company
-- - Salary growth rate
-- - Performance trend
-- - Promotion likelihood (based on patterns)
-- - Comparison to peers hired same year

-- YOUR CODE HERE


-- Bonus 5: Seasonal Decomposition
-- TODO: Decompose revenue into:
-- - Trend (moving average)
-- - Seasonal component
-- - Residual
-- - Year-over-year growth

-- YOUR CODE HERE


-- =========================================================
-- CLEANUP (Optional)
-- =========================================================

-- Uncomment to clean up
-- DROP DATABASE IF EXISTS day8_window_functions CASCADE;


-- =========================================================
-- KEY LEARNINGS CHECKLIST
-- =========================================================
-- [ ] Mastered ROW_NUMBER, RANK, DENSE_RANK
-- [ ] Used LAG and LEAD for time-series analysis
-- [ ] Calculated running totals and cumulative sums
-- [ ] Created moving averages with window frames
-- [ ] Applied FIRST_VALUE and LAST_VALUE
-- [ ] Used NTILE for bucketing
-- [ ] Calculated percentages of totals
-- [ ] Performed top N per group queries
-- [ ] Analyzed trends with window functions
-- [ ] Built real-world analytics queries

