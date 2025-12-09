-- Day 9: Advanced SQL - Higher-Order Functions - Exercises
-- =========================================================

USE day9_higher_order_functions;

-- =========================================================
-- PART 1: TRANSFORM FUNCTION
-- =========================================================

-- Exercise 1: Basic TRANSFORM
-- TODO: Double all order amounts

-- YOUR CODE HERE


-- Exercise 2: TRANSFORM with Tax
-- TODO: Add 10% tax to all order amounts

-- YOUR CODE HERE


-- Exercise 3: TRANSFORM with Index
-- TODO: Create order labels like "Order_1", "Order_2" using index

-- YOUR CODE HERE


-- Exercise 4: TRANSFORM Prices
-- TODO: Apply 15% discount to all product prices

-- YOUR CODE HERE


-- Exercise 5: TRANSFORM Strings
-- TODO: Convert all tags to uppercase

-- YOUR CODE HERE


-- =========================================================
-- PART 2: FILTER FUNCTION
-- =========================================================

-- Exercise 6: Basic FILTER
-- TODO: Keep only order amounts greater than 200

-- YOUR CODE HERE


-- Exercise 7: FILTER with Multiple Conditions
-- TODO: Keep only completed orders with amounts > 150

-- YOUR CODE HERE


-- Exercise 8: FILTER Reviews
-- TODO: Keep only verified reviews with rating >= 4

-- YOUR CODE HERE


-- Exercise 9: FILTER Events
-- TODO: Keep only purchase events

-- YOUR CODE HERE


-- Exercise 10: FILTER by Date
-- TODO: Keep only orders from February 2024

-- YOUR CODE HERE


-- =========================================================
-- PART 3: AGGREGATE FUNCTION
-- =========================================================

-- Exercise 11: Sum with AGGREGATE
-- TODO: Calculate total of all order amounts

-- YOUR CODE HERE


-- Exercise 12: Average with AGGREGATE
-- TODO: Calculate average of order amounts

-- YOUR CODE HERE


-- Exercise 13: Product with AGGREGATE
-- TODO: Calculate product of daily sales

-- YOUR CODE HERE


-- Exercise 14: Concatenate with AGGREGATE
-- TODO: Concatenate all tags with commas

-- YOUR CODE HERE


-- Exercise 15: Complex AGGREGATE
-- TODO: Calculate weighted average of prices

-- YOUR CODE HERE


-- =========================================================
-- PART 4: EXISTS AND FORALL
-- =========================================================

-- Exercise 16: EXISTS - Check for Large Orders
-- TODO: Check if customer has any order > 300

-- YOUR CODE HERE


-- Exercise 17: EXISTS - Check for Status
-- TODO: Check if customer has any pending orders

-- YOUR CODE HERE


-- Exercise 18: FORALL - All Completed
-- TODO: Check if all orders are completed

-- YOUR CODE HERE


-- Exercise 19: FORALL - All Positive
-- TODO: Check if all daily sales are positive

-- YOUR CODE HERE


-- Exercise 20: EXISTS vs FORALL
-- TODO: Compare EXISTS and FORALL for high ratings

-- YOUR CODE HERE


-- =========================================================
-- PART 5: ARRAY FUNCTIONS
-- =========================================================

-- Exercise 21: array_contains
-- TODO: Check if 'laptop' tag exists

-- YOUR CODE HERE


-- Exercise 22: array_distinct
-- TODO: Remove duplicate regions

-- YOUR CODE HERE


-- Exercise 23: array_union
-- TODO: Combine order_ids from two customers

-- YOUR CODE HERE


-- Exercise 24: array_intersect
-- TODO: Find common tags between products

-- YOUR CODE HERE


-- Exercise 25: array_sort
-- TODO: Sort order amounts in ascending order

-- YOUR CODE HERE


-- Exercise 26: array_min and array_max
-- TODO: Find min and max order amounts

-- YOUR CODE HERE


-- Exercise 27: array_position
-- TODO: Find position of 'Completed' in statuses

-- YOUR CODE HERE


-- Exercise 28: array_remove
-- TODO: Remove 'Pending' from order statuses

-- YOUR CODE HERE


-- Exercise 29: array_join
-- TODO: Join tags with ' | ' separator

-- YOUR CODE HERE


-- Exercise 30: size
-- TODO: Count number of orders per customer

-- YOUR CODE HERE


-- =========================================================
-- PART 6: EXPLODE AND FLATTEN
-- =========================================================

-- Exercise 31: Basic EXPLODE
-- TODO: Explode order_ids to separate rows

-- YOUR CODE HERE


-- Exercise 32: EXPLODE with JOIN
-- TODO: Explode order amounts and join with customer info

-- YOUR CODE HERE


-- Exercise 33: POSEXPLODE
-- TODO: Explode with position index

-- YOUR CODE HERE


-- Exercise 34: EXPLODE_OUTER
-- TODO: Explode keeping NULL arrays

-- YOUR CODE HERE


-- Exercise 35: FLATTEN
-- TODO: Flatten nested daily_events

-- YOUR CODE HERE


-- =========================================================
-- PART 7: WORKING WITH STRUCTS
-- =========================================================

-- Exercise 36: Access Struct Fields
-- TODO: Extract rating and comment from reviews

-- YOUR CODE HERE


-- Exercise 37: TRANSFORM Structs
-- TODO: Increment all review ratings by 1

-- YOUR CODE HERE


-- Exercise 38: FILTER Structs
-- TODO: Keep only ERROR level log entries

-- YOUR CODE HERE


-- Exercise 39: AGGREGATE Structs
-- TODO: Calculate average rating from reviews

-- YOUR CODE HERE


-- Exercise 40: EXPLODE Structs
-- TODO: Explode reviews to separate rows

-- YOUR CODE HERE


-- =========================================================
-- PART 8: WORKING WITH MAPS
-- =========================================================

-- Exercise 41: map_keys
-- TODO: Get all attribute keys

-- YOUR CODE HERE


-- Exercise 42: map_values
-- TODO: Get all attribute values

-- YOUR CODE HERE


-- Exercise 43: map_contains_key
-- TODO: Check if 'warranty' key exists

-- YOUR CODE HERE


-- Exercise 44: Access Map Value
-- TODO: Get 'brand' value from attributes

-- YOUR CODE HERE


-- Exercise 45: transform_values
-- TODO: Convert all map values to uppercase

-- YOUR CODE HERE


-- =========================================================
-- PART 9: COMBINING FUNCTIONS
-- =========================================================

-- Exercise 46: TRANSFORM + FILTER
-- TODO: Apply tax then filter amounts > 200

-- YOUR CODE HERE


-- Exercise 47: FILTER + AGGREGATE
-- TODO: Sum only large orders (> 200)

-- YOUR CODE HERE


-- Exercise 48: TRANSFORM + EXPLODE
-- TODO: Double amounts then explode

-- YOUR CODE HERE


-- Exercise 49: FILTER + EXISTS
-- TODO: Find customers with large completed orders

-- YOUR CODE HERE


-- Exercise 50: Complex Combination
-- TODO: Transform, filter, and aggregate in one query

-- YOUR CODE HERE


-- =========================================================
-- REFLECTION QUESTIONS
-- =========================================================
-- Answer these in a markdown cell:
--
-- 1. What's the difference between TRANSFORM and FILTER?
-- 2. When would you use AGGREGATE instead of SUM?
-- 3. What's the difference between EXISTS and FORALL?
-- 4. When should you use EXPLODE vs FLATTEN?
-- 5. How do you access a struct field?
-- 6. What's the difference between EXPLODE and EXPLODE_OUTER?
-- 7. How do you check if an array contains a value?
-- 8. What does array_distinct do?
-- 9. How do you get keys from a map?
-- 10. Why use higher-order functions instead of EXPLODE?


-- =========================================================
-- BONUS CHALLENGES
-- =========================================================

-- Bonus 1: E-commerce Analytics
-- TODO: Calculate:
-- - Total revenue per customer
-- - Average order value
-- - Number of high-value orders (>250)
-- - Percentage of completed orders

-- YOUR CODE HERE


-- Bonus 2: Product Performance
-- TODO: For each product:
-- - Average price across all price points
-- - Number of 5-star reviews
-- - Percentage of verified reviews
-- - Most common tag

-- YOUR CODE HERE


-- Bonus 3: User Behavior Analysis
-- TODO: Analyze user events:
-- - Total purchase amount per user
-- - Number of purchases
-- - Average time between events
-- - Most common event type

-- YOUR CODE HERE


-- Bonus 4: Sales Trend Analysis
-- TODO: For each product:
-- - Total sales (sum of daily_sales)
-- - Average daily sales
-- - Best performing day
-- - Growth rate (last day vs first day)

-- YOUR CODE HERE


-- Bonus 5: Log Analysis
-- TODO: Analyze log entries:
-- - Count by log level
-- - Extract all error messages
-- - Find entries with specific metadata
-- - Time between errors

-- YOUR CODE HERE


-- =========================================================
-- CLEANUP (Optional)
-- =========================================================

-- Uncomment to clean up
-- DROP DATABASE IF EXISTS day9_higher_order_functions CASCADE;


-- =========================================================
-- KEY LEARNINGS CHECKLIST
-- =========================================================
-- [ ] Mastered TRANSFORM for array transformations
-- [ ] Used FILTER for conditional array filtering
-- [ ] Applied AGGREGATE for custom reductions
-- [ ] Understood EXISTS and FORALL
-- [ ] Worked with array functions
-- [ ] Used EXPLODE and FLATTEN
-- [ ] Accessed struct fields
-- [ ] Manipulated maps
-- [ ] Combined multiple higher-order functions
-- [ ] Applied to real-world scenarios

