-- Day 9: Advanced SQL - Higher-Order Functions - Solutions
-- =========================================================

USE day9_higher_order_functions;

-- =========================================================
-- PART 1: TRANSFORM FUNCTION
-- =========================================================

-- Exercise 1: Basic TRANSFORM
SELECT 
    customer_name,
    order_amounts,
    TRANSFORM(order_amounts, amt -> amt * 2) as doubled_amounts
FROM customer_orders;

-- Exercise 2: TRANSFORM with Tax
SELECT 
    customer_name,
    order_amounts,
    TRANSFORM(order_amounts, amt -> amt * 1.10) as amounts_with_tax
FROM customer_orders;

-- Exercise 3: TRANSFORM with Index
SELECT 
    customer_name,
    order_ids,
    TRANSFORM(order_ids, (id, idx) -> concat('Order_', idx + 1)) as order_labels
FROM customer_orders;

-- Exercise 4: TRANSFORM Prices
SELECT 
    product_name,
    prices,
    TRANSFORM(prices, p -> p * 0.85) as discounted_prices
FROM products;

-- Exercise 5: TRANSFORM Strings
SELECT 
    product_name,
    tags,
    TRANSFORM(tags, t -> upper(t)) as uppercase_tags
FROM products;

-- =========================================================
-- PART 2: FILTER FUNCTION
-- =========================================================

-- Exercise 6: Basic FILTER
SELECT 
    customer_name,
    order_amounts,
    FILTER(order_amounts, amt -> amt > 200) as large_orders
FROM customer_orders;

-- Exercise 7: FILTER with Multiple Conditions
SELECT 
    customer_name,
    order_amounts,
    order_statuses,
    FILTER(order_amounts, amt -> amt > 150) as filtered_amounts,
    FILTER(order_statuses, status -> status = 'Completed') as completed_statuses
FROM customer_orders;

-- Exercise 8: FILTER Reviews
SELECT 
    product_name,
    FILTER(reviews, r -> r.verified = true AND r.rating >= 4) as good_verified_reviews
FROM products;

-- Exercise 9: FILTER Events
SELECT 
    username,
    FILTER(events, e -> e.event_type = 'purchase') as purchase_events
FROM user_events;

-- Exercise 10: FILTER by Date
SELECT 
    customer_name,
    FILTER(order_dates, d -> d >= '2024-02-01' AND d < '2024-03-01') as february_orders
FROM customer_orders;

-- =========================================================
-- PART 3: AGGREGATE FUNCTION
-- =========================================================

-- Exercise 11: Sum with AGGREGATE
SELECT 
    customer_name,
    AGGREGATE(order_amounts, 0.0, (acc, amt) -> acc + amt) as total_spent
FROM customer_orders;

-- Exercise 12: Average with AGGREGATE
SELECT 
    customer_name,
    AGGREGATE(
        order_amounts,
        struct(0.0 as sum, 0 as count),
        (acc, amt) -> struct(acc.sum + amt as sum, acc.count + 1 as count),
        acc -> acc.sum / acc.count
    ) as average_order
FROM customer_orders;

-- Exercise 13: Product with AGGREGATE
SELECT 
    product_name,
    AGGREGATE(daily_sales, 1, (acc, sales) -> acc * sales) as sales_product
FROM sales_data;

-- Exercise 14: Concatenate with AGGREGATE
SELECT 
    product_name,
    AGGREGATE(tags, '', (acc, tag) -> concat(acc, IF(acc = '', '', ', '), tag)) as tags_string
FROM products;

-- Exercise 15: Complex AGGREGATE
SELECT 
    product_name,
    prices,
    AGGREGATE(
        prices,
        struct(0.0 as sum, 0 as count),
        (acc, price) -> struct(acc.sum + price as sum, acc.count + 1 as count),
        acc -> round(acc.sum / acc.count, 2)
    ) as weighted_avg_price
FROM products;

-- =========================================================
-- PART 4: EXISTS AND FORALL
-- =========================================================

-- Exercise 16: EXISTS - Check for Large Orders
SELECT 
    customer_name,
    EXISTS(order_amounts, amt -> amt > 300) as has_large_order
FROM customer_orders;

-- Exercise 17: EXISTS - Check for Status
SELECT 
    customer_name,
    EXISTS(order_statuses, status -> status = 'Pending') as has_pending_orders
FROM customer_orders;

-- Exercise 18: FORALL - All Completed
SELECT 
    customer_name,
    FORALL(order_statuses, status -> status = 'Completed') as all_completed
FROM customer_orders;

-- Exercise 19: FORALL - All Positive
SELECT 
    product_name,
    FORALL(daily_sales, sales -> sales > 0) as all_positive_sales
FROM sales_data;

-- Exercise 20: EXISTS vs FORALL
SELECT 
    product_name,
    EXISTS(reviews, r -> r.rating >= 4) as has_high_rating,
    FORALL(reviews, r -> r.rating >= 4) as all_high_ratings
FROM products;

-- =========================================================
-- PART 5: ARRAY FUNCTIONS
-- =========================================================

-- Exercise 21: array_contains
SELECT 
    product_name,
    array_contains(tags, 'laptop') as has_laptop_tag
FROM products;

-- Exercise 22: array_distinct
SELECT 
    product_name,
    regions,
    array_distinct(regions) as unique_regions
FROM sales_data;

-- Exercise 23: array_union
SELECT 
    array_union(
        (SELECT order_ids FROM customer_orders WHERE customer_id = 101),
        (SELECT order_ids FROM customer_orders WHERE customer_id = 102)
    ) as combined_orders;

-- Exercise 24: array_intersect
SELECT 
    p1.product_name as product1,
    p2.product_name as product2,
    array_intersect(p1.tags, p2.tags) as common_tags
FROM products p1
CROSS JOIN products p2
WHERE p1.product_id < p2.product_id;

-- Exercise 25: array_sort
SELECT 
    customer_name,
    order_amounts,
    array_sort(order_amounts) as sorted_amounts
FROM customer_orders;

-- Exercise 26: array_min and array_max
SELECT 
    customer_name,
    array_min(order_amounts) as min_order,
    array_max(order_amounts) as max_order
FROM customer_orders;

-- Exercise 27: array_position
SELECT 
    customer_name,
    order_statuses,
    array_position(order_statuses, 'Completed') as first_completed_position
FROM customer_orders;

-- Exercise 28: array_remove
SELECT 
    customer_name,
    order_statuses,
    array_remove(order_statuses, 'Pending') as without_pending
FROM customer_orders;

-- Exercise 29: array_join
SELECT 
    product_name,
    array_join(tags, ' | ') as tags_string
FROM products;

-- Exercise 30: size
SELECT 
    customer_name,
    size(order_ids) as order_count
FROM customer_orders;

-- =========================================================
-- PART 6: EXPLODE AND FLATTEN
-- =========================================================

-- Exercise 31: Basic EXPLODE
SELECT 
    customer_name,
    EXPLODE(order_ids) as order_id
FROM customer_orders;

-- Exercise 32: EXPLODE with JOIN
SELECT 
    customer_name,
    exploded_amount
FROM customer_orders
LATERAL VIEW EXPLODE(order_amounts) as exploded_amount;

-- Exercise 33: POSEXPLODE
SELECT 
    customer_name,
    pos,
    order_id
FROM customer_orders
LATERAL VIEW POSEXPLODE(order_ids) as pos, order_id;

-- Exercise 34: EXPLODE_OUTER
SELECT 
    customer_name,
    EXPLODE_OUTER(order_ids) as order_id
FROM customer_orders;

-- Exercise 35: FLATTEN
SELECT 
    username,
    FLATTEN(daily_events) as all_events
FROM user_events;

-- =========================================================
-- PART 7: WORKING WITH STRUCTS
-- =========================================================

-- Exercise 36: Access Struct Fields
SELECT 
    product_name,
    TRANSFORM(reviews, r -> struct(r.rating as rating, r.comment as comment)) as review_info
FROM products;

-- Exercise 37: TRANSFORM Structs
SELECT 
    product_name,
    TRANSFORM(reviews, r -> struct(r.rating + 1 as rating, r.comment as comment, r.verified as verified)) as boosted_reviews
FROM products;

-- Exercise 38: FILTER Structs
SELECT 
    application,
    FILTER(entries, e -> e.level = 'ERROR') as error_entries
FROM log_entries;

-- Exercise 39: AGGREGATE Structs
SELECT 
    product_name,
    AGGREGATE(
        reviews,
        struct(0.0 as sum, 0 as count),
        (acc, r) -> struct(acc.sum + r.rating as sum, acc.count + 1 as count),
        acc -> round(acc.sum / acc.count, 2)
    ) as avg_rating
FROM products;

-- Exercise 40: EXPLODE Structs
SELECT 
    product_name,
    review.rating,
    review.comment,
    review.verified
FROM products
LATERAL VIEW EXPLODE(reviews) as review;

-- =========================================================
-- PART 8: WORKING WITH MAPS
-- =========================================================

-- Exercise 41: map_keys
SELECT 
    product_name,
    map_keys(attributes) as attribute_keys
FROM products;

-- Exercise 42: map_values
SELECT 
    product_name,
    map_values(attributes) as attribute_values
FROM products;

-- Exercise 43: map_contains_key
SELECT 
    product_name,
    map_contains_key(attributes, 'warranty') as has_warranty
FROM products;

-- Exercise 44: Access Map Value
SELECT 
    product_name,
    attributes['brand'] as brand
FROM products;

-- Exercise 45: transform_values
SELECT 
    product_name,
    transform_values(attributes, (k, v) -> upper(v)) as uppercase_attributes
FROM products;

-- =========================================================
-- PART 9: COMBINING FUNCTIONS
-- =========================================================

-- Exercise 46: TRANSFORM + FILTER
SELECT 
    customer_name,
    FILTER(
        TRANSFORM(order_amounts, amt -> amt * 1.10),
        amt -> amt > 200
    ) as large_orders_with_tax
FROM customer_orders;

-- Exercise 47: FILTER + AGGREGATE
SELECT 
    customer_name,
    AGGREGATE(
        FILTER(order_amounts, amt -> amt > 200),
        0.0,
        (acc, amt) -> acc + amt
    ) as total_large_orders
FROM customer_orders;

-- Exercise 48: TRANSFORM + EXPLODE
SELECT 
    customer_name,
    EXPLODE(TRANSFORM(order_amounts, amt -> amt * 2)) as doubled_amount
FROM customer_orders;

-- Exercise 49: FILTER + EXISTS
SELECT 
    customer_name,
    EXISTS(
        FILTER(order_amounts, amt -> amt > 200),
        amt -> true
    ) as has_large_completed_order
FROM customer_orders
WHERE EXISTS(order_statuses, status -> status = 'Completed');

-- Exercise 50: Complex Combination
SELECT 
    customer_name,
    AGGREGATE(
        FILTER(
            TRANSFORM(order_amounts, amt -> amt * 1.10),
            amt -> amt > 200
        ),
        0.0,
        (acc, amt) -> acc + amt
    ) as total_large_orders_with_tax
FROM customer_orders;

-- =========================================================
-- BONUS SOLUTIONS
-- =========================================================

-- Bonus 1: E-commerce Analytics
SELECT 
    customer_name,
    AGGREGATE(order_amounts, 0.0, (acc, amt) -> acc + amt) as total_revenue,
    AGGREGATE(
        order_amounts,
        struct(0.0 as sum, 0 as count),
        (acc, amt) -> struct(acc.sum + amt as sum, acc.count + 1 as count),
        acc -> round(acc.sum / acc.count, 2)
    ) as avg_order_value,
    size(FILTER(order_amounts, amt -> amt > 250)) as high_value_orders,
    round(
        size(FILTER(order_statuses, status -> status = 'Completed')) * 100.0 / size(order_statuses),
        2
    ) as completion_rate
FROM customer_orders;

-- Bonus 2: Product Performance
SELECT 
    product_name,
    AGGREGATE(
        prices,
        struct(0.0 as sum, 0 as count),
        (acc, p) -> struct(acc.sum + p as sum, acc.count + 1 as count),
        acc -> round(acc.sum / acc.count, 2)
    ) as avg_price,
    size(FILTER(reviews, r -> r.rating = 5)) as five_star_count,
    round(
        size(FILTER(reviews, r -> r.verified = true)) * 100.0 / size(reviews),
        2
    ) as verified_percentage,
    tags[0] as most_common_tag
FROM products;

-- Bonus 3: User Behavior Analysis
SELECT 
    username,
    AGGREGATE(
        FILTER(events, e -> e.event_type = 'purchase'),
        0.0,
        (acc, e) -> acc + e.amount
    ) as total_purchase_amount,
    size(FILTER(events, e -> e.event_type = 'purchase')) as purchase_count,
    size(events) as total_events
FROM user_events;

-- Bonus 4: Sales Trend Analysis
SELECT 
    product_name,
    AGGREGATE(daily_sales, 0, (acc, sales) -> acc + sales) as total_sales,
    AGGREGATE(
        daily_sales,
        struct(0 as sum, 0 as count),
        (acc, sales) -> struct(acc.sum + sales as sum, acc.count + 1 as count),
        acc -> round(acc.sum * 1.0 / acc.count, 2)
    ) as avg_daily_sales,
    array_max(daily_sales) as best_day_sales,
    round(
        (daily_sales[size(daily_sales) - 1] - daily_sales[0]) * 100.0 / daily_sales[0],
        2
    ) as growth_rate
FROM sales_data;

-- Bonus 5: Log Analysis
SELECT 
    application,
    size(FILTER(entries, e -> e.level = 'ERROR')) as error_count,
    size(FILTER(entries, e -> e.level = 'WARN')) as warn_count,
    size(FILTER(entries, e -> e.level = 'INFO')) as info_count,
    TRANSFORM(
        FILTER(entries, e -> e.level = 'ERROR'),
        e -> e.message
    ) as error_messages
FROM log_entries;

-- =========================================================
-- KEY LEARNINGS SUMMARY
-- =========================================================
%md
## Key Learnings from Day 9

### Higher-Order Functions
- **TRANSFORM**: Apply function to each element
- **FILTER**: Keep elements matching condition
- **AGGREGATE**: Reduce array to single value
- **EXISTS**: Check if any element matches
- **FORALL**: Check if all elements match

### Array Functions
- **array_contains**: Check membership
- **array_distinct**: Remove duplicates
- **array_union/intersect/except**: Set operations
- **array_sort**: Sort elements
- **array_min/max**: Find extremes
- **size**: Get array length

### Explode Functions
- **EXPLODE**: Array to rows
- **EXPLODE_OUTER**: Keep NULL arrays
- **POSEXPLODE**: With position index
- **FLATTEN**: Unnest nested arrays

### Complex Types
- **Structs**: Access with dot notation
- **Maps**: Access with bracket notation
- **Nested data**: Combine functions

### Best Practices
- Use higher-order functions instead of EXPLODE when possible (more efficient)
- Combine functions for complex transformations
- Use EXISTS/FORALL for validation
- Leverage array functions for common operations

**Next**: Day 10 will introduce Python DataFrames API!

