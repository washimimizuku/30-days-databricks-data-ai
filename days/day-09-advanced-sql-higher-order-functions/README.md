# Day 9: Advanced SQL - Higher-Order Functions

## Learning Objectives

By the end of today, you will:
- Understand higher-order functions in Spark SQL
- Master array manipulation functions (TRANSFORM, FILTER, AGGREGATE)
- Work with complex data types (arrays, structs, maps)
- Use EXPLODE and FLATTEN to unnest data
- Apply array functions (array_contains, array_distinct, etc.)
- Combine higher-order functions with window functions
- Handle nested and semi-structured data

## What Are Higher-Order Functions?

**Higher-order functions** are functions that take other functions as arguments, allowing you to apply transformations to complex data types like arrays without exploding them.

**Why Use Them?**
- Process arrays without EXPLODE (more efficient)
- Apply transformations element-by-element
- Filter array elements based on conditions
- Aggregate array values with custom logic

**Key Functions**:
- `TRANSFORM`: Apply function to each array element
- `FILTER`: Keep only elements matching condition
- `AGGREGATE`: Reduce array to single value
- `EXISTS`: Check if any element matches condition
- `FORALL`: Check if all elements match condition

## Topics Covered

### 1. TRANSFORM Function

**Purpose**: Apply a transformation to each element in an array

**Syntax**:
```sql
TRANSFORM(array, element -> expression)
```

**Examples**:
```sql
-- Double each element
SELECT TRANSFORM(array(1, 2, 3), x -> x * 2);
-- Result: [2, 4, 6]

-- Convert to uppercase
SELECT TRANSFORM(array('hello', 'world'), x -> upper(x));
-- Result: ['HELLO', 'WORLD']

-- Add index (using two parameters)
SELECT TRANSFORM(array('a', 'b', 'c'), (x, i) -> concat(x, '_', i));
-- Result: ['a_0', 'b_1', 'c_2']

-- Complex transformation
SELECT 
    product_id,
    TRANSFORM(prices, p -> p * 1.1) as prices_with_tax
FROM products;
```

**With Table Data**:
```sql
-- Increase all order amounts by 10%
SELECT 
    customer_id,
    order_amounts,
    TRANSFORM(order_amounts, amt -> amt * 1.10) as adjusted_amounts
FROM customer_orders;
```

### 2. FILTER Function

**Purpose**: Keep only array elements that match a condition

**Syntax**:
```sql
FILTER(array, element -> boolean_condition)
```

**Examples**:
```sql
-- Keep only positive numbers
SELECT FILTER(array(-1, 2, -3, 4), x -> x > 0);
-- Result: [2, 4]

-- Keep only long strings
SELECT FILTER(array('hi', 'hello', 'hey'), x -> length(x) > 3);
-- Result: ['hello']

-- Filter with table data
SELECT 
    customer_id,
    FILTER(order_amounts, amt -> amt > 100) as large_orders
FROM customer_orders;

-- Filter with complex condition
SELECT 
    product_id,
    FILTER(reviews, r -> r.rating >= 4 AND r.verified = true) as good_verified_reviews
FROM products;
```

### 3. AGGREGATE Function

**Purpose**: Reduce an array to a single value using custom logic

**Syntax**:
```sql
AGGREGATE(array, initial_value, (accumulator, element) -> expression, final_expression)
```

**Examples**:
```sql
-- Sum array elements
SELECT AGGREGATE(array(1, 2, 3, 4), 0, (acc, x) -> acc + x);
-- Result: 10

-- Product of array elements
SELECT AGGREGATE(array(2, 3, 4), 1, (acc, x) -> acc * x);
-- Result: 24

-- Concatenate strings
SELECT AGGREGATE(array('Hello', 'World'), '', (acc, x) -> concat(acc, ' ', x));
-- Result: ' Hello World'

-- With final transformation
SELECT AGGREGATE(
    array(1, 2, 3, 4), 
    0, 
    (acc, x) -> acc + x,
    acc -> acc / 4.0
);
-- Result: 2.5 (average)

-- Complex aggregation
SELECT 
    customer_id,
    AGGREGATE(
        order_amounts,
        0.0,
        (total, amt) -> total + amt,
        total -> round(total, 2)
    ) as total_spent
FROM customer_orders;
```

### 4. EXISTS and FORALL

**EXISTS**: Check if any element matches condition
```sql
-- Check if any element is positive
SELECT EXISTS(array(-1, 2, -3), x -> x > 0);
-- Result: true

-- Check if customer has any large order
SELECT 
    customer_id,
    EXISTS(order_amounts, amt -> amt > 1000) as has_large_order
FROM customer_orders;
```

**FORALL**: Check if all elements match condition
```sql
-- Check if all elements are positive
SELECT FORALL(array(1, 2, 3), x -> x > 0);
-- Result: true

-- Check if all orders are completed
SELECT 
    customer_id,
    FORALL(order_statuses, status -> status = 'Completed') as all_completed
FROM customer_orders;
```

### 5. Array Functions

**Array Creation**:
```sql
-- Create array
SELECT array(1, 2, 3) as numbers;

-- Array from columns
SELECT array(col1, col2, col3) as values FROM table;

-- Array with COLLECT_LIST
SELECT customer_id, COLLECT_LIST(order_id) as order_ids
FROM orders
GROUP BY customer_id;
```

**Array Operations**:
```sql
-- Array size
SELECT size(array(1, 2, 3));  -- Result: 3

-- Array contains
SELECT array_contains(array(1, 2, 3), 2);  -- Result: true

-- Array distinct
SELECT array_distinct(array(1, 2, 2, 3, 3));  -- Result: [1, 2, 3]

-- Array union
SELECT array_union(array(1, 2), array(2, 3));  -- Result: [1, 2, 3]

-- Array intersect
SELECT array_intersect(array(1, 2, 3), array(2, 3, 4));  -- Result: [2, 3]

-- Array except
SELECT array_except(array(1, 2, 3), array(2, 3));  -- Result: [1]

-- Array sort
SELECT array_sort(array(3, 1, 2));  -- Result: [1, 2, 3]

-- Array min/max
SELECT array_min(array(3, 1, 2));  -- Result: 1
SELECT array_max(array(3, 1, 2));  -- Result: 3

-- Array position
SELECT array_position(array('a', 'b', 'c'), 'b');  -- Result: 2

-- Array remove
SELECT array_remove(array(1, 2, 3, 2), 2);  -- Result: [1, 3]

-- Array repeat
SELECT array_repeat('x', 3);  -- Result: ['x', 'x', 'x']

-- Array join
SELECT array_join(array('a', 'b', 'c'), '-');  -- Result: 'a-b-c'
```

### 6. EXPLODE and FLATTEN

**EXPLODE**: Convert array to rows
```sql
-- Basic explode
SELECT EXPLODE(array(1, 2, 3)) as value;
-- Result: 3 rows with values 1, 2, 3

-- Explode with table
SELECT 
    customer_id,
    EXPLODE(order_ids) as order_id
FROM customer_orders;

-- Explode with position
SELECT 
    customer_id,
    POSEXPLODE(order_ids) as (position, order_id)
FROM customer_orders;

-- Explode map
SELECT 
    EXPLODE(map('a', 1, 'b', 2)) as (key, value);
-- Result: 2 rows: ('a', 1) and ('b', 2)
```

**FLATTEN**: Flatten nested arrays
```sql
-- Flatten nested array
SELECT FLATTEN(array(array(1, 2), array(3, 4)));
-- Result: [1, 2, 3, 4]

-- Flatten with table
SELECT 
    customer_id,
    FLATTEN(nested_order_ids) as all_order_ids
FROM customer_data;
```

**EXPLODE_OUTER**: Like EXPLODE but keeps NULL arrays
```sql
-- Regular EXPLODE drops rows with NULL arrays
SELECT customer_id, EXPLODE(order_ids) as order_id
FROM customers;

-- EXPLODE_OUTER keeps them
SELECT customer_id, EXPLODE_OUTER(order_ids) as order_id
FROM customers;
```

### 7. Working with Structs

**Struct Creation**:
```sql
-- Create struct
SELECT struct('John' as name, 30 as age) as person;

-- Create named struct
SELECT named_struct('name', 'John', 'age', 30) as person;

-- Access struct fields
SELECT person.name, person.age
FROM (SELECT struct('John' as name, 30 as age) as person);
```

**Array of Structs**:
```sql
-- Create array of structs
SELECT array(
    struct('John' as name, 30 as age),
    struct('Jane' as name, 25 as age)
) as people;

-- Transform array of structs
SELECT TRANSFORM(
    people,
    p -> struct(p.name as name, p.age + 1 as age)
) as people_next_year
FROM employees;

-- Filter array of structs
SELECT FILTER(
    orders,
    o -> o.amount > 100 AND o.status = 'Completed'
) as large_completed_orders
FROM customer_data;
```

### 8. Working with Maps

**Map Creation**:
```sql
-- Create map
SELECT map('key1', 'value1', 'key2', 'value2') as my_map;

-- Map from arrays
SELECT map_from_arrays(
    array('a', 'b', 'c'),
    array(1, 2, 3)
) as my_map;
```

**Map Operations**:
```sql
-- Map keys
SELECT map_keys(map('a', 1, 'b', 2));  -- Result: ['a', 'b']

-- Map values
SELECT map_values(map('a', 1, 'b', 2));  -- Result: [1, 2]

-- Map contains key
SELECT map_contains_key(map('a', 1, 'b', 2), 'a');  -- Result: true

-- Access map value
SELECT my_map['key1'] FROM (SELECT map('key1', 'value1') as my_map);

-- Map concat
SELECT map_concat(map('a', 1), map('b', 2));  -- Result: {'a': 1, 'b': 2}

-- Transform map values
SELECT transform_values(
    map('a', 1, 'b', 2),
    (k, v) -> v * 2
);  -- Result: {'a': 2, 'b': 4}

-- Transform map keys
SELECT transform_keys(
    map('a', 1, 'b', 2),
    (k, v) -> upper(k)
);  -- Result: {'A': 1, 'B': 2}

-- Filter map
SELECT map_filter(
    map('a', 1, 'b', 2, 'c', 3),
    (k, v) -> v > 1
);  -- Result: {'b': 2, 'c': 3}
```

### 9. Combining Functions

**TRANSFORM + FILTER**:
```sql
-- Transform then filter
SELECT 
    customer_id,
    FILTER(
        TRANSFORM(order_amounts, amt -> amt * 1.1),
        amt -> amt > 100
    ) as adjusted_large_orders
FROM customer_orders;
```

**FILTER + AGGREGATE**:
```sql
-- Filter then aggregate
SELECT 
    customer_id,
    AGGREGATE(
        FILTER(order_amounts, amt -> amt > 100),
        0.0,
        (acc, amt) -> acc + amt
    ) as total_large_orders
FROM customer_orders;
```

**TRANSFORM + EXPLODE**:
```sql
-- Transform then explode
SELECT 
    customer_id,
    EXPLODE(TRANSFORM(order_ids, id -> id * 10)) as transformed_id
FROM customer_orders;
```

### 10. Real-World Use Cases

**E-commerce Order Processing**:
```sql
-- Calculate total with discounts
SELECT 
    order_id,
    items,
    AGGREGATE(
        TRANSFORM(items, item -> item.price * item.quantity * (1 - item.discount)),
        0.0,
        (total, amount) -> total + amount
    ) as order_total
FROM orders;
```

**Log Analysis**:
```sql
-- Extract error messages
SELECT 
    log_date,
    FILTER(log_entries, entry -> entry.level = 'ERROR') as errors,
    size(FILTER(log_entries, entry -> entry.level = 'ERROR')) as error_count
FROM application_logs;
```

**User Behavior Analysis**:
```sql
-- Find users with specific behavior pattern
SELECT 
    user_id,
    EXISTS(events, e -> e.type = 'purchase' AND e.amount > 1000) as has_large_purchase,
    FORALL(events, e -> e.timestamp > '2024-01-01') as all_recent
FROM user_events;
```

**Data Quality Checks**:
```sql
-- Validate all values in array
SELECT 
    record_id,
    FORALL(values, v -> v IS NOT NULL AND v > 0) as all_valid,
    FILTER(values, v -> v IS NULL OR v <= 0) as invalid_values
FROM data_records;
```

## Hands-On Exercises

See `exercise.sql` for practical exercises covering:
- TRANSFORM for array transformations
- FILTER for conditional array filtering
- AGGREGATE for custom array reductions
- EXISTS and FORALL for array validation
- Array manipulation functions
- EXPLODE and FLATTEN for unnesting
- Working with structs and maps
- Combining higher-order functions

## Key Takeaways

1. **Higher-order functions process arrays** without exploding them
2. **TRANSFORM applies function** to each element
3. **FILTER keeps elements** matching condition
4. **AGGREGATE reduces array** to single value
5. **EXISTS checks if any** element matches
6. **FORALL checks if all** elements match
7. **EXPLODE converts arrays** to rows
8. **FLATTEN unnests** nested arrays
9. **Structs group related** fields together
10. **Maps store key-value** pairs

## Exam Tips

- **Know TRANSFORM syntax**: `TRANSFORM(array, x -> expression)`
- **Understand FILTER**: Keeps elements where condition is true
- **Master AGGREGATE**: Initial value, accumulator function, optional final function
- **Know array functions**: array_contains, array_distinct, array_union, etc.
- **Understand EXPLODE**: Converts array to rows (one row per element)
- **Know FLATTEN**: Flattens nested arrays
- **Remember EXISTS vs FORALL**: Any vs all
- **Understand struct access**: Use dot notation (struct.field)
- **Know map operations**: map_keys, map_values, map_contains_key
- **Practice combining functions**: TRANSFORM + FILTER, FILTER + AGGREGATE

## Common Exam Questions

1. How do you double each element in an array?
2. How do you filter an array to keep only values > 100?
3. What's the difference between EXPLODE and FLATTEN?
4. How do you check if an array contains a specific value?
5. How do you sum all elements in an array using AGGREGATE?
6. What does EXISTS return?
7. How do you access a struct field?
8. How do you get all keys from a map?
9. What's the difference between EXPLODE and EXPLODE_OUTER?
10. How do you remove duplicates from an array?

## Additional Resources

- [Databricks Higher-Order Functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions-builtin.html#higher-order-functions)
- [Array Functions Reference](https://docs.databricks.com/sql/language-manual/sql-ref-functions-builtin.html#array-functions)
- [Complex Types Guide](https://docs.databricks.com/sql/language-manual/sql-ref-datatypes.html)

## Next Steps

Tomorrow: [Day 10 - Python DataFrames Basics](../day-10-python-dataframes-basics/README.md)

