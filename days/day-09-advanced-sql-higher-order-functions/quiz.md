# Day 9 Quiz: Advanced SQL - Higher-Order Functions

Test your knowledge of higher-order functions, arrays, and complex data types.

---

## Question 1
**What does the TRANSFORM function do?**

A) Transforms table structure  
B) Applies a function to each element in an array  
C) Converts data types  
D) Transforms rows to columns  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Applies a function to each element in an array**

Explanation: TRANSFORM takes an array and a lambda function, applying the function to each element and returning a new array with the transformed values.
</details>

---

## Question 2
**What is the correct syntax for TRANSFORM?**

A) TRANSFORM(array, function)  
B) TRANSFORM(array, x => expression)  
C) TRANSFORM(array, x -> expression)  
D) TRANSFORM(array) AS expression  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) TRANSFORM(array, x -> expression)**

Explanation: The correct syntax uses `->` (not `=>`) for the lambda function. Example: `TRANSFORM(array(1,2,3), x -> x * 2)`.
</details>

---

## Question 3
**What does FILTER do?**

A) Filters table rows  
B) Keeps only array elements that match a condition  
C) Removes NULL values  
D) Filters columns  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Keeps only array elements that match a condition**

Explanation: FILTER takes an array and a boolean condition, returning a new array containing only elements where the condition is true.
</details>

---

## Question 4
**What is the purpose of AGGREGATE?**

A) Group rows like GROUP BY  
B) Reduce an array to a single value using custom logic  
C) Aggregate multiple tables  
D) Calculate sum of array  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Reduce an array to a single value using custom logic**

Explanation: AGGREGATE reduces an array to a single value using an accumulator function. Syntax: `AGGREGATE(array, initial_value, (acc, x) -> expression)`.
</details>

---

## Question 5
**What's the difference between EXISTS and FORALL?**

A) EXISTS is faster  
B) EXISTS checks if any element matches, FORALL checks if all match  
C) FORALL is for arrays, EXISTS is for tables  
D) They are the same  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) EXISTS checks if any element matches, FORALL checks if all match**

Explanation: EXISTS returns true if at least one element matches the condition. FORALL returns true only if all elements match the condition.
</details>

---

## Question 6
**What does EXPLODE do?**

A) Deletes array elements  
B) Converts an array to multiple rows (one row per element)  
C) Expands table columns  
D) Increases array size  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Converts an array to multiple rows (one row per element)**

Explanation: EXPLODE takes an array column and creates a new row for each array element. It's the opposite of COLLECT_LIST.
</details>

---

## Question 7
**What's the difference between EXPLODE and FLATTEN?**

A) EXPLODE is faster  
B) EXPLODE converts array to rows, FLATTEN unnests nested arrays  
C) FLATTEN is for maps, EXPLODE is for arrays  
D) They are the same  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) EXPLODE converts array to rows, FLATTEN unnests nested arrays**

Explanation: EXPLODE creates rows from array elements. FLATTEN takes nested arrays (array of arrays) and flattens them into a single-level array.
</details>

---

## Question 8
**How do you access a struct field?**

A) struct['field']  
B) struct.field  
C) struct->field  
D) GET(struct, 'field')  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) struct.field**

Explanation: Struct fields are accessed using dot notation. Example: `person.name` or `review.rating`.
</details>

---

## Question 9
**How do you check if an array contains a specific value?**

A) CONTAINS(array, value)  
B) array_contains(array, value)  
C) array.contains(value)  
D) IN(value, array)  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) array_contains(array, value)**

Explanation: The array_contains function checks if a value exists in an array. Returns true if found, false otherwise.
</details>

---

## Question 10
**What does array_distinct do?**

A) Sorts array elements  
B) Removes duplicate elements from an array  
C) Finds distinct arrays  
D) Counts unique elements  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Removes duplicate elements from an array**

Explanation: array_distinct returns a new array with duplicate elements removed, keeping only unique values.
</details>

---

## Question 11
**What's the difference between EXPLODE and EXPLODE_OUTER?**

A) EXPLODE_OUTER is faster  
B) EXPLODE_OUTER keeps rows with NULL or empty arrays  
C) EXPLODE_OUTER works with maps  
D) They are the same  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) EXPLODE_OUTER keeps rows with NULL or empty arrays**

Explanation: EXPLODE drops rows where the array is NULL or empty. EXPLODE_OUTER keeps these rows, returning NULL for the exploded column.
</details>

---

## Question 12
**How do you get all keys from a map?**

A) map.keys()  
B) map_keys(map)  
C) KEYS(map)  
D) map['keys']  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) map_keys(map)**

Explanation: The map_keys function returns an array of all keys in the map. Similarly, map_values returns all values.
</details>

---

## Question 13
**What does size() return for an array?**

A) The memory size in bytes  
B) The number of elements in the array  
C) The maximum value  
D) The array dimensions  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) The number of elements in the array**

Explanation: The size function returns the number of elements in an array. Returns 0 for empty arrays and NULL for NULL arrays.
</details>

---

## Question 14
**Why use higher-order functions instead of EXPLODE?**

A) They are easier to write  
B) They are more efficient (no row explosion)  
C) EXPLODE doesn't work with arrays  
D) Higher-order functions are newer  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) They are more efficient (no row explosion)**

Explanation: Higher-order functions process arrays without creating intermediate rows, making them more efficient than EXPLODE + aggregate + GROUP BY patterns.
</details>

---

## Question 15
**What does array_union do?**

A) Joins two tables  
B) Combines two arrays, removing duplicates  
C) Finds common elements  
D) Merges array columns  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Combines two arrays, removing duplicates**

Explanation: array_union combines elements from two arrays and removes duplicates. For keeping duplicates, use array_concat.
</details>

---

## Scoring Guide

- **13-15 correct**: Excellent! You've mastered higher-order functions.
- **10-12 correct**: Good job! Review the questions you missed.
- **7-9 correct**: Fair. Review the Day 9 materials and practice more.
- **Below 7**: Review the Day 9 README and complete more exercises.

---

## Key Concepts to Remember

1. **TRANSFORM**: Apply function to each element (`TRANSFORM(array, x -> expression)`)
2. **FILTER**: Keep elements matching condition (`FILTER(array, x -> condition)`)
3. **AGGREGATE**: Reduce array to single value (`AGGREGATE(array, init, (acc, x) -> expr)`)
4. **EXISTS**: Check if any element matches
5. **FORALL**: Check if all elements match
6. **EXPLODE**: Array to rows
7. **FLATTEN**: Unnest nested arrays
8. **Struct access**: Use dot notation (`struct.field`)
9. **Map access**: Use bracket notation (`map['key']`)
10. **array_contains**: Check membership

---

## Common Exam Scenarios

### Scenario 1: Transform Array
**Question**: Double all prices in a price array.

**Answer**:
```sql
SELECT TRANSFORM(prices, p -> p * 2) as doubled_prices
FROM products;
```

### Scenario 2: Filter Array
**Question**: Keep only orders greater than $100.

**Answer**:
```sql
SELECT FILTER(order_amounts, amt -> amt > 100) as large_orders
FROM customer_orders;
```

### Scenario 3: Sum Array
**Question**: Calculate total of all order amounts.

**Answer**:
```sql
SELECT AGGREGATE(order_amounts, 0.0, (acc, amt) -> acc + amt) as total
FROM customer_orders;
```

### Scenario 4: Check Array
**Question**: Check if customer has any pending orders.

**Answer**:
```sql
SELECT EXISTS(order_statuses, status -> status = 'Pending') as has_pending
FROM customer_orders;
```

---

## Practice Tips

1. **Start with TRANSFORM** (simplest higher-order function)
2. **Practice FILTER** with different conditions
3. **Master AGGREGATE** for custom reductions
4. **Understand EXISTS vs FORALL**
5. **Learn array functions** (array_contains, array_distinct, etc.)
6. **Practice EXPLODE** for unnesting
7. **Work with structs** and maps
8. **Combine functions** for complex transformations

---

## Common Mistakes to Avoid

- Using `=>` instead of `->` in lambda functions
- Forgetting that EXPLODE creates rows (not columns)
- Confusing EXISTS (any) with FORALL (all)
- Not using EXPLODE_OUTER when NULL arrays matter
- Forgetting initial value in AGGREGATE
- Using EXPLODE when higher-order functions would be more efficient
- Incorrect struct/map access syntax

---

## Function Quick Reference

**Higher-Order Functions**:
- `TRANSFORM(array, x -> expr)` - Transform each element
- `FILTER(array, x -> condition)` - Keep matching elements
- `AGGREGATE(array, init, (acc, x) -> expr)` - Reduce to single value
- `EXISTS(array, x -> condition)` - Check if any match
- `FORALL(array, x -> condition)` - Check if all match

**Array Functions**:
- `array_contains(array, value)` - Check membership
- `array_distinct(array)` - Remove duplicates
- `array_union(arr1, arr2)` - Combine arrays
- `array_intersect(arr1, arr2)` - Common elements
- `size(array)` - Get length

**Explode Functions**:
- `EXPLODE(array)` - Array to rows
- `EXPLODE_OUTER(array)` - Keep NULL arrays
- `FLATTEN(nested_array)` - Unnest arrays

---

## Next Steps

- Review any questions you got wrong
- Re-read relevant sections in Day 9 README
- Practice with complex nested data
- Complete bonus exercises
- Move on to Day 10: Python DataFrames Basics

Good luck! 🚀
