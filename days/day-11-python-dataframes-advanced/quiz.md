# Day 11 Quiz: Python DataFrames Advanced

## Instructions
- 15 multiple choice questions
- Each question has one correct answer
- Answers are at the bottom
- Try to answer without looking at the answers first!

---

## Questions

### Question 1
Which join type returns only rows from the left DataFrame that have matches in the right DataFrame, but only includes columns from the left DataFrame?

A) Inner join  
B) Left join  
C) Left semi join  
D) Left anti join

### Question 2
What is the difference between `rank()` and `dense_rank()` window functions?

A) `rank()` is faster than `dense_rank()`  
B) `rank()` leaves gaps after ties, `dense_rank()` does not  
C) `rank()` requires orderBy, `dense_rank()` does not  
D) They are aliases for the same function

### Question 3
Which of the following is the correct syntax for a broadcast join?

A) `df1.join(df2.broadcast(), "id")`  
B) `df1.join(broadcast(df2), "id")`  
C) `df1.broadcastJoin(df2, "id")`  
D) `df1.join(df2, "id", broadcast=True)`

### Question 4
What does `rowsBetween(-2, 0)` specify in a window frame?

A) 2 rows after the current row  
B) Current row and 2 rows before  
C) 2 rows before and 2 rows after  
D) All rows in the partition

### Question 5
Why are Pandas UDFs faster than standard UDFs?

A) They use Apache Arrow for vectorized processing  
B) They are written in C++  
C) They don't support null values  
D) They can only process numeric data

### Question 6
Which join type creates a Cartesian product of two DataFrames?

A) Inner join  
B) Outer join  
C) Cross join  
D) Semi join

### Question 7
How do you access a field named "city" from a struct column named "address"?

A) `col("address").city`  
B) `col("address.city")`  
C) `col("address->city")`  
D) `col("address")["city"]`

### Question 8
What is the purpose of a left anti join?

A) Returns all rows from both DataFrames  
B) Returns rows from left that have matches in right  
C) Returns rows from left that DON'T have matches in right  
D) Returns only matching rows

### Question 9
Which function is used to convert an array column into multiple rows?

A) `expand()`  
B) `explode()`  
C) `flatten()`  
D) `unnest()`

### Question 10
What does `Window.unboundedPreceding` represent?

A) The first row in the entire DataFrame  
B) The first row in the current partition  
C) All rows before the current row  
D) The current row only

### Question 11
How do you handle duplicate column names after a join?

A) Spark automatically renames them  
B) Use aliases: `df1.alias("a").join(df2.alias("b"), ...)`  
C) Drop one of the columns before joining  
D) Joins with duplicate columns are not allowed

### Question 12
Which window function would you use to calculate a running total?

A) `sum().over(window_spec)` with appropriate frame  
B) `cumsum().over(window_spec)`  
C) `running_sum().over(window_spec)`  
D) `total().over(window_spec)`

### Question 13
What is the difference between `repartition()` and `coalesce()`?

A) `repartition()` can only increase partitions, `coalesce()` can only decrease  
B) `repartition()` does full shuffle, `coalesce()` avoids full shuffle  
C) They are aliases for the same function  
D) `coalesce()` is deprecated, use `repartition()` instead

### Question 14
Which function creates an array from aggregated values?

A) `array_agg()`  
B) `collect_list()`  
C) `group_array()`  
D) `aggregate_array()`

### Question 15
What is the correct way to create a Pandas UDF?

A) `@udf(DoubleType())`  
B) `@pandas_udf(DoubleType())`  
C) `@vectorized_udf(DoubleType())`  
D) `@arrow_udf(DoubleType())`

---

## Answers

### Answer 1
**C) Left semi join**

Explanation: Left semi join returns rows from the left DataFrame that have matches in the right DataFrame, but only includes columns from the left DataFrame. It's like a filtered version of the left DataFrame.

### Answer 2
**B) `rank()` leaves gaps after ties, `dense_rank()` does not**

Explanation: If two rows tie for rank 2, `rank()` assigns both rank 2 and the next rank is 4 (leaving a gap). `dense_rank()` assigns both rank 2 and the next rank is 3 (no gap).

### Answer 3
**B) `df1.join(broadcast(df2), "id")`**

Explanation: Use the `broadcast()` function from `pyspark.sql.functions` to wrap the smaller DataFrame in the join.

### Answer 4
**B) Current row and 2 rows before**

Explanation: `rowsBetween(-2, 0)` specifies a frame from 2 rows before (-2) to the current row (0), inclusive.

### Answer 5
**A) They use Apache Arrow for vectorized processing**

Explanation: Pandas UDFs use Apache Arrow to transfer data efficiently and process data in batches (vectorized), making them 10-100x faster than standard UDFs.

### Answer 6
**C) Cross join**

Explanation: Cross join creates a Cartesian product, combining every row from the left DataFrame with every row from the right DataFrame. This can be very expensive.

### Answer 7
**B) `col("address.city")`**

Explanation: Use dot notation within the string to access struct fields: `col("address.city")`.

### Answer 8
**C) Returns rows from left that DON'T have matches in right**

Explanation: Left anti join is the opposite of left semi join. It returns rows from the left DataFrame that don't have matches in the right DataFrame. Useful for finding missing data.

### Answer 9
**B) `explode()`**

Explanation: `explode()` converts an array column into multiple rows, one row per array element.

### Answer 10
**B) The first row in the current partition**

Explanation: `Window.unboundedPreceding` represents the start of the current partition, not the entire DataFrame. Windows are always scoped to partitions.

### Answer 11
**B) Use aliases: `df1.alias("a").join(df2.alias("b"), ...)`**

Explanation: Use aliases to disambiguate columns: `df1.alias("a").join(df2.alias("b"), ...)` then select with `col("a.id")` and `col("b.id")`.

### Answer 12
**A) `sum().over(window_spec)` with appropriate frame**

Explanation: Use `sum()` with a window specification that includes `rowsBetween(Window.unboundedPreceding, Window.currentRow)` to calculate a running total.

### Answer 13
**B) `repartition()` does full shuffle, `coalesce()` avoids full shuffle**

Explanation: `repartition()` performs a full shuffle and can increase or decrease partitions. `coalesce()` only decreases partitions and avoids a full shuffle, making it more efficient for reducing partitions.

### Answer 14
**B) `collect_list()`**

Explanation: `collect_list()` aggregates values into an array. Use `collect_set()` for unique values only.

### Answer 15
**B) `@pandas_udf(DoubleType())`**

Explanation: Use the `@pandas_udf` decorator with the return type to create a Pandas UDF.

---

## Scoring

- 13-15 correct: Excellent! You have mastered advanced DataFrame operations.
- 10-12 correct: Good job! Review the topics you missed.
- 7-9 correct: Fair. Practice more with joins and window functions.
- Below 7: Review the Day 11 materials and practice the exercises.

---

## Key Concepts to Remember

1. **Join Types**: Inner, left, right, outer, semi, anti, cross - know when to use each
2. **Broadcast Joins**: Use for small DataFrames to avoid shuffles
3. **Window Functions**: Ranking (row_number, rank, dense_rank), analytic (lag, lead), aggregates
4. **Window Frames**: rowsBetween vs rangeBetween
5. **Pandas UDFs**: Much faster than standard UDFs (10-100x)
6. **Array Operations**: explode, collect_list, array_contains
7. **Struct Access**: Use dot notation col("struct.field")
8. **Performance**: Cache frequently used DataFrames, broadcast small tables
9. **Aliases**: Use to handle duplicate column names in joins
10. **Optimization**: repartition for parallelism, coalesce for output
