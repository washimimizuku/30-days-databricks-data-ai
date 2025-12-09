# Day 10 Quiz: Python DataFrames Basics

## Instructions
- 15 multiple choice questions
- Each question has one correct answer
- Answers are at the bottom
- Try to answer without looking at the answers first!

---

## Questions

### Question 1
What is the correct way to create a DataFrame from a list of tuples with columns id, name, age?

A) `df = spark.DataFrame(data, ["id", "name", "age"])`  
B) `df = spark.createDataFrame(data, ["id", "name", "age"])`  
C) `df = spark.create(data, columns=["id", "name", "age"])`  
D) `df = spark.newDataFrame(data, schema=["id", "name", "age"])`

### Question 2
Which of the following is a transformation (lazy operation)?

A) `df.show()`  
B) `df.count()`  
C) `df.filter(col("age") > 25)`  
D) `df.collect()`

### Question 3
What is the correct syntax to read a CSV file with header and inferred schema?

A) `spark.read.csv("file.csv", header=True, inferSchema=True)`  
B) `spark.read("file.csv", format="csv", header=True)`  
C) `spark.csv("file.csv", header=True, schema="infer")`  
D) `spark.readCSV("file.csv", headers=True, infer=True)`

### Question 4
How do you select multiple columns from a DataFrame?

A) `df.select("name", "age", "city")`  
B) `df.select(["name", "age", "city"])`  
C) `df.columns("name", "age", "city")`  
D) Both A and B are correct

### Question 5
What is the correct way to filter rows where salary is greater than 80000 AND department is "Engineering"?

A) `df.filter(col("salary") > 80000 and col("department") == "Engineering")`  
B) `df.filter(col("salary") > 80000 & col("department") == "Engineering")`  
C) `df.filter((col("salary") > 80000) & (col("department") == "Engineering"))`  
D) `df.filter(col("salary") > 80000 && col("department") == "Engineering")`

### Question 6
Which function adds a new column to a DataFrame?

A) `df.addColumn("new_col", value)`  
B) `df.withColumn("new_col", value)`  
C) `df.insertColumn("new_col", value)`  
D) `df.newColumn("new_col", value)`

### Question 7
What does the following code do: `df.groupBy("department").agg(avg("salary"))`?

A) Filters departments by average salary  
B) Calculates average salary for each department  
C) Groups all departments together and calculates total average  
D) Sorts departments by average salary

### Question 8
How do you sort a DataFrame by salary in descending order?

A) `df.orderBy("salary", desc=True)`  
B) `df.orderBy(col("salary").desc())`  
C) `df.sort("salary", descending=True)`  
D) `df.orderBy("salary").descending()`

### Question 9
What is the difference between `dropna()` and `fillna()`?

A) `dropna()` removes null values, `fillna()` replaces them  
B) `dropna()` replaces null values, `fillna()` removes them  
C) They are aliases for the same function  
D) `dropna()` works on columns, `fillna()` works on rows

### Question 10
Which write mode replaces existing data?

A) `mode="replace"`  
B) `mode="overwrite"`  
C) `mode="truncate"`  
D) `mode="delete"`

### Question 11
How do you create a temporary view from a DataFrame?

A) `df.createView("view_name")`  
B) `df.createTempView("view_name")`  
C) `df.createOrReplaceTempView("view_name")`  
D) `df.toView("view_name")`

### Question 12
What is the recommended file format for structured data in Databricks?

A) CSV  
B) JSON  
C) Parquet  
D) Delta Lake

### Question 13
Which of the following is an action that triggers computation?

A) `df.select("name")`  
B) `df.filter(col("age") > 25)`  
C) `df.withColumn("new_col", lit(1))`  
D) `df.count()`

### Question 14
How do you remove duplicate rows based on specific columns?

A) `df.distinct(["col1", "col2"])`  
B) `df.dropDuplicates(["col1", "col2"])`  
C) `df.removeDuplicates(["col1", "col2"])`  
D) `df.unique(["col1", "col2"])`

### Question 15
What does the following code do: `df.select(col("salary") + 1000).alias("new_salary")`?

A) Adds 1000 to salary and renames the column to "new_salary"  
B) Creates a new column called "new_salary" with value 1000  
C) Filters rows where salary is greater than 1000  
D) This code will produce an error

---

## Answers

### Answer 1
**B) `df = spark.createDataFrame(data, ["id", "name", "age"])`**

Explanation: `createDataFrame()` is the correct method to create a DataFrame from a list of tuples with column names.

### Answer 2
**C) `df.filter(col("age") > 25)`**

Explanation: `filter()` is a transformation that returns a new DataFrame without executing immediately. `show()`, `count()`, and `collect()` are actions that trigger computation.

### Answer 3
**A) `spark.read.csv("file.csv", header=True, inferSchema=True)`**

Explanation: This is the correct syntax for reading a CSV file with header and schema inference.

### Answer 4
**D) Both A and B are correct**

Explanation: You can pass column names as separate arguments or as a list to `select()`.

### Answer 5
**C) `df.filter((col("salary") > 80000) & (col("department") == "Engineering"))`**

Explanation: Use `&` for AND (not `and`), and wrap each condition in parentheses. `&&` is not valid Python syntax.

### Answer 6
**B) `df.withColumn("new_col", value)`**

Explanation: `withColumn()` is the method to add or modify columns in a DataFrame.

### Answer 7
**B) Calculates average salary for each department**

Explanation: `groupBy()` groups rows by department, and `agg(avg("salary"))` calculates the average salary for each group.

### Answer 8
**B) `df.orderBy(col("salary").desc())`**

Explanation: Use `.desc()` method on the column object to sort in descending order.

### Answer 9
**A) `dropna()` removes null values, `fillna()` replaces them**

Explanation: `dropna()` drops rows with null values, while `fillna()` fills null values with specified defaults.

### Answer 10
**B) `mode="overwrite"`**

Explanation: `mode="overwrite"` replaces existing data. Other modes are `append`, `ignore`, and `error`.

### Answer 11
**C) `df.createOrReplaceTempView("view_name")`**

Explanation: `createOrReplaceTempView()` creates a temporary view that can be queried with SQL.

### Answer 12
**D) Delta Lake**

Explanation: Delta Lake is recommended for structured data in Databricks due to ACID transactions, time travel, and better performance.

### Answer 13
**D) `df.count()`**

Explanation: `count()` is an action that triggers computation and returns a result. The others are transformations.

### Answer 14
**B) `df.dropDuplicates(["col1", "col2"])`**

Explanation: `dropDuplicates()` removes duplicate rows based on specified columns.

### Answer 15
**D) This code will produce an error**

Explanation: The `.alias()` method should be called on the column expression, not on the DataFrame. Correct syntax: `df.select((col("salary") + 1000).alias("new_salary"))`.

---

## Scoring

- 13-15 correct: Excellent! You have a strong understanding of PySpark DataFrames.
- 10-12 correct: Good job! Review the topics you missed.
- 7-9 correct: Fair. Spend more time practicing DataFrame operations.
- Below 7: Review the Day 10 materials and practice the exercises.

---

## Key Concepts to Remember

1. **Transformations vs Actions**: Transformations are lazy (filter, select), actions trigger execution (count, show)
2. **Filter Syntax**: Use `&` for AND, `|` for OR, wrap conditions in parentheses
3. **Column Functions**: Use `col()` for complex expressions
4. **Write Modes**: overwrite, append, ignore, error
5. **Null Handling**: dropna() removes, fillna() replaces
6. **Delta Lake**: Recommended format for structured data
7. **Temporary Views**: Use createOrReplaceTempView() for SQL queries
8. **Schema**: Explicit schema is better for production
9. **Method Chaining**: Chain transformations for cleaner code
10. **Lazy Evaluation**: Allows Spark to optimize execution plans
