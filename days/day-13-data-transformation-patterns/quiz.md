# Day 13 Quiz: Data Transformation Patterns

## Instructions
- 15 multiple choice questions
- Each question has one correct answer
- Answers are at the bottom
- Try to answer without looking at the answers first!

---

## Questions

### Question 1
Which function removes rows where ANY column has a null value?

A) `df.dropna()`  
B) `df.dropna(how="any")`  
C) `df.filter(col("*").isNotNull())`  
D) Both A and B

### Question 2
How do you convert a string column to integer type?

A) `df.withColumn("col", col("col").toInt())`  
B) `df.withColumn("col", col("col").cast(IntegerType()))`  
C) `df.withColumn("col", int(col("col")))`  
D) `df.withColumn("col", col("col").asInt())`

### Question 3
Which function removes leading and trailing whitespace?

A) `strip()`  
B) `trim()`  
C) `clean()`  
D) `removeWhitespace()`

### Question 4
How do you parse a date string "2024-01-15" to a date type?

A) `to_date(col("date"), "yyyy-MM-dd")`  
B) `parse_date(col("date"), "yyyy-MM-dd")`  
C) `date_parse(col("date"), "yyyy-MM-dd")`  
D) `col("date").toDate("yyyy-MM-dd")`

### Question 5
Which function extracts a pattern from a string using regex?

A) `regex_extract()`  
B) `regexp_extract()`  
C) `extract_regex()`  
D) `pattern_extract()`

### Question 6
How do you remove duplicate rows based on specific columns?

A) `df.distinct(["col1", "col2"])`  
B) `df.dropDuplicates(["col1", "col2"])`  
C) `df.removeDuplicates(["col1", "col2"])`  
D) `df.unique(["col1", "col2"])`

### Question 7
What does `coalesce(col("amount"), lit(0))` do?

A) Returns 0 if amount is null, otherwise returns amount  
B) Returns amount if it's 0, otherwise returns null  
C) Combines amount and 0  
D) Converts amount to 0

### Question 8
How do you convert email addresses to lowercase?

A) `df.withColumn("email", col("email").lowercase())`  
B) `df.withColumn("email", lower(col("email")))`  
C) `df.withColumn("email", col("email").lower())`  
D) `df.withColumn("email", toLower(col("email")))`

### Question 9
Which function removes all non-numeric characters from a string?

A) `remove_non_numeric(col("phone"))`  
B) `regexp_replace(col("phone"), "[^0-9]", "")`  
C) `strip_non_numeric(col("phone"))`  
D) `clean_numeric(col("phone"))`

### Question 10
How do you calculate the difference in days between two dates?

A) `date_diff(col("end"), col("start"))`  
B) `datediff(col("end"), col("start"))`  
C) `days_between(col("end"), col("start"))`  
D) `col("end") - col("start")`

### Question 11
What does `fillna({"col1": 0, "col2": "Unknown"})` do?

A) Fills all nulls with 0 and "Unknown"  
B) Fills nulls in col1 with 0 and col2 with "Unknown"  
C) Fills nulls in all columns with 0 or "Unknown"  
D) Creates new columns col1 and col2

### Question 12
How do you validate if an email matches a pattern?

A) `col("email").matches("pattern")`  
B) `col("email").rlike("pattern")`  
C) `col("email").regex("pattern")`  
D) `col("email").pattern("pattern")`

### Question 13
Which function splits a string into an array?

A) `split(col("text"), delimiter)`  
B) `str_split(col("text"), delimiter)`  
C) `explode(col("text"), delimiter)`  
D) `array_split(col("text"), delimiter)`

### Question 14
How do you add the current timestamp to a DataFrame?

A) `df.withColumn("ts", now())`  
B) `df.withColumn("ts", current_timestamp())`  
C) `df.withColumn("ts", timestamp())`  
D) `df.withColumn("ts", get_timestamp())`

### Question 15
What's the safest way to handle type conversion errors?

A) Use try-catch in UDF  
B) Use `when().otherwise()` with cast  
C) Check if cast result is not null  
D) All of the above

---

## Answers

### Answer 1
**D) Both A and B**

Explanation: `df.dropna()` and `df.dropna(how="any")` are equivalent. Both remove rows where any column has a null value. Use `how="all"` to remove only rows where all columns are null.

### Answer 2
**B) `df.withColumn("col", col("col").cast(IntegerType()))`**

Explanation: Use `.cast(IntegerType())` or `.cast("int")` to convert types. The cast method is the standard way to convert column types in PySpark.

### Answer 3
**B) `trim()`**

Explanation: `trim()` removes leading and trailing whitespace. Use `ltrim()` for leading only and `rtrim()` for trailing only.

### Answer 4
**A) `to_date(col("date"), "yyyy-MM-dd")`**

Explanation: `to_date()` parses a string to date type with the specified format. The format pattern must match the input string format.

### Answer 5
**B) `regexp_extract()`**

Explanation: `regexp_extract(col, pattern, idx)` extracts a specific group from a regex pattern match. Use `regexp_replace()` to replace patterns.

### Answer 6
**B) `df.dropDuplicates(["col1", "col2"])`**

Explanation: `dropDuplicates()` removes duplicate rows based on specified columns. Use `distinct()` to remove all duplicate rows.

### Answer 7
**A) Returns 0 if amount is null, otherwise returns amount**

Explanation: `coalesce()` returns the first non-null value from its arguments. It's commonly used to provide default values for nulls.

### Answer 8
**B) `df.withColumn("email", lower(col("email")))`**

Explanation: Use the `lower()` function to convert strings to lowercase. Use `upper()` for uppercase and `initcap()` for title case.

### Answer 9
**B) `regexp_replace(col("phone"), "[^0-9]", "")`**

Explanation: `regexp_replace()` replaces all matches of a pattern. `[^0-9]` matches any non-digit character, and `""` replaces it with nothing.

### Answer 10
**B) `datediff(col("end"), col("start"))`**

Explanation: `datediff()` returns the number of days between two dates. The first argument is the end date, the second is the start date.

### Answer 11
**B) Fills nulls in col1 with 0 and col2 with "Unknown"**

Explanation: `fillna()` with a dictionary fills nulls in specific columns with specific values. This allows column-specific null handling.

### Answer 12
**B) `col("email").rlike("pattern")`**

Explanation: `rlike()` (or `regexp_like()`) checks if a column matches a regular expression pattern. Returns a boolean column.

### Answer 13
**A) `split(col("text"), delimiter)`**

Explanation: `split()` splits a string into an array based on a delimiter. Use `explode()` to convert the array into multiple rows.

### Answer 14
**B) `df.withColumn("ts", current_timestamp())`**

Explanation: `current_timestamp()` returns the current timestamp. Use `current_date()` for just the date without time.

### Answer 15
**D) All of the above**

Explanation: All three approaches are valid for safe type conversion. The best approach depends on your specific use case and error handling requirements.

---

## Scoring

- 13-15 correct: Excellent! You have mastered data transformation patterns.
- 10-12 correct: Good job! Review the topics you missed.
- 7-9 correct: Fair. Practice more with transformations.
- Below 7: Review the Day 13 materials and practice the exercises.

---

## Key Concepts to Remember

1. **Null Handling**: dropna(), fillna(), coalesce()
2. **Type Conversion**: cast(), safe conversion with when/otherwise
3. **String Functions**: trim(), lower(), upper(), concat(), substring()
4. **Regex**: regexp_extract(), regexp_replace(), rlike()
5. **Date Functions**: to_date(), date_format(), datediff()
6. **Duplicates**: distinct(), dropDuplicates()
7. **Validation**: rlike() for pattern matching
8. **Current Values**: current_date(), current_timestamp()
9. **String Splitting**: split(), array operations
10. **Safe Operations**: Always handle nulls and invalid values
