# Day 26 Quiz: Data Quality & Testing

Test your knowledge of data quality and testing concepts.

---

## Question 1
**Which of the following is NOT one of the six dimensions of data quality?**

A) Completeness  
B) Accuracy  
C) Scalability  
D) Consistency  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Scalability**

Explanation: The six dimensions of data quality are: completeness, accuracy, consistency, timeliness, validity, and uniqueness. Scalability is a system performance characteristic, not a data quality dimension.
</details>

---

## Question 2
**What does completeness measure in data quality?**

A) Whether data is up-to-date  
B) Whether all required values are present  
C) Whether data is accurate  
D) Whether data has no duplicates  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Whether all required values are present**

Explanation: Completeness measures whether data has all required values (no missing/null values in critical fields).
</details>

---

## Question 3
**How do you add a CHECK constraint to a Delta table?**

A) `CREATE CONSTRAINT table_name CHECK (condition)`  
B) `ALTER TABLE table_name ADD CONSTRAINT name CHECK (condition)`  
C) `INSERT CONSTRAINT INTO table_name CHECK (condition)`  
D) `UPDATE TABLE table_name SET CONSTRAINT CHECK (condition)`  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) ALTER TABLE table_name ADD CONSTRAINT name CHECK (condition)**

Explanation: The correct syntax is `ALTER TABLE table_name ADD CONSTRAINT constraint_name CHECK (condition)`.
</details>

---

## Question 4
**What happens when a Delta Lake constraint is violated during a write operation?**

A) The invalid rows are skipped  
B) The invalid rows are written to a quarantine table  
C) The entire write operation fails with an error  
D) A warning is logged but the write succeeds  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) The entire write operation fails with an error**

Explanation: When a constraint is violated, the entire write operation fails atomically. No data is written.
</details>

---

## Question 5
**Which Spark function is used to check if a column matches a regex pattern?**

A) `matches()`  
B) `regex()`  
C) `rlike()`  
D) `pattern()`  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) rlike()****

Explanation: The `rlike()` function (or `regexp_like()`) is used to check if a column value matches a regular expression pattern.
</details>

---

## Question 6
**What type of join is used to find orphaned records (records in table A with no match in table B)?**

A) Inner join  
B) Left join  
C) Right join  
D) Left anti join  

<details>
<summary>Click to reveal answer</summary>

**Answer: D) Left anti join**

Explanation: A left anti join returns rows from the left table that have no matching rows in the right table, perfect for finding orphaned records.
</details>

---

## Question 7
**How do you calculate the completeness percentage for a column?**

A) `(null_count / total_rows) * 100`  
B) `(1 - null_count / total_rows) * 100`  
C) `(total_rows - null_count) * 100`  
D) `(distinct_count / total_rows) * 100`  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) (1 - null_count / total_rows) * 100**

Explanation: Completeness is calculated as (1 - null_count / total_rows) * 100, which gives the percentage of non-null values.
</details>

---

## Question 8
**Which of the following is the best practice for handling data that fails quality checks?**

A) Delete the bad data immediately  
B) Fix the data automatically  
C) Quarantine the bad data for investigation  
D) Ignore the bad data and continue processing  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Quarantine the bad data for investigation**

Explanation: Best practice is to quarantine bad data in a separate table with a reason code, allowing investigation and potential recovery.
</details>

---

## Question 9
**What is referential integrity?**

A) Ensuring all values are unique  
B) Ensuring foreign key values exist in the referenced table  
C) Ensuring data types are consistent  
D) Ensuring no null values exist  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Ensuring foreign key values exist in the referenced table**

Explanation: Referential integrity ensures that foreign key values in one table have corresponding primary key values in the referenced table.
</details>

---

## Question 10
**Which function removes duplicate rows from a DataFrame?**

A) `distinct()`  
B) `dropDuplicates()`  
C) `removeDuplicates()`  
D) `unique()`  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) dropDuplicates()**

Explanation: The `dropDuplicates()` function removes duplicate rows. You can optionally specify columns to consider for duplication.
</details>

---

## Question 11
**What is the purpose of unit testing in data pipelines?**

A) To test the entire pipeline end-to-end  
B) To test individual functions or transformations in isolation  
C) To test database connections  
D) To test data quality  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To test individual functions or transformations in isolation**

Explanation: Unit tests focus on testing individual functions or transformations in isolation with known inputs and expected outputs.
</details>

---

## Question 12
**Which SQL command is used to view constraints on a Delta table?**

A) `SHOW CONSTRAINTS table_name`  
B) `DESCRIBE EXTENDED table_name`  
C) `LIST CONSTRAINTS table_name`  
D) `GET CONSTRAINTS table_name`  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) DESCRIBE EXTENDED table_name**

Explanation: `DESCRIBE EXTENDED table_name` shows detailed table information including constraints.
</details>

---

## Question 13
**What is the difference between accuracy and validity in data quality?**

A) There is no difference  
B) Accuracy checks if data is correct; validity checks if data conforms to rules  
C) Accuracy checks for nulls; validity checks for duplicates  
D) Accuracy is for numbers; validity is for strings  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Accuracy checks if data is correct; validity checks if data conforms to rules**

Explanation: Accuracy measures if data correctly represents reality, while validity checks if data conforms to defined business rules and formats.
</details>

---

## Question 14
**How should you handle null values in a data quality pipeline?**

A) Always remove rows with nulls  
B) Always replace nulls with zeros  
C) Evaluate based on business rules and context  
D) Ignore null values  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Evaluate based on business rules and context**

Explanation: Null handling depends on business context. Some nulls may be valid (optional fields), others may indicate data quality issues. Always evaluate based on business rules.
</details>

---

## Question 15
**What is the recommended approach for monitoring data quality over time?**

A) Run quality checks only once during development  
B) Track quality metrics over time and alert on degradation  
C) Only check quality when issues are reported  
D) Rely on end users to report quality issues  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Track quality metrics over time and alert on degradation**

Explanation: Best practice is to continuously track quality metrics, store them in a metrics table, and set up alerts when quality degrades below acceptable thresholds.
</details>

---

## Scoring Guide

- **13-15 correct**: Excellent! You have a strong understanding of data quality and testing.
- **10-12 correct**: Good job! Review the concepts you missed.
- **7-9 correct**: Fair. Review the material and practice more.
- **Below 7**: Re-read Day 26 content and complete the exercises.

---

## Key Concepts to Remember

1. **Six Data Quality Dimensions**: Completeness, accuracy, consistency, timeliness, validity, uniqueness
2. **Delta Constraints**: Use `ALTER TABLE ADD CONSTRAINT CHECK` to enforce quality rules
3. **Constraint Violations**: Cause entire write operation to fail atomically
4. **Quarantine Pattern**: Separate good and bad data for investigation
5. **Referential Integrity**: Use left anti join to find orphaned records
6. **Quality Monitoring**: Track metrics over time and alert on degradation
7. **Testing**: Unit tests for functions, integration tests for pipelines
8. **Completeness Formula**: `(1 - null_count / total_rows) * 100`
9. **Quality Checker Pattern**: Chain multiple checks for comprehensive validation
10. **Best Practice**: Define quality rules early and automate checks

</content>
</file>