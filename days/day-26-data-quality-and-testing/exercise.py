# Day 26: Data Quality & Testing - Exercises
# Complete these exercises to practice data quality and testing concepts

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

# ============================================================================
# PART 1: COMPLETENESS CHECKS (10 exercises)
# ============================================================================

# Exercise 1: Count null values in each column of customers_raw
# TODO: Write code to count nulls in each column


# Exercise 2: Calculate completeness percentage for each column
# TODO: Calculate (1 - null_count/total_rows) * 100 for each column


# Exercise 3: Find rows with any null values
# TODO: Filter customers_raw to show rows with at least one null


# Exercise 4: Find rows with all required fields populated
# TODO: Filter customers_raw where customer_id, name, and email are NOT null


# Exercise 5: Create a completeness report
# TODO: Create a DataFrame with columns: column_name, null_count, completeness_pct


# Exercise 6: Identify columns below 90% completeness
# TODO: Filter completeness report to show columns < 90% complete


# Exercise 7: Count rows with missing email
# TODO: Count how many customers have null email


# Exercise 8: Find products with missing price
# TODO: Select products where price IS NULL


# Exercise 9: Calculate overall data completeness
# TODO: Calculate average completeness across all columns


# Exercise 10: Create a function to check completeness
# TODO: Write function check_completeness(df, column) that returns percentage


# ============================================================================
# PART 2: ACCURACY CHECKS (10 exercises)
# ============================================================================

# Exercise 11: Find customers with invalid age (< 0 or > 120)
# TODO: Filter customers_raw for invalid ages


# Exercise 12: Find orders with negative amounts
# TODO: Filter orders_raw where amount < 0


# Exercise 13: Find orders with zero amounts
# TODO: Filter orders_raw where amount = 0


# Exercise 14: Count products with negative stock
# TODO: Count products where stock_quantity < 0


# Exercise 15: Find customers with invalid email format
# TODO: Filter customers where email doesn't match email pattern


# Exercise 16: Validate state codes
# TODO: Find customers with state NOT IN ('CA', 'NY', 'TX', 'FL')


# Exercise 17: Check for future dates
# TODO: Find orders where order_date > current_date()


# Exercise 18: Find products with empty names
# TODO: Filter products where product_name = '' or is null


# Exercise 19: Calculate percentage of invalid ages
# TODO: Calculate (invalid_age_count / total_count) * 100


# Exercise 20: Create accuracy score for customers table
# TODO: Calculate percentage of rows with all valid data


# ============================================================================
# PART 3: CONSISTENCY CHECKS (10 exercises)
# ============================================================================

# Exercise 21: Find orders without matching customers (referential integrity)
# TODO: Left anti join orders with customers on customer_id


# Exercise 22: Count orphaned orders
# TODO: Count orders where customer_id doesn't exist in customers


# Exercise 23: Find duplicate customer_ids
# TODO: Group by customer_id and filter where count > 1


# Exercise 24: Check email consistency (case sensitivity)
# TODO: Find emails that appear in multiple cases (e.g., "john@email.com" and "JOHN@EMAIL.COM")


# Exercise 25: Validate order status consistency
# TODO: Find orders with status not in ('pending', 'completed', 'cancelled')


# Exercise 26: Check product category consistency
# TODO: Show distinct categories and count products in each


# Exercise 27: Find customers with inconsistent data
# TODO: Find customers where name is null but email exists


# Exercise 28: Cross-table consistency check
# TODO: Verify all customer_ids in orders exist in customers


# Exercise 29: Check date format consistency
# TODO: Find dates that don't match 'YYYY-MM-DD' format


# Exercise 30: Validate price consistency
# TODO: Find products where price is null but stock_quantity > 0


# ============================================================================
# PART 4: UNIQUENESS CHECKS (10 exercises)
# ============================================================================

# Exercise 31: Find duplicate rows in customers_raw
# TODO: Use groupBy all columns and filter count > 1


# Exercise 32: Count total duplicates
# TODO: Calculate total_rows - distinct_rows


# Exercise 33: Find duplicate customer_ids
# TODO: Show customer_ids that appear more than once


# Exercise 34: Remove duplicates from customers
# TODO: Use dropDuplicates() to get unique rows


# Exercise 35: Find duplicate emails
# TODO: Group by email and show those with count > 1


# Exercise 36: Check order_id uniqueness
# TODO: Verify each order_id appears only once


# Exercise 37: Find duplicate product_ids
# TODO: Show product_ids that appear more than once


# Exercise 38: Calculate uniqueness percentage
# TODO: Calculate (distinct_count / total_count) * 100


# Exercise 39: Identify duplicate keys with different values
# TODO: Find customer_ids with same ID but different names


# Exercise 40: Create deduplication strategy
# TODO: Keep most recent record based on created_date


# ============================================================================
# PART 5: VALIDITY CHECKS (10 exercises)
# ============================================================================

# Exercise 41: Validate email format using regex
# TODO: Check email matches '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'


# Exercise 42: Validate phone format (if you add phone column)
# TODO: Check phone matches '\\d{3}-\\d{3}-\\d{4}'


# Exercise 43: Validate date format
# TODO: Try to cast order_date to date type and catch failures


# Exercise 44: Check value domains
# TODO: Verify status is in allowed values


# Exercise 45: Validate numeric ranges
# TODO: Check age between 0 and 120, amount > 0


# Exercise 46: Check string length constraints
# TODO: Verify name length between 1 and 100 characters


# Exercise 47: Validate required fields
# TODO: Check customer_id, name, email are not null


# Exercise 48: Check business rules
# TODO: Verify completed orders have amount > 0


# Exercise 49: Validate foreign keys
# TODO: Check all customer_ids in orders exist in customers


# Exercise 50: Create comprehensive validity report
# TODO: Combine all validity checks into one report


# ============================================================================
# PART 6: DATA QUALITY CHECKER CLASS (10 exercises)
# ============================================================================

# Exercise 51: Use DataQualityChecker to check customer_id not null
# TODO: checker = DataQualityChecker(df).check_not_null("customer_id")


# Exercise 52: Check customer_id uniqueness
# TODO: Add check_unique("customer_id") to checker


# Exercise 53: Check age range (0 to 120)
# TODO: Add check_range("age", 0, 120) to checker


# Exercise 54: Check email format
# TODO: Add check_format("email", email_pattern) to checker


# Exercise 55: Get all check results
# TODO: Call get_results() and display


# Exercise 56: Assert all checks passed
# TODO: Call assert_all_passed() and handle exception


# Exercise 57: Create custom check method
# TODO: Add check_values_in_list(column, valid_values) method


# Exercise 58: Chain multiple checks
# TODO: Chain 5 different checks in one statement


# Exercise 59: Create quality report DataFrame
# TODO: Convert check results to DataFrame


# Exercise 60: Save quality results to table
# TODO: Write check results to quality_metrics table


# ============================================================================
# PART 7: DELTA LAKE CONSTRAINTS (5 exercises)
# ============================================================================

# Exercise 61: Add NOT NULL constraint to customers_clean
# TODO: ALTER TABLE customers_clean ALTER COLUMN customer_id SET NOT NULL


# Exercise 62: Add CHECK constraint for valid age
# TODO: ALTER TABLE customers_clean ADD CONSTRAINT valid_age CHECK (age >= 0 AND age <= 120)


# Exercise 63: Add CHECK constraint for valid amount
# TODO: ALTER TABLE orders_clean ADD CONSTRAINT valid_amount CHECK (amount > 0)


# Exercise 64: Try to insert invalid data (should fail)
# TODO: Try to insert row with age = -5, observe constraint violation


# Exercise 65: View all constraints on a table
# TODO: DESCRIBE EXTENDED customers_clean and look for constraints


# ============================================================================
# PART 8: DATA CLEANING AND QUARANTINE (10 exercises)
# ============================================================================

# Exercise 66: Separate good and bad customer data
# TODO: Filter customers into good_data and bad_data based on quality rules


# Exercise 67: Write good data to customers_clean
# TODO: Insert valid rows into customers_clean table


# Exercise 68: Write bad data to quarantine with reason
# TODO: Add quarantine_reason column and write to quarantine_customers


# Exercise 69: Clean email addresses (lowercase, trim)
# TODO: Apply lower() and trim() to email column


# Exercise 70: Standardize state codes
# TODO: Convert state to uppercase


# Exercise 71: Fix date formats
# TODO: Convert string dates to proper date type


# Exercise 72: Handle null values
# TODO: Replace null ages with median age


# Exercise 73: Remove duplicates
# TODO: Keep only first occurrence of each customer_id


# Exercise 74: Validate and clean orders
# TODO: Filter orders with valid amount, status, and existing customer_id


# Exercise 75: Create cleaning pipeline
# TODO: Chain all cleaning operations together


# ============================================================================
# PART 9: QUALITY METRICS AND MONITORING (10 exercises)
# ============================================================================

# Exercise 76: Calculate completeness metrics for all columns
# TODO: Create metrics DataFrame with table, column, completeness


# Exercise 77: Calculate accuracy metrics
# TODO: Calculate percentage of valid values per column


# Exercise 78: Calculate uniqueness metrics
# TODO: Calculate percentage of unique values


# Exercise 79: Store metrics in quality_metrics table
# TODO: Write metrics with current_timestamp to quality_metrics


# Exercise 80: Create quality dashboard query
# TODO: Query quality_metrics for latest metrics per table


# Exercise 81: Calculate quality score (0-100)
# TODO: Average all quality dimensions into single score


# Exercise 82: Identify quality trends
# TODO: Compare today's metrics with previous day


# Exercise 83: Create quality alert function
# TODO: Alert if any metric below threshold (e.g., 95%)


# Exercise 84: Generate quality report
# TODO: Create summary report with all quality dimensions


# Exercise 85: Visualize quality over time
# TODO: Query metrics grouped by date to show trends


# ============================================================================
# PART 10: TESTING DATA TRANSFORMATIONS (5 exercises)
# ============================================================================

# Exercise 86: Write unit test for email cleaning function
# TODO: Create test data, apply function, assert expected output


# Exercise 87: Test age validation function
# TODO: Test function that validates age range


# Exercise 88: Test deduplication logic
# TODO: Create test data with duplicates, verify removal


# Exercise 89: Test referential integrity check
# TODO: Test function that finds orphaned records


# Exercise 90: Integration test for full pipeline
# TODO: Test bronze -> silver -> gold transformation


# ============================================================================
# BONUS CHALLENGES (5 exercises)
# ============================================================================

# Bonus 1: Create automated quality check pipeline
# TODO: Build function that runs all checks and generates report


# Bonus 2: Implement data quality SLA monitoring
# TODO: Track if quality meets SLA (e.g., 99% completeness)


# Bonus 3: Create quality dashboard
# TODO: Build comprehensive quality metrics dashboard


# Bonus 4: Implement data lineage tracking
# TODO: Track which quality checks were applied to each table


# Bonus 5: Build quality alerting system
# TODO: Send alerts when quality drops below threshold


# ============================================================================
# VERIFICATION QUERIES
# ============================================================================

# Run these to verify your work:

# 1. Check quality metrics collected
# spark.sql("SELECT * FROM quality_testing.quality_metrics ORDER BY measured_at DESC LIMIT 10").show()

# 2. Check quarantined data
# spark.sql("SELECT * FROM quality_testing.quarantine_customers").show()

# 3. Check clean data
# spark.sql("SELECT COUNT(*) as clean_count FROM quality_testing.customers_clean").show()

# 4. Verify constraints
# spark.sql("DESCRIBE EXTENDED quality_testing.customers_clean").show(100, False)

# ============================================================================
# END OF EXERCISES
# ============================================================================

# Great job! You've completed 95 exercises covering:
# ✅ Completeness checks
# ✅ Accuracy validation
# ✅ Consistency verification
# ✅ Uniqueness detection
# ✅ Validity rules
# ✅ Quality checker class
# ✅ Delta constraints
# ✅ Data cleaning and quarantine
# ✅ Quality metrics and monitoring
# ✅ Testing transformations
# ✅ Bonus challenges

# Next: Check solution.py for complete solutions and explanations
