# Databricks notebook source
# MAGIC %md
# MAGIC # Day 18: Delta Lake MERGE (UPSERT) - Exercises
# MAGIC 
# MAGIC ## Instructions
# MAGIC - Complete each exercise by writing the required MERGE operation
# MAGIC - Run setup.md first to create all necessary tables
# MAGIC - Test your solutions and verify the results
# MAGIC - Check solution.py if you get stuck

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Check

# COMMAND ----------

# Verify setup is complete
spark.sql("USE day18_merge")
print("✓ Using day18_merge database")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Basic MERGE Operations (15 exercises)

# COMMAND ----------

# Exercise 1: Simple UPSERT
# Merge customer_updates into customers
# Update existing customers, insert new ones
# TODO: Write MERGE statement

# COMMAND ----------

# Exercise 2: Verify Exercise 1
# Show the customers table after merge
# Should have 7 customers total (5 original + 2 new)
# TODO: Display results

# COMMAND ----------

# Exercise 3: MERGE with DELETE
# Merge inventory_updates into inventory
# Delete products with quantity = 0
# Update existing products, insert new ones
# TODO: Write MERGE statement

# COMMAND ----------

# Exercise 4: Python MERGE - Basic UPSERT
# Use DeltaTable API to merge customer_updates into customers
# TODO: Write Python MERGE code

# COMMAND ----------

# Exercise 5: MERGE with Timestamp Tracking
# Merge customer_updates into customers
# Add updated_at timestamp for matched records
# Add created_at timestamp for new records
# TODO: Write MERGE with timestamps


# COMMAND ----------

# Exercise 6: MERGE Sales Updates
# Merge sales_updates into sales table
# Update status for existing sales
# Insert new sales
# TODO: Write MERGE statement

# COMMAND ----------

# Exercise 7: View MERGE History
# Use DESCRIBE HISTORY to see the MERGE operations
# TODO: Show history for customers table

# COMMAND ----------

# Exercise 8: MERGE with Multiple Conditions
# Merge sales_updates into sales
# Handle different statuses differently:
# - CANCELLED: Update status and set cancelled_at
# - REFUNDED: Update status and set refunded_at
# - COMPLETED: Update amount and status
# TODO: Write MERGE with multiple WHEN MATCHED clauses

# COMMAND ----------

# Exercise 9: MERGE Metrics
# Perform a MERGE and capture the metrics
# Display number of rows updated and inserted
# TODO: Capture and display MERGE metrics

# COMMAND ----------

# Exercise 10: Conditional INSERT
# Merge customer_updates into customers
# Only insert new customers if email is not null
# TODO: Write MERGE with conditional INSERT

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Deduplication with MERGE (10 exercises)

# COMMAND ----------

# Exercise 11: Deduplicate Source Before MERGE
# Deduplicate user_activity_raw by event_id (keep latest by event_time)
# Then merge into user_activity table
# TODO: Deduplicate and MERGE

# COMMAND ----------

# Exercise 12: Verify Deduplication
# Count records in user_activity
# Should have no duplicates
# TODO: Verify no duplicate event_ids

# COMMAND ----------

# Exercise 13: Window Function Deduplication
# Use window function to deduplicate user_activity_raw
# Keep the record with the latest event_time for each event_id
# TODO: Use row_number() to deduplicate

# COMMAND ----------

# Exercise 14: MERGE with Deduplication in One Query
# Create a MERGE that deduplicates source in the USING clause
# TODO: Deduplicate inline in MERGE

# COMMAND ----------

# Exercise 15: Incremental Deduplication
# Add new activity records and deduplicate incrementally
# TODO: Handle incremental deduplication

# COMMAND ----------

# Exercise 16: Deduplicate by Multiple Columns
# Deduplicate by user_id and event_type (keep latest)
# TODO: Deduplicate by composite key

# COMMAND ----------

# Exercise 17: Deduplication with Aggregation
# For duplicate events, sum values before merging
# TODO: Aggregate duplicates before MERGE

# COMMAND ----------

# Exercise 18: Soft Delete Duplicates
# Instead of removing duplicates, mark them as inactive
# TODO: Mark duplicates with is_duplicate flag

# COMMAND ----------

# Exercise 19: Deduplication Performance
# Compare performance of different deduplication methods
# TODO: Time different approaches

# COMMAND ----------

# Exercise 20: Verify Final Deduplicated State
# Ensure user_activity has no duplicates
# TODO: Run quality checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Conditional MERGE (15 exercises)

# COMMAND ----------

# Exercise 21: Version-Based MERGE
# Merge order_updates into orders
# Only update if source version > target version
# TODO: Write conditional MERGE based on version

# COMMAND ----------

# Exercise 22: Timestamp-Based MERGE
# Only update if source updated_at > target updated_at
# TODO: Write time-based conditional MERGE

# COMMAND ----------

# Exercise 23: Multiple Conditional Updates
# Handle different version scenarios:
# - source.version > target.version: Update all fields
# - source.version = target.version AND amounts differ: Flag conflict
# - source.version < target.version: Do nothing
# TODO: Write MERGE with multiple conditions

# COMMAND ----------

# Exercise 24: Conditional DELETE
# Delete orders where version = 0 (marked for deletion)
# TODO: Write MERGE with conditional DELETE

# COMMAND ----------

# Exercise 25: WHEN NOT MATCHED BY SOURCE
# Mark customers as inactive if they don't exist in updates
# TODO: Use WHEN NOT MATCHED BY SOURCE

# COMMAND ----------

# Exercise 26: Complex Business Logic
# Merge with business rules:
# - Update if amount increased
# - Flag if amount decreased
# - Ignore if no change
# TODO: Implement business logic in MERGE

# COMMAND ----------

# Exercise 27: Conditional INSERT with Validation
# Only insert orders if amount > 0 and version >= 1
# TODO: Write MERGE with validation

# COMMAND ----------

# Exercise 28: Status-Based Conditional MERGE
# Different actions based on status field
# TODO: Handle multiple status values

# COMMAND ----------

# Exercise 29: Null Handling in Conditions
# Handle NULL values in merge conditions properly
# TODO: Write NULL-safe MERGE

# COMMAND ----------

# Exercise 30: Priority-Based MERGE
# First matching condition wins - test order
# TODO: Demonstrate condition order importance

# COMMAND ----------

# Exercise 31: Conflict Resolution
# When versions match but data differs, use conflict resolution
# TODO: Implement conflict resolution logic

# COMMAND ----------

# Exercise 32: Audit Trail with MERGE
# Track who made changes and when
# TODO: Add audit columns in MERGE

# COMMAND ----------

# Exercise 33: Conditional MERGE with Expressions
# Use complex expressions in conditions
# TODO: Write MERGE with calculated conditions

# COMMAND ----------

# Exercise 34: Multi-Field Conditional Logic
# Conditions based on multiple field comparisons
# TODO: Complex multi-field conditions

# COMMAND ----------

# Exercise 35: Verify Conditional Logic
# Test that conditions worked as expected
# TODO: Verify conditional MERGE results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Performance Optimization (10 exercises)

# COMMAND ----------

# Exercise 36: Partition Pruning in MERGE
# Merge only specific date partitions in sales table
# TODO: Add partition filter to MERGE

# COMMAND ----------

# Exercise 37: Measure MERGE Performance
# Time a MERGE operation
# TODO: Measure execution time

# COMMAND ----------

# Exercise 38: Optimize Before MERGE
# Run OPTIMIZE on target table before large MERGE
# TODO: OPTIMIZE then MERGE

# COMMAND ----------

# Exercise 39: Repartition Source
# Repartition source to match target partitioning
# TODO: Repartition before MERGE

# COMMAND ----------

# Exercise 40: Broadcast Small Source
# Use broadcast hint for small source table
# TODO: Broadcast join in MERGE

# COMMAND ----------

# Exercise 41: Filter Source Before MERGE
# Pre-filter source to reduce data volume
# TODO: Filter then MERGE

# COMMAND ----------

# Exercise 42: Batch MERGE Operations
# Process large updates in batches
# TODO: Implement batched MERGE

# COMMAND ----------

# Exercise 43: Monitor MERGE Metrics
# Track rows updated, inserted, deleted
# TODO: Capture detailed metrics

# COMMAND ----------

# Exercise 44: OPTIMIZE After MERGE
# Compact files after large MERGE
# TODO: MERGE then OPTIMIZE

# COMMAND ----------

# Exercise 45: Z-ORDER After MERGE
# Apply Z-ordering on frequently queried columns
# TODO: MERGE, OPTIMIZE, ZORDER

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Advanced Patterns (15 exercises)

# COMMAND ----------

# Exercise 46: Streaming MERGE with foreachBatch
# Create a streaming MERGE using foreachBatch
# TODO: Implement streaming MERGE

# COMMAND ----------

# Exercise 47: Multi-Table MERGE
# Merge same source into multiple target tables
# TODO: MERGE into multiple tables

# COMMAND ----------

# Exercise 48: Schema Evolution in MERGE
# Enable schema auto-merge and add new columns
# TODO: MERGE with schema evolution

# COMMAND ----------

# Exercise 49: SCD Type 1 Implementation
# Implement Slowly Changing Dimension Type 1
# TODO: Full SCD Type 1 MERGE

# COMMAND ----------

# Exercise 50: Soft Delete Pattern
# Mark records as deleted instead of removing them
# TODO: Implement soft delete with MERGE

# COMMAND ----------

# Exercise 51: Incremental Daily Load
# Load yesterday's data incrementally
# TODO: Daily incremental MERGE

# COMMAND ----------

# Exercise 52: MERGE with Aggregation
# Aggregate source data before merging
# TODO: Aggregate then MERGE

# COMMAND ----------

# Exercise 53: Lookup Enrichment
# Enrich source with lookup data before MERGE
# TODO: Join with dimension then MERGE

# COMMAND ----------

# Exercise 54: Error Handling in MERGE
# Handle errors gracefully in MERGE operation
# TODO: Add try-except around MERGE

# COMMAND ----------

# Exercise 55: Idempotent MERGE
# Ensure MERGE can be run multiple times safely
# TODO: Test idempotency

# COMMAND ----------

# Exercise 56: MERGE with Complex Joins
# Use multiple join conditions in MERGE
# TODO: Multi-column join in MERGE

# COMMAND ----------

# Exercise 57: Partial Column Updates
# Update only specific columns, not all
# TODO: Selective column MERGE

# COMMAND ----------

# Exercise 58: MERGE with Calculated Fields
# Calculate new values during MERGE
# TODO: Use expressions in SET clause

# COMMAND ----------

# Exercise 59: Historical Tracking
# Keep history of changes using MERGE
# TODO: Track change history

# COMMAND ----------

# Exercise 60: Complete Pipeline with MERGE
# Build end-to-end pipeline using MERGE
# TODO: Full pipeline implementation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Challenges (5 exercises)

# COMMAND ----------

# Bonus 1: Build a Reusable MERGE Function
# Create a generic MERGE function for any table
# TODO: Write reusable MERGE function

# COMMAND ----------

# Bonus 2: MERGE Performance Comparison
# Compare MERGE vs separate INSERT/UPDATE
# TODO: Benchmark different approaches

# COMMAND ----------

# Bonus 3: Complex CDC Processing
# Process CDC events with MERGE (INSERT, UPDATE, DELETE)
# TODO: Full CDC MERGE implementation

# COMMAND ----------

# Bonus 4: Data Quality with MERGE
# Add data quality checks before MERGE
# TODO: Validate before MERGE

# COMMAND ----------

# Bonus 5: Production-Ready MERGE Pipeline
# Build production-ready MERGE with:
# - Error handling
# - Logging
# - Metrics
# - Monitoring
# - Retry logic
# TODO: Complete production pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC You've completed 65 MERGE exercises covering:
# MAGIC - ✅ Basic MERGE operations
# MAGIC - ✅ Deduplication patterns
# MAGIC - ✅ Conditional logic
# MAGIC - ✅ Performance optimization
# MAGIC - ✅ Advanced patterns
# MAGIC - ✅ Production best practices
# MAGIC 
# MAGIC **Next**: Check solution.py for complete solutions!
