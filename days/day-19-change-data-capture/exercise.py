# Databricks notebook source
# MAGIC %md
# MAGIC # Day 19: Change Data Capture (CDC) - Exercises
# MAGIC 
# MAGIC ## Instructions
# MAGIC - Complete each exercise by implementing CDC processing
# MAGIC - Run setup.md first to create all necessary tables
# MAGIC - Test your solutions and verify the results
# MAGIC - Check solution.py if you get stuck

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Check

# COMMAND ----------

# Verify setup is complete
spark.sql("USE day19_cdc")
print("✓ Using day19_cdc database")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Basic CDC Processing (15 exercises)

# COMMAND ----------

# Exercise 1: Apply CDC Events with MERGE
# Process all CDC events from cdc_events table into customers table
# Handle INSERT, UPDATE, and DELETE operations
# TODO: Write MERGE statement to apply CDC

# COMMAND ----------

# Exercise 2: Verify CDC Results
# Show the customers table after applying CDC
# Should have 6 customers (1 deleted, 2 added)
# TODO: Display and verify results

# COMMAND ----------

# Exercise 3: CDC with Sequence Numbers
# Apply CDC events only if sequence_number is greater than current
# Prevents applying old events
# TODO: Write conditional MERGE with sequence number check

# COMMAND ----------

# Exercise 4: Process Product CDC Events
# Apply product_cdc_events to the empty products table
# TODO: Process all product CDC events

# COMMAND ----------

# Exercise 5: CDC Event Summary
# Count how many INSERT, UPDATE, DELETE events were processed
# TODO: Summarize CDC events by operation type


# COMMAND ----------

# Exercise 6: CDC with Python DeltaTable API
# Use DeltaTable.merge() to process CDC events
# TODO: Implement CDC using Python API

# COMMAND ----------

# Exercise 7: View CDC History
# Use DESCRIBE HISTORY to see CDC operations
# TODO: Show history of customers table

# COMMAND ----------

# Exercise 8: CDC Metrics
# Capture and display metrics from CDC operation
# TODO: Show rows inserted, updated, deleted

# COMMAND ----------

# Exercise 9: Incremental CDC Processing
# Process only new CDC events (simulate incremental load)
# TODO: Process CDC events with timestamp filter

# COMMAND ----------

# Exercise 10: CDC with Validation
# Validate CDC events before processing
# Check for required fields and valid operations
# TODO: Add validation logic

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Handling CDC Challenges (10 exercises)

# COMMAND ----------

# Exercise 11: Deduplicate CDC Events
# Process cdc_events_with_duplicates table
# Remove duplicates before applying CDC
# TODO: Deduplicate by customer_id and sequence_number

# COMMAND ----------

# Exercise 12: Handle Out-of-Order Events
# Process cdc_events_out_of_order table
# Ensure events are applied in correct sequence
# TODO: Order by sequence_number before processing

# COMMAND ----------

# Exercise 13: Late-Arriving INSERT
# Handle case where UPDATE arrives before INSERT
# TODO: Process out-of-order events correctly

# COMMAND ----------

# Exercise 14: Idempotent CDC Processing
# Run CDC processing multiple times
# Verify results are the same (idempotent)
# TODO: Test idempotency

# COMMAND ----------

# Exercise 15: CDC Error Handling
# Identify and log invalid CDC events
# TODO: Filter and log malformed events

# COMMAND ----------

# Exercise 16: CDC with NULL Handling
# Handle CDC events with NULL values
# TODO: Process events with missing data

# COMMAND ----------

# Exercise 17: Batch CDC Processing
# Process CDC events in batches
# TODO: Implement batched CDC processing

# COMMAND ----------

# Exercise 18: CDC Performance Monitoring
# Measure CDC processing time
# TODO: Time CDC operations

# COMMAND ----------

# Exercise 19: CDC Rollback
# Restore table to previous state using time travel
# TODO: Rollback CDC changes

# COMMAND ----------

# Exercise 20: CDC Audit Trail
# Track which CDC events were applied
# TODO: Create audit log of CDC operations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: SCD Type 1 Implementation (10 exercises)

# COMMAND ----------

# Exercise 21: Simple SCD Type 1
# Implement SCD Type 1 (overwrite) for customers
# TODO: Apply updates without history tracking

# COMMAND ----------

# Exercise 22: SCD Type 1 with Timestamps
# Track when records were last updated
# TODO: Add updated_at timestamp

# COMMAND ----------

# Exercise 23: SCD Type 1 for Multiple Tables
# Apply SCD Type 1 to both customers and products
# TODO: Process multiple tables

# COMMAND ----------

# Exercise 24: Selective SCD Type 1
# Update only specific columns
# TODO: Partial column updates

# COMMAND ----------

# Exercise 25: SCD Type 1 with Validation
# Validate data before overwriting
# TODO: Add data quality checks

# COMMAND ----------

# Exercise 26: SCD Type 1 Metrics
# Track how many records were updated vs inserted
# TODO: Capture SCD Type 1 metrics

# COMMAND ----------

# Exercise 27: SCD Type 1 with Soft Deletes
# Mark records as inactive instead of deleting
# TODO: Implement soft delete pattern

# COMMAND ----------

# Exercise 28: SCD Type 1 Comparison
# Compare before and after states
# TODO: Show what changed

# COMMAND ----------

# Exercise 29: SCD Type 1 Rollback
# Restore previous values
# TODO: Implement rollback mechanism

# COMMAND ----------

# Exercise 30: SCD Type 1 Best Practices
# Implement complete SCD Type 1 with all best practices
# TODO: Production-ready SCD Type 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: SCD Type 2 Implementation (15 exercises)

# COMMAND ----------

# Exercise 31: Basic SCD Type 2
# Implement SCD Type 2 for one customer update
# Close old record, insert new version
# TODO: Implement basic SCD Type 2

# COMMAND ----------

# Exercise 32: SCD Type 2 - Close Old Records
# Set end_date and is_current = false for changed records
# TODO: Close out old versions

# COMMAND ----------

# Exercise 33: SCD Type 2 - Insert New Versions
# Insert new versions with is_current = true
# TODO: Add new versions

# COMMAND ----------

# Exercise 34: SCD Type 2 - Handle New Records
# Insert completely new customers
# TODO: Add new records to SCD Type 2 table

# COMMAND ----------

# Exercise 35: Complete SCD Type 2 Function
# Create reusable function for SCD Type 2
# TODO: Build SCD Type 2 function

# COMMAND ----------

# Exercise 36: Apply SCD Type 2 Updates
# Process scd2_updates into customers_scd2
# TODO: Apply all SCD Type 2 updates

# COMMAND ----------

# Exercise 37: Query Current Records
# Get only current versions (is_current = true)
# TODO: Query current state

# COMMAND ----------

# Exercise 38: Query Historical Records
# Get all versions for a specific customer
# TODO: Show full history

# COMMAND ----------

# Exercise 39: Point-in-Time Query
# Get customer state at specific date
# TODO: Query as of specific date

# COMMAND ----------

# Exercise 40: SCD Type 2 with Effective Dates
# Ensure effective_date and end_date are correct
# TODO: Validate date ranges

# COMMAND ----------

# Exercise 41: SCD Type 2 Gap Detection
# Find gaps in effective date ranges
# TODO: Detect missing date ranges

# COMMAND ----------

# Exercise 42: SCD Type 2 Overlap Detection
# Find overlapping effective date ranges
# TODO: Detect overlaps

# COMMAND ----------

# Exercise 43: SCD Type 2 Metrics
# Count total versions per customer
# TODO: Analyze version counts

# COMMAND ----------

# Exercise 44: SCD Type 2 with Multiple Changes
# Handle multiple attribute changes
# TODO: Track multiple column changes

# COMMAND ----------

# Exercise 45: SCD Type 2 Performance
# Optimize SCD Type 2 processing
# TODO: Improve performance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Streaming CDC (10 exercises)

# COMMAND ----------

# Exercise 46: Streaming CDC Setup
# Create streaming source for CDC events
# TODO: Set up streaming CDC source

# COMMAND ----------

# Exercise 47: foreachBatch CDC Processing
# Use foreachBatch to apply CDC in streaming
# TODO: Implement streaming CDC with foreachBatch

# COMMAND ----------

# Exercise 48: Streaming CDC with Watermark
# Add watermark for late data handling
# TODO: Add watermark to streaming CDC

# COMMAND ----------

# Exercise 49: Streaming CDC Deduplication
# Deduplicate events in streaming context
# TODO: Deduplicate streaming CDC events

# COMMAND ----------

# Exercise 50: Streaming CDC Monitoring
# Monitor streaming CDC query
# TODO: Check streaming metrics

# COMMAND ----------

# Exercise 51: Streaming CDC Error Handling
# Handle errors in streaming CDC
# TODO: Add error handling

# COMMAND ----------

# Exercise 52: Streaming CDC Checkpointing
# Configure checkpoint location
# TODO: Set up checkpointing

# COMMAND ----------

# Exercise 53: Streaming CDC Trigger
# Configure processing trigger
# TODO: Set trigger interval

# COMMAND ----------

# Exercise 54: Streaming CDC State Management
# Monitor state size
# TODO: Check state metrics

# COMMAND ----------

# Exercise 55: Stop and Restart Streaming CDC
# Stop query and restart from checkpoint
# TODO: Test checkpoint recovery

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Advanced CDC Patterns (10 exercises)

# COMMAND ----------

# Exercise 56: CDC Pipeline Class
# Create reusable CDC processor class
# TODO: Build CDCProcessor class

# COMMAND ----------

# Exercise 57: Multi-Table CDC
# Process CDC for multiple tables
# TODO: Handle multiple target tables

# COMMAND ----------

# Exercise 58: CDC with Transformations
# Transform data during CDC processing
# TODO: Add business logic

# COMMAND ----------

# Exercise 59: CDC with Lookups
# Enrich CDC events with dimension data
# TODO: Join with dimension tables

# COMMAND ----------

# Exercise 60: CDC with Aggregations
# Aggregate CDC events before applying
# TODO: Aggregate then apply

# COMMAND ----------

# Exercise 61: CDC with Data Quality
# Add data quality checks to CDC
# TODO: Validate data quality

# COMMAND ----------

# Exercise 62: CDC with Schema Evolution
# Handle schema changes in CDC
# TODO: Support schema evolution

# COMMAND ----------

# Exercise 63: CDC with Partitioning
# Optimize CDC with partitioning
# TODO: Use partition pruning

# COMMAND ----------

# Exercise 64: CDC with Compaction
# Compact CDC events before processing
# TODO: Optimize CDC events

# COMMAND ----------

# Exercise 65: Production CDC Pipeline
# Build complete production-ready CDC pipeline
# TODO: Implement full pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Challenges (5 exercises)

# COMMAND ----------

# Bonus 1: CDC with Change Data Feed
# Use Delta's Change Data Feed feature
# TODO: Enable and use CDF

# COMMAND ----------

# Bonus 2: Bi-Directional CDC
# Sync changes between two tables
# TODO: Implement bi-directional sync

# COMMAND ----------

# Bonus 3: CDC Conflict Resolution
# Handle conflicting updates
# TODO: Resolve conflicts

# COMMAND ----------

# Bonus 4: CDC Performance Benchmark
# Compare different CDC approaches
# TODO: Benchmark CDC methods

# COMMAND ----------

# Bonus 5: End-to-End CDC Solution
# Build complete CDC solution with:
# - Streaming ingestion
# - Deduplication
# - Validation
# - SCD Type 2
# - Monitoring
# - Error handling
# TODO: Complete end-to-end solution

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC You've completed 70 CDC exercises covering:
# MAGIC - ✅ Basic CDC processing
# MAGIC - ✅ Handling CDC challenges
# MAGIC - ✅ SCD Type 1 implementation
# MAGIC - ✅ SCD Type 2 implementation
# MAGIC - ✅ Streaming CDC
# MAGIC - ✅ Advanced CDC patterns
# MAGIC 
# MAGIC **Next**: Check solution.py for complete solutions!
