# Databricks notebook source
# MAGIC %md
# MAGIC # Day 21: Optimization Techniques - Exercises
# MAGIC 
# MAGIC ## Instructions
# MAGIC - Complete each exercise by applying optimization techniques
# MAGIC - Run setup.md first to create all necessary tables
# MAGIC - Measure performance before and after
# MAGIC - Check solution.py if you get stuck

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Check

# COMMAND ----------

spark.sql("USE day21_optimization")
print("✓ Using day21_optimization database")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Table Analysis (10 exercises)

# COMMAND ----------

# Exercise 1: Analyze Table Statistics
# Use DESCRIBE DETAIL to analyze sales_unoptimized
# TODO: Show table details

# COMMAND ----------

# Exercise 2: Count Files
# Count number of files in sales_unoptimized
# TODO: Extract numFiles from DESCRIBE DETAIL

# COMMAND ----------

# Exercise 3: Calculate Average File Size
# Calculate average file size in MB
# TODO: Compute avg file size

# COMMAND ----------

# Exercise 4: Identify Small File Problem
# Determine if table has small file problem (< 128 MB avg)
# TODO: Check for small files

# COMMAND ----------

# Exercise 5: Check Partitioning
# Check if table is partitioned
# TODO: Show partition columns

# COMMAND ----------

# Exercise 6: View Table History
# Show Delta Lake history
# TODO: DESCRIBE HISTORY

# COMMAND ----------

# Exercise 7: Analyze Statistics
# Run ANALYZE TABLE to collect statistics
# TODO: Collect table statistics

# COMMAND ----------

# Exercise 8: Check Data Skipping Stats
# View min/max statistics for columns
# TODO: Show statistics

# COMMAND ----------

# Exercise 9: Compare Table Sizes
# Compare sizes of all tables
# TODO: Show size comparison

# COMMAND ----------

# Exercise 10: Baseline Query Performance
# Measure query performance before optimization
# TODO: Time a sample query

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: OPTIMIZE Command (15 exercises)

# COMMAND ----------

# Exercise 11: Basic OPTIMIZE
# Run OPTIMIZE on sales_unoptimized
# TODO: OPTIMIZE table

# COMMAND ----------

# Exercise 12: Verify OPTIMIZE Results
# Check file count after OPTIMIZE
# TODO: Compare before/after

# COMMAND ----------

# Exercise 13: OPTIMIZE Specific Partition
# OPTIMIZE only records from 2024-01
# TODO: OPTIMIZE with WHERE clause

# COMMAND ----------

# Exercise 14: OPTIMIZE with Python API
# Use DeltaTable.optimize()
# TODO: Python OPTIMIZE

# COMMAND ----------

# Exercise 15: Measure OPTIMIZE Impact
# Compare query performance before/after OPTIMIZE
# TODO: Benchmark queries

# COMMAND ----------

# Exercise 16: OPTIMIZE Multiple Tables
# OPTIMIZE all tables in database
# TODO: Loop through tables

# COMMAND ----------

# Exercise 17: OPTIMIZE with File Size Target
# Set target file size and OPTIMIZE
# TODO: Configure file size

# COMMAND ----------

# Exercise 18: Check OPTIMIZE History
# View OPTIMIZE operations in history
# TODO: Filter history for OPTIMIZE

# COMMAND ----------

# Exercise 19: OPTIMIZE Metrics
# Extract metrics from OPTIMIZE operation
# TODO: Show operation metrics

# COMMAND ----------

# Exercise 20: Schedule OPTIMIZE
# Create function to schedule OPTIMIZE
# TODO: Build scheduler function

# COMMAND ----------

# Exercise 21: OPTIMIZE Best Practices
# Implement OPTIMIZE with all best practices
# TODO: Complete OPTIMIZE strategy

# COMMAND ----------

# Exercise 22: OPTIMIZE Performance
# Measure OPTIMIZE execution time
# TODO: Time OPTIMIZE operation

# COMMAND ----------

# Exercise 23: OPTIMIZE vs No OPTIMIZE
# Compare query performance with/without OPTIMIZE
# TODO: A/B comparison

# COMMAND ----------

# Exercise 24: OPTIMIZE Frequency
# Determine optimal OPTIMIZE frequency
# TODO: Analyze write patterns

# COMMAND ----------

# Exercise 25: OPTIMIZE Monitoring
# Build monitoring for OPTIMIZE operations
# TODO: Create monitoring dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Partitioning (15 exercises)

# COMMAND ----------

# Exercise 26: Partition by Date
# Partition sales_for_partitioning by date
# TODO: Repartition table

# COMMAND ----------

# Exercise 27: Partition by Multiple Columns
# Partition by date and region
# TODO: Multi-column partitioning

# COMMAND ----------

# Exercise 28: Test Partition Pruning
# Query with partition filter and verify pruning
# TODO: Check partition pruning

# COMMAND ----------

# Exercise 29: Count Partitions
# Count number of partitions created
# TODO: Count partitions

# COMMAND ----------

# Exercise 30: Partition Size Analysis
# Analyze size of each partition
# TODO: Show partition sizes

# COMMAND ----------

# Exercise 31: Over-Partitioning Problem
# Identify if table is over-partitioned
# TODO: Check partition count

# COMMAND ----------

# Exercise 32: Repartition Strategy
# Determine optimal partitioning strategy
# TODO: Analyze query patterns

# COMMAND ----------

# Exercise 33: Dynamic Partition Overwrite
# Use dynamic partition overwrite mode
# TODO: Configure and test

# COMMAND ----------

# Exercise 34: Partition Pruning Performance
# Measure performance improvement from pruning
# TODO: Benchmark with/without pruning

# COMMAND ----------

# Exercise 35: Partition Column Selection
# Choose best columns for partitioning
# TODO: Analyze cardinality

# COMMAND ----------

# Exercise 36: Partition Maintenance
# Build partition maintenance strategy
# TODO: Create maintenance plan

# COMMAND ----------

# Exercise 37: Partition vs No Partition
# Compare partitioned vs non-partitioned performance
# TODO: Performance comparison

# COMMAND ----------

# Exercise 38: Partition Metadata
# View partition metadata
# TODO: Show partition info

# COMMAND ----------

# Exercise 39: Partition Skew
# Identify partition skew
# TODO: Analyze partition distribution

# COMMAND ----------

# Exercise 40: Partition Best Practices
# Implement all partitioning best practices
# TODO: Complete partitioning strategy

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Z-Ordering (15 exercises)

# COMMAND ----------

# Exercise 41: Basic Z-Order
# Z-order sales_for_zorder by customer_id
# TODO: OPTIMIZE ZORDER BY

# COMMAND ----------

# Exercise 42: Multi-Column Z-Order
# Z-order by customer_id and product_id
# TODO: Z-order multiple columns

# COMMAND ----------

# Exercise 43: Test Data Skipping
# Query and verify data skipping with Z-order
# TODO: Check files scanned

# COMMAND ----------

# Exercise 44: Z-Order Performance
# Measure query performance improvement
# TODO: Benchmark before/after Z-order

# COMMAND ----------

# Exercise 45: Z-Order Column Selection
# Choose best columns for Z-ordering
# TODO: Analyze query patterns

# COMMAND ----------

# Exercise 46: Z-Order Limitations
# Test Z-order with > 4 columns
# TODO: Understand limitations

# COMMAND ----------

# Exercise 47: Z-Order vs Partition
# Compare Z-order vs partitioning
# TODO: Performance comparison

# COMMAND ----------

# Exercise 48: Z-Order Maintenance
# Determine Z-order refresh frequency
# TODO: Build maintenance schedule

# COMMAND ----------

# Exercise 49: Z-Order with Partition
# Combine partitioning and Z-ordering
# TODO: Hybrid strategy

# COMMAND ----------

# Exercise 50: Z-Order Metrics
# Extract Z-order operation metrics
# TODO: Show metrics

# COMMAND ----------

# Exercise 51: Z-Order Best Practices
# Implement all Z-ordering best practices
# TODO: Complete Z-order strategy

# COMMAND ----------

# Exercise 52: Z-Order Impact Analysis
# Analyze impact on different query types
# TODO: Query pattern analysis

# COMMAND ----------

# Exercise 53: Z-Order for Joins
# Optimize join performance with Z-order
# TODO: Z-order join columns

# COMMAND ----------

# Exercise 54: Z-Order Monitoring
# Monitor Z-order effectiveness
# TODO: Build monitoring

# COMMAND ----------

# Exercise 55: Z-Order Production Strategy
# Build production Z-order strategy
# TODO: Complete production plan

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: VACUUM (10 exercises)

# COMMAND ----------

# Exercise 56: VACUUM Dry Run
# Run VACUUM dry run to see what would be deleted
# TODO: VACUUM DRY RUN

# COMMAND ----------

# Exercise 57: VACUUM with Default Retention
# VACUUM with 7-day retention
# TODO: VACUUM table

# COMMAND ----------

# Exercise 58: VACUUM with Custom Retention
# VACUUM with 30-day retention
# TODO: VACUUM RETAIN 720 HOURS

# COMMAND ----------

# Exercise 59: VACUUM Specific Partition
# VACUUM only old partitions
# TODO: VACUUM with WHERE

# COMMAND ----------

# Exercise 60: Check VACUUM Impact
# Measure storage savings from VACUUM
# TODO: Compare sizes before/after

# COMMAND ----------

# Exercise 61: VACUUM Safety Checks
# Implement safety checks before VACUUM
# TODO: Build safety validation

# COMMAND ----------

# Exercise 62: VACUUM All Tables
# VACUUM all tables in database
# TODO: Loop through tables

# COMMAND ----------

# Exercise 63: VACUUM Monitoring
# Monitor VACUUM operations
# TODO: Track VACUUM metrics

# COMMAND ----------

# Exercise 64: VACUUM Best Practices
# Implement all VACUUM best practices
# TODO: Complete VACUUM strategy

# COMMAND ----------

# Exercise 65: VACUUM Schedule
# Create VACUUM schedule
# TODO: Build scheduler

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Challenges (5 exercises)

# COMMAND ----------

# Bonus 1: Complete Optimization Pipeline
# Build end-to-end optimization pipeline with:
# - Analysis
# - OPTIMIZE
# - Partitioning
# - Z-ordering
# - VACUUM
# - Monitoring
# TODO: Complete pipeline

# COMMAND ----------

# Bonus 2: Optimization ROI Calculator
# Calculate ROI of optimization efforts
# TODO: Build ROI calculator

# COMMAND ----------

# Bonus 3: Automated Optimization
# Build automated optimization system
# TODO: Auto-optimization framework

# COMMAND ----------

# Bonus 4: Optimization Dashboard
# Create optimization monitoring dashboard
# TODO: Build dashboard

# COMMAND ----------

# Bonus 5: Production Optimization Strategy
# Complete production-ready optimization strategy
# TODO: Full production plan

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC You've completed 70 optimization exercises covering:
# MAGIC - ✅ Table analysis
# MAGIC - ✅ OPTIMIZE command
# MAGIC - ✅ Partitioning strategies
# MAGIC - ✅ Z-ordering techniques
# MAGIC - ✅ VACUUM operations
# MAGIC 
# MAGIC **Next**: Check solution.py for complete solutions!
