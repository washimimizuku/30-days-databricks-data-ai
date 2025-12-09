# Databricks notebook source
# MAGIC %md
# MAGIC # Day 20: Multi-Hop Architecture - Exercises
# MAGIC 
# MAGIC ## Instructions
# MAGIC - Complete each exercise by building Bronze → Silver → Gold pipelines
# MAGIC - Run setup.md first to create all necessary tables
# MAGIC - Test your solutions and verify data flow
# MAGIC - Check solution.py if you get stuck

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Check

# COMMAND ----------

# Verify setup is complete
print("Checking databases...")
spark.sql("SHOW DATABASES").show()

# Check tables
print("\nBronze tables:")
spark.sql("SHOW TABLES IN bronze").show()

print("\nSilver tables:")
spark.sql("SHOW TABLES IN silver").show()

print("\nGold tables:")
spark.sql("SHOW TABLES IN gold").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Bronze Layer (15 exercises)

# COMMAND ----------

# Exercise 1: Inspect Bronze Data
# View the raw data in bronze.events
# TODO: Display bronze.events table

# COMMAND ----------

# Exercise 2: Count Bronze Records
# Count total records in Bronze layer
# TODO: Count records

# COMMAND ----------

# Exercise 3: Identify Data Quality Issues
# Find records with NULL event_ids
# TODO: Filter and count NULL event_ids

# COMMAND ----------

# Exercise 4: Find Duplicates in Bronze
# Identify duplicate event_ids
# TODO: Find duplicates

# COMMAND ----------

# Exercise 5: Bronze Metadata Analysis
# Analyze ingestion_time and source_file columns
# TODO: Show unique source files and time ranges

# COMMAND ----------

# Exercise 6: Bronze Schema Inspection
# Display the schema of bronze.events
# TODO: Show schema

# COMMAND ----------

# Exercise 7: Bronze Partitioning
# Check if Bronze table is partitioned
# TODO: Show table details

# COMMAND ----------

# Exercise 8: Bronze Data Types
# Identify columns with incorrect data types
# TODO: Check timestamp column (should be string in Bronze)

# COMMAND ----------

# Exercise 9: Bronze Record Sample
# Show sample of Bronze records
# TODO: Display 10 sample records

# COMMAND ----------

# Exercise 10: Bronze Quality Metrics
# Calculate quality metrics (nulls, duplicates, etc.)
# TODO: Create quality report

# COMMAND ----------

# Exercise 11: Append to Bronze
# Add bronze.events_batch2 to bronze.events
# TODO: Append new batch

# COMMAND ----------

# Exercise 12: Bronze History
# View Delta Lake history for Bronze table
# TODO: Show DESCRIBE HISTORY

# COMMAND ----------

# Exercise 13: Bronze Time Travel
# Query Bronze table as of version 0
# TODO: Use time travel

# COMMAND ----------

# Exercise 14: Bronze File Count
# Count number of data files in Bronze table
# TODO: Show file statistics

# COMMAND ----------

# Exercise 15: Bronze Summary Statistics
# Calculate summary stats for Bronze layer
# TODO: Show min/max/avg for numeric columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Silver Layer Transformation (15 exercises)

# COMMAND ----------

# Exercise 16: Basic Bronze to Silver
# Transform Bronze to Silver with basic cleaning
# Remove NULL event_ids, convert timestamp to proper type
# TODO: Create Silver transformation

# COMMAND ----------

# Exercise 17: Deduplicate in Silver
# Remove duplicate event_ids when moving to Silver
# TODO: Implement deduplication

# COMMAND ----------

# Exercise 18: Type Conversions
# Convert timestamp from string to timestamp type
# Convert amount to double
# TODO: Apply type conversions

# COMMAND ----------

# Exercise 19: Data Validation
# Filter out invalid records (NULL required fields)
# TODO: Add validation rules

# COMMAND ----------

# Exercise 20: Standardization
# Standardize event_type to uppercase
# Trim whitespace from string fields
# TODO: Standardize data

# COMMAND ----------

# Exercise 21: Add Processing Metadata
# Add processed_time column
# TODO: Add metadata

# COMMAND ----------

# Exercise 22: Write to Silver
# Write transformed data to silver.events
# TODO: Write to Silver table

# COMMAND ----------

# Exercise 23: Verify Silver Data
# Check Silver table for quality
# TODO: Verify no duplicates, no NULLs in required fields

# COMMAND ----------

# Exercise 24: Silver Record Count
# Compare Bronze vs Silver record counts
# TODO: Show difference

# COMMAND ----------

# Exercise 25: Incremental Silver Processing
# Process only new Bronze records to Silver
# TODO: Implement incremental logic

# COMMAND ----------

# Exercise 26: Silver with Enrichment
# Join with silver.products for enrichment
# TODO: Enrich Silver data

# COMMAND ----------

# Exercise 27: Silver Data Quality Report
# Generate quality report for Silver layer
# TODO: Create quality metrics

# COMMAND ----------

# Exercise 28: Silver Schema Evolution
# Add a new column to Silver
# TODO: Alter Silver schema

# COMMAND ----------

# Exercise 29: Silver Optimization
# Optimize Silver table
# TODO: Run OPTIMIZE

# COMMAND ----------

# Exercise 30: Silver Partitioning
# Partition Silver by date
# TODO: Repartition and write

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Gold Layer Aggregation (15 exercises)

# COMMAND ----------

# Exercise 31: Daily Metrics
# Aggregate Silver to daily metrics by event_type
# TODO: Create daily aggregates

# COMMAND ----------

# Exercise 32: Write to Gold Daily
# Write daily metrics to gold.daily_metrics
# TODO: Write to Gold

# COMMAND ----------

# Exercise 33: Hourly Metrics
# Create hourly aggregates
# TODO: Aggregate by hour

# COMMAND ----------

# Exercise 34: User Summary
# Create user-level summary statistics
# TODO: Aggregate by user_id

# COMMAND ----------

# Exercise 35: Gold with MERGE
# Use MERGE to update Gold table incrementally
# TODO: Implement MERGE for Gold

# COMMAND ----------

# Exercise 36: Gold Denormalization
# Create denormalized Gold table for BI
# TODO: Join and flatten data

# COMMAND ----------

# Exercise 37: Gold Business Metrics
# Calculate business KPIs (conversion rate, ARPU, etc.)
# TODO: Calculate business metrics

# COMMAND ----------

# Exercise 38: Gold Time-Series
# Create time-series data for trending
# TODO: Build time-series aggregates

# COMMAND ----------

# Exercise 39: Gold Optimization
# Optimize Gold tables for query performance
# TODO: OPTIMIZE and Z-ORDER

# COMMAND ----------

# Exercise 40: Gold Data Validation
# Validate Gold aggregates match Silver source
# TODO: Verify aggregate accuracy

# COMMAND ----------

# Exercise 41: Gold Materialized View
# Create a materialized view pattern
# TODO: Build refreshable Gold view

# COMMAND ----------

# Exercise 42: Gold Partitioning Strategy
# Partition Gold by date for performance
# TODO: Implement partitioning

# COMMAND ----------

# Exercise 43: Gold Incremental Update
# Update Gold with only new Silver data
# TODO: Incremental Gold update

# COMMAND ----------

# Exercise 44: Gold Snapshot Table
# Create point-in-time snapshot
# TODO: Build snapshot table

# COMMAND ----------

# Exercise 45: Gold Quality Checks
# Validate Gold data quality
# TODO: Run quality checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: End-to-End Pipeline (10 exercises)

# COMMAND ----------

# Exercise 46: Complete Batch Pipeline
# Build Bronze → Silver → Gold batch pipeline
# TODO: Implement full pipeline

# COMMAND ----------

# Exercise 47: Pipeline with Error Handling
# Add error handling to pipeline
# TODO: Implement try-except

# COMMAND ----------

# Exercise 48: Pipeline Monitoring
# Add monitoring and logging
# TODO: Track metrics

# COMMAND ----------

# Exercise 49: Pipeline Orchestration
# Create function to run all layers
# TODO: Build orchestration function

# COMMAND ----------

# Exercise 50: Incremental Pipeline
# Build incremental processing pipeline
# TODO: Process only new data

# COMMAND ----------

# Exercise 51: Pipeline Testing
# Test pipeline with new data
# TODO: Add test data and verify

# COMMAND ----------

# Exercise 52: Pipeline Performance
# Measure pipeline execution time
# TODO: Time each layer

# COMMAND ----------

# Exercise 53: Pipeline Idempotency
# Ensure pipeline can run multiple times safely
# TODO: Test idempotency

# COMMAND ----------

# Exercise 54: Pipeline Rollback
# Implement rollback capability
# TODO: Use time travel for rollback

# COMMAND ----------

# Exercise 55: Pipeline Documentation
# Document the pipeline flow
# TODO: Create documentation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Advanced Patterns (10 exercises)

# COMMAND ----------

# Exercise 56: Streaming Bronze Ingestion
# Implement streaming ingestion to Bronze
# TODO: Create streaming Bronze pipeline

# COMMAND ----------

# Exercise 57: Streaming Silver Processing
# Stream from Bronze to Silver
# TODO: Streaming transformation

# COMMAND ----------

# Exercise 58: Streaming Gold Aggregation
# Stream aggregates to Gold
# TODO: Streaming aggregation

# COMMAND ----------

# Exercise 59: Mixed Batch/Streaming
# Combine batch and streaming processing
# TODO: Hybrid pipeline

# COMMAND ----------

# Exercise 60: Multi-Hop with CDC
# Integrate CDC processing in multi-hop
# TODO: Add CDC to pipeline

# COMMAND ----------

# Exercise 61: Data Lineage Tracking
# Track data lineage across layers
# TODO: Implement lineage

# COMMAND ----------

# Exercise 62: Schema Evolution Handling
# Handle schema changes across layers
# TODO: Implement schema evolution

# COMMAND ----------

# Exercise 63: Multi-Source Bronze
# Ingest from multiple sources to Bronze
# TODO: Multiple source ingestion

# COMMAND ----------

# Exercise 64: Gold Layer Variants
# Create multiple Gold tables for different use cases
# TODO: Build specialized Gold tables

# COMMAND ----------

# Exercise 65: Production Pipeline Class
# Build reusable MedallionPipeline class
# TODO: Create production-ready class

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Challenges (5 exercises)

# COMMAND ----------

# Bonus 1: Complete Medallion Architecture
# Build full production-ready medallion architecture with:
# - Streaming Bronze ingestion
# - Batch Silver processing
# - Incremental Gold updates
# - Error handling
# - Monitoring
# - Data quality checks
# TODO: Complete solution

# COMMAND ----------

# Bonus 2: Performance Optimization
# Optimize entire pipeline for performance
# TODO: Apply all optimization techniques

# COMMAND ----------

# Bonus 3: Data Quality Framework
# Build comprehensive data quality framework
# TODO: Quality checks at each layer

# COMMAND ----------

# Bonus 4: Multi-Tenant Architecture
# Extend to support multiple tenants
# TODO: Add tenant isolation

# COMMAND ----------

# Bonus 5: Real-Time Dashboard Pipeline
# Build pipeline optimized for real-time dashboards
# TODO: Low-latency Gold layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC You've completed 70 multi-hop architecture exercises covering:
# MAGIC - ✅ Bronze layer (raw data ingestion)
# MAGIC - ✅ Silver layer (cleaning and validation)
# MAGIC - ✅ Gold layer (business aggregates)
# MAGIC - ✅ End-to-end pipelines
# MAGIC - ✅ Advanced patterns
# MAGIC 
# MAGIC **Next**: Check solution.py for complete solutions!
