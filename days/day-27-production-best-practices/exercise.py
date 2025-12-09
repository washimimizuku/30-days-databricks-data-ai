# Day 27: Production Best Practices - Exercises
# Complete these exercises to practice production pipeline concepts

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

spark = SparkSession.builder.getOrCreate()
logger = logging.getLogger(__name__)

# ============================================================================
# PART 1: ERROR HANDLING (10 exercises)
# ============================================================================

# Exercise 1: Create a function to read a table with try-except
# TODO: Write function read_table_safely(table_name) that handles AnalysisException


# Exercise 2: Implement graceful degradation
# TODO: Write function read_with_fallback(primary_table, fallback_table)


# Exercise 3: Create custom exception class
# TODO: Create DataQualityException class


# Exercise 4: Validate data quality with custom exception
# TODO: Write function validate_row_count(df, min_rows) that raises DataQualityException


# Exercise 5: Handle multiple exception types
# TODO: Write function that catches AnalysisException and ValueError separately


# Exercise 6: Implement error logging
# TODO: Write function that logs errors before re-raising


# Exercise 7: Create error recovery function
# TODO: Write function that attempts recovery on specific errors


# Exercise 8: Implement circuit breaker pattern
# TODO: Write function that stops retrying after too many failures


# Exercise 9: Handle schema mismatch errors
# TODO: Write function that catches and handles schema errors


# Exercise 10: Create comprehensive error handler
# TODO: Write function that handles all common Spark errors


# ============================================================================
# PART 2: RETRY LOGIC (10 exercises)
# ============================================================================

# Exercise 11: Create simple retry decorator
# TODO: Write retry decorator with max_attempts parameter


# Exercise 12: Add exponential backoff to retry
# TODO: Enhance retry decorator with exponential backoff


# Exercise 13: Retry only specific exceptions
# TODO: Write retry decorator that only retries on specific exceptions


# Exercise 14: Add logging to retry decorator
# TODO: Log each retry attempt with details


# Exercise 15: Implement max retry time limit
# TODO: Add timeout parameter to retry decorator


# Exercise 16: Create retry with jitter
# TODO: Add random jitter to backoff to avoid thundering herd


# Exercise 17: Test retry decorator
# TODO: Create function that fails N times then succeeds, test with retry


# Exercise 18: Retry with callback
# TODO: Add callback function that runs on each retry


# Exercise 19: Conditional retry
# TODO: Write retry that checks if retry is worthwhile


# Exercise 20: Retry with metrics
# TODO: Track and log retry metrics (attempts, total time, etc.)


# ============================================================================
# PART 3: LOGGING (10 exercises)
# ============================================================================

# Exercise 21: Set up structured logging
# TODO: Create StructuredLogger class


# Exercise 22: Log pipeline start and end
# TODO: Write function that logs pipeline execution


# Exercise 23: Log with context
# TODO: Add context fields (pipeline_name, run_id) to logs


# Exercise 24: Create log levels
# TODO: Implement info, warning, error logging methods


# Exercise 25: Log data metrics
# TODO: Log row counts, processing time, etc.


# Exercise 26: Create audit log
# TODO: Write function that logs all data access


# Exercise 27: Log to table
# TODO: Write logs to Delta table for persistence


# Exercise 28: Implement log rotation
# TODO: Create function to archive old logs


# Exercise 29: Log performance metrics
# TODO: Track and log query execution time


# Exercise 30: Create log analysis query
# TODO: Write SQL to analyze error patterns in logs


# ============================================================================
# PART 4: MONITORING AND HEALTH CHECKS (10 exercises)
# ============================================================================

# Exercise 31: Check data freshness
# TODO: Write function to check if data is recent (< 24 hours old)


# Exercise 32: Check row count threshold
# TODO: Write function to verify minimum row count


# Exercise 33: Check schema consistency
# TODO: Write function to verify expected columns exist


# Exercise 34: Check for null values
# TODO: Write function to check critical columns have no nulls


# Exercise 35: Check data quality score
# TODO: Calculate overall quality score (0-100)


# Exercise 36: Create health check class
# TODO: Create PipelineHealthCheck class with multiple checks


# Exercise 37: Run all health checks
# TODO: Execute all checks and return status


# Exercise 38: Store health check results
# TODO: Write health check results to table


# Exercise 39: Create health check dashboard query
# TODO: Write SQL to show health status over time


# Exercise 40: Alert on health check failure
# TODO: Send alert if any health check fails


# ============================================================================
# PART 5: ALERTING (10 exercises)
# ============================================================================

# Exercise 41: Create alert manager class
# TODO: Create AlertManager class


# Exercise 42: Send alert on failure
# TODO: Write function to send alert with severity and message


# Exercise 43: Create alert decorator
# TODO: Write decorator that alerts on function failure


# Exercise 44: Implement alert throttling
# TODO: Prevent duplicate alerts within time window


# Exercise 45: Create alert escalation
# TODO: Escalate alert severity if not resolved


# Exercise 46: Log all alerts
# TODO: Write alerts to alert_log table


# Exercise 47: Create alert summary
# TODO: Generate daily alert summary


# Exercise 48: Implement alert routing
# TODO: Route alerts based on severity (email, slack, pagerduty)


# Exercise 49: Create alert resolution tracking
# TODO: Track when alerts are resolved


# Exercise 50: Build alert dashboard
# TODO: Create query to show active alerts


# ============================================================================
# PART 6: IDEMPOTENCY (10 exercises)
# ============================================================================

# Exercise 51: Implement idempotent write using MERGE
# TODO: Write function write_idempotent(df, table, key_columns)


# Exercise 52: Create checkpoint manager
# TODO: Create CheckpointManager class


# Exercise 53: Get checkpoint value
# TODO: Implement get_checkpoint(pipeline_name, key)


# Exercise 54: Set checkpoint value
# TODO: Implement set_checkpoint(pipeline_name, key, value)


# Exercise 55: Use checkpoint for incremental processing
# TODO: Process only new data since last checkpoint


# Exercise 56: Implement transaction log
# TODO: Log each write operation for audit


# Exercise 57: Create idempotent delete
# TODO: Write function that safely deletes records


# Exercise 58: Implement upsert pattern
# TODO: Update existing records, insert new ones


# Exercise 59: Handle duplicate keys
# TODO: Deduplicate before writing


# Exercise 60: Test idempotency
# TODO: Run same write operation twice, verify same result


# ============================================================================
# PART 7: SCHEMA EVOLUTION (10 exercises)
# ============================================================================

# Exercise 61: Detect schema changes
# TODO: Compare source and target schemas


# Exercise 62: Add missing columns
# TODO: ALTER TABLE to add new columns


# Exercise 63: Handle column type changes
# TODO: Cast columns to new types safely


# Exercise 64: Implement schema versioning
# TODO: Track schema versions in metadata table


# Exercise 65: Merge with schema evolution
# TODO: MERGE that handles new columns automatically


# Exercise 66: Validate schema compatibility
# TODO: Check if schemas are compatible before merge


# Exercise 67: Handle column renames
# TODO: Map old column names to new names


# Exercise 68: Drop deprecated columns
# TODO: Remove columns that are no longer needed


# Exercise 69: Create schema migration script
# TODO: Generate ALTER TABLE statements for schema changes


# Exercise 70: Test schema evolution
# TODO: Add column to source, verify target updates


# ============================================================================
# PART 8: CONFIGURATION MANAGEMENT (10 exercises)
# ============================================================================

# Exercise 71: Load configuration from file
# TODO: Create PipelineConfig class


# Exercise 72: Get configuration value
# TODO: Implement get(key, default) method


# Exercise 73: Environment-specific config
# TODO: Load different config for dev/prod


# Exercise 74: Validate configuration
# TODO: Check required config keys exist


# Exercise 75: Override config with environment variables
# TODO: Allow env vars to override file config


# Exercise 76: Create config schema
# TODO: Define expected config structure


# Exercise 77: Encrypt sensitive config
# TODO: Use Databricks Secrets for passwords


# Exercise 78: Config versioning
# TODO: Track config changes over time


# Exercise 79: Dynamic config reload
# TODO: Reload config without restarting pipeline


# Exercise 80: Config validation on startup
# TODO: Validate all config before pipeline runs


# ============================================================================
# PART 9: PERFORMANCE OPTIMIZATION (10 exercises)
# ============================================================================

# Exercise 81: Implement caching strategy
# TODO: Cache DataFrame when reused multiple times


# Exercise 82: Use partition pruning
# TODO: Filter on partition column to read less data


# Exercise 83: Optimize join strategy
# TODO: Use broadcast join for small tables


# Exercise 84: Implement batch processing
# TODO: Process data in batches instead of all at once


# Exercise 85: Use column pruning
# TODO: Select only needed columns early


# Exercise 86: Optimize file sizes
# TODO: Use OPTIMIZE to compact small files


# Exercise 87: Implement Z-ordering
# TODO: Z-order table on frequently filtered columns


# Exercise 88: Monitor query performance
# TODO: Log query execution time


# Exercise 89: Identify slow queries
# TODO: Find queries taking > threshold time


# Exercise 90: Create performance report
# TODO: Generate report of pipeline performance metrics


# ============================================================================
# PART 10: DEPLOYMENT AND TESTING (10 exercises)
# ============================================================================

# Exercise 91: Create deployment checklist
# TODO: List all pre-deployment checks


# Exercise 92: Implement smoke test
# TODO: Run basic test after deployment


# Exercise 93: Create rollback procedure
# TODO: Document steps to rollback deployment


# Exercise 94: Implement canary deployment
# TODO: Route small percentage to new version


# Exercise 95: Create shadow mode
# TODO: Run new version without affecting production


# Exercise 96: Version control integration
# TODO: Tag releases in git


# Exercise 97: Automated testing
# TODO: Run tests before deployment


# Exercise 98: Blue-green deployment
# TODO: Switch traffic between two environments


# Exercise 99: Monitor deployment health
# TODO: Check metrics after deployment


# Exercise 100: Create deployment documentation
# TODO: Document deployment process


# ============================================================================
# VERIFICATION QUERIES
# ============================================================================

# Run these to verify your work:

# 1. Check pipeline executions
# spark.sql("SELECT * FROM production_practice.pipeline_executions ORDER BY start_time DESC LIMIT 10").show()

# 2. Check checkpoints
# spark.sql("SELECT * FROM production_practice.pipeline_checkpoints ORDER BY updated_at DESC").show()

# 3. Check health checks
# spark.sql("SELECT * FROM production_practice.health_checks ORDER BY checked_at DESC LIMIT 10").show()

# 4. Check alerts
# spark.sql("SELECT * FROM production_practice.alert_log ORDER BY created_at DESC LIMIT 10").show()

# ============================================================================
# END OF EXERCISES
# ============================================================================

# Great job! You've completed 100 exercises covering:
# ✅ Error handling
# ✅ Retry logic
# ✅ Logging
# ✅ Monitoring and health checks
# ✅ Alerting
# ✅ Idempotency
# ✅ Schema evolution
# ✅ Configuration management
# ✅ Performance optimization
# ✅ Deployment and testing

# Next: Check solution.py for complete solutions and explanations
