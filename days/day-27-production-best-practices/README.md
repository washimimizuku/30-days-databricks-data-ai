# Day 27: Production Best Practices

## Overview

Today you'll learn best practices for deploying and maintaining production data pipelines. Building reliable, maintainable, and scalable production systems requires careful attention to error handling, monitoring, logging, and operational excellence.

**Time**: 1.5 hours  
**Focus**: Error handling, retry logic, monitoring, alerting, logging, deployment strategies

## Learning Objectives

By the end of today, you will be able to:
- Implement robust error handling and retry logic
- Set up comprehensive logging and monitoring
- Create alerting systems for pipeline failures
- Deploy pipelines using CI/CD best practices
- Implement idempotent and fault-tolerant pipelines
- Handle schema evolution gracefully
- Optimize pipeline performance
- Implement proper testing strategies

## Production Pipeline Principles

### 1. Reliability
Pipelines should run consistently and recover from failures automatically.

### 2. Observability
You should always know what your pipeline is doing and why it failed.

### 3. Maintainability
Code should be easy to understand, modify, and debug.

### 4. Scalability
Pipelines should handle growing data volumes without manual intervention.

### 5. Security
Data and credentials should be protected at all times.

## Error Handling

### Try-Except Blocks

```python
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import logging

spark = SparkSession.builder.getOrCreate()
logger = logging.getLogger(__name__)

def read_table_safely(table_name):
    """Read table with error handling"""
    try:
        df = spark.table(table_name)
        logger.info(f"Successfully read table: {table_name}")
        return df
    except AnalysisException as e:
        logger.error(f"Table not found: {table_name}. Error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error reading {table_name}: {str(e)}")
        raise

# Usage
try:
    customers_df = read_table_safely("customers")
except Exception as e:
    # Handle or re-raise
    print(f"Failed to read customers: {e}")
```

### Graceful Degradation

```python
def read_with_fallback(primary_table, fallback_table):
    """Try primary source, fall back to secondary"""
    try:
        return spark.table(primary_table)
    except AnalysisException:
        logger.warning(f"Primary table {primary_table} not found, using fallback")
        return spark.table(fallback_table)

# Usage
df = read_with_fallback("prod.customers", "staging.customers")
```

### Custom Exception Classes

```python
class DataQualityException(Exception):
    """Raised when data quality checks fail"""
    pass

class SchemaValidationException(Exception):
    """Raised when schema validation fails"""
    pass

def validate_data_quality(df, min_rows=1):
    """Validate data quality"""
    row_count = df.count()
    if row_count < min_rows:
        raise DataQualityException(
            f"Insufficient data: {row_count} rows (minimum: {min_rows})"
        )
    return True

# Usage
try:
    validate_data_quality(df, min_rows=1000)
except DataQualityException as e:
    logger.error(f"Data quality check failed: {e}")
    # Send alert, quarantine data, etc.
```

## Retry Logic

### Simple Retry Decorator

```python
import time
from functools import wraps

def retry(max_attempts=3, delay=5, backoff=2):
    """Retry decorator with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            current_delay = delay
            
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts >= max_attempts:
                        logger.error(f"Failed after {max_attempts} attempts: {e}")
                        raise
                    
                    logger.warning(
                        f"Attempt {attempts} failed: {e}. "
                        f"Retrying in {current_delay}s..."
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff
            
        return wrapper
    return decorator

# Usage
@retry(max_attempts=3, delay=5, backoff=2)
def read_external_data(path):
    """Read data with retry logic"""
    return spark.read.parquet(path)

# Will retry up to 3 times with exponential backoff
df = read_external_data("s3://bucket/data/")
```

### Retry Specific Exceptions

```python
def retry_on_specific_errors(func, max_attempts=3, retryable_exceptions=None):
    """Retry only on specific exceptions"""
    if retryable_exceptions is None:
        retryable_exceptions = (ConnectionError, TimeoutError)
    
    for attempt in range(max_attempts):
        try:
            return func()
        except retryable_exceptions as e:
            if attempt == max_attempts - 1:
                raise
            logger.warning(f"Retryable error on attempt {attempt + 1}: {e}")
            time.sleep(2 ** attempt)  # Exponential backoff
        except Exception as e:
            # Don't retry on non-retryable exceptions
            logger.error(f"Non-retryable error: {e}")
            raise
```

## Logging

### Structured Logging

```python
import logging
import json
from datetime import datetime

class StructuredLogger:
    """Structured logging for production pipelines"""
    
    def __init__(self, name):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # Console handler
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(handler)
    
    def log(self, level, message, **kwargs):
        """Log structured message"""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "message": message,
            **kwargs
        }
        self.logger.log(getattr(logging, level.upper()), json.dumps(log_entry))
    
    def info(self, message, **kwargs):
        self.log("info", message, **kwargs)
    
    def error(self, message, **kwargs):
        self.log("error", message, **kwargs)
    
    def warning(self, message, **kwargs):
        self.log("warning", message, **kwargs)

# Usage
logger = StructuredLogger("pipeline")

logger.info(
    "Pipeline started",
    pipeline_name="customer_etl",
    run_id="run_123",
    source_table="bronze.customers"
)

logger.error(
    "Pipeline failed",
    pipeline_name="customer_etl",
    error_type="DataQualityException",
    error_message="Insufficient rows"
)
```

### Pipeline Execution Logging

```python
class PipelineLogger:
    """Track pipeline execution metrics"""
    
    def __init__(self, pipeline_name):
        self.pipeline_name = pipeline_name
        self.start_time = None
        self.metrics = {}
    
    def start(self):
        """Start pipeline execution"""
        self.start_time = datetime.now()
        logger.info(
            "Pipeline started",
            pipeline=self.pipeline_name,
            start_time=self.start_time.isoformat()
        )
    
    def log_metric(self, metric_name, value):
        """Log a metric"""
        self.metrics[metric_name] = value
        logger.info(
            "Metric logged",
            pipeline=self.pipeline_name,
            metric=metric_name,
            value=value
        )
    
    def complete(self, status="success"):
        """Complete pipeline execution"""
        duration = (datetime.now() - self.start_time).total_seconds()
        
        logger.info(
            "Pipeline completed",
            pipeline=self.pipeline_name,
            status=status,
            duration_seconds=duration,
            metrics=self.metrics
        )

# Usage
pipeline_logger = PipelineLogger("customer_etl")
pipeline_logger.start()

# Process data
df = spark.table("bronze.customers")
pipeline_logger.log_metric("input_rows", df.count())

# Transform
result_df = df.filter(col("age") > 18)
pipeline_logger.log_metric("output_rows", result_df.count())

pipeline_logger.complete(status="success")
```

## Monitoring and Alerting

### Health Checks

```python
class PipelineHealthCheck:
    """Monitor pipeline health"""
    
    def __init__(self, pipeline_name):
        self.pipeline_name = pipeline_name
        self.checks = []
    
    def check_data_freshness(self, table_name, max_age_hours=24):
        """Check if data is fresh"""
        from pyspark.sql.functions import max as spark_max, current_timestamp, hour
        
        df = spark.table(table_name)
        latest_timestamp = df.select(spark_max("updated_at")).collect()[0][0]
        
        if latest_timestamp:
            age_hours = (datetime.now() - latest_timestamp).total_seconds() / 3600
            is_fresh = age_hours <= max_age_hours
            
            self.checks.append({
                "check": "data_freshness",
                "table": table_name,
                "passed": is_fresh,
                "age_hours": age_hours,
                "max_age_hours": max_age_hours
            })
            
            return is_fresh
        return False
    
    def check_row_count(self, table_name, min_rows=1):
        """Check minimum row count"""
        df = spark.table(table_name)
        row_count = df.count()
        passed = row_count >= min_rows
        
        self.checks.append({
            "check": "row_count",
            "table": table_name,
            "passed": passed,
            "row_count": row_count,
            "min_rows": min_rows
        })
        
        return passed
    
    def check_schema(self, table_name, expected_columns):
        """Check schema has expected columns"""
        df = spark.table(table_name)
        actual_columns = set(df.columns)
        expected_columns = set(expected_columns)
        
        missing = expected_columns - actual_columns
        passed = len(missing) == 0
        
        self.checks.append({
            "check": "schema",
            "table": table_name,
            "passed": passed,
            "missing_columns": list(missing)
        })
        
        return passed
    
    def get_health_status(self):
        """Get overall health status"""
        all_passed = all(check["passed"] for check in self.checks)
        return {
            "pipeline": self.pipeline_name,
            "healthy": all_passed,
            "checks": self.checks,
            "timestamp": datetime.now().isoformat()
        }

# Usage
health = PipelineHealthCheck("customer_pipeline")
health.check_data_freshness("gold.customers", max_age_hours=24)
health.check_row_count("gold.customers", min_rows=1000)
health.check_schema("gold.customers", ["customer_id", "name", "email"])

status = health.get_health_status()
if not status["healthy"]:
    # Send alert
    logger.error("Health check failed", **status)
```

### Alerting

```python
class AlertManager:
    """Manage pipeline alerts"""
    
    def __init__(self, pipeline_name):
        self.pipeline_name = pipeline_name
    
    def send_alert(self, severity, message, details=None):
        """Send alert (implement with your alerting system)"""
        alert = {
            "pipeline": self.pipeline_name,
            "severity": severity,
            "message": message,
            "details": details or {},
            "timestamp": datetime.now().isoformat()
        }
        
        # Log alert
        logger.error("ALERT", **alert)
        
        # In production, integrate with:
        # - PagerDuty
        # - Slack
        # - Email
        # - SNS/SQS
        # - Databricks Jobs notifications
        
        return alert
    
    def alert_on_failure(self, func):
        """Decorator to alert on function failure"""
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                self.send_alert(
                    severity="high",
                    message=f"Pipeline failed: {func.__name__}",
                    details={"error": str(e), "function": func.__name__}
                )
                raise
        return wrapper

# Usage
alert_manager = AlertManager("customer_pipeline")

@alert_manager.alert_on_failure
def process_customers():
    """Process customer data"""
    df = spark.table("bronze.customers")
    # Process...
    return df

# If this fails, an alert will be sent
result = process_customers()
```

## Idempotency

### Idempotent Writes

```python
def write_idempotent(df, table_name, key_columns):
    """Write data idempotently using MERGE"""
    from delta.tables import DeltaTable
    
    # Create table if not exists
    if not spark.catalog.tableExists(table_name):
        df.write.format("delta").saveAsTable(table_name)
        logger.info(f"Created table: {table_name}")
        return
    
    # MERGE for idempotency
    delta_table = DeltaTable.forName(spark, table_name)
    
    # Build merge condition
    merge_condition = " AND ".join([
        f"target.{col} = source.{col}" for col in key_columns
    ])
    
    (delta_table.alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    
    logger.info(f"Idempotent write to {table_name} completed")

# Usage - can run multiple times safely
write_idempotent(
    df=customers_df,
    table_name="gold.customers",
    key_columns=["customer_id"]
)
```

### Checkpoint-Based Processing

```python
class CheckpointManager:
    """Manage processing checkpoints"""
    
    def __init__(self, checkpoint_table="pipeline_checkpoints"):
        self.checkpoint_table = checkpoint_table
        self._ensure_checkpoint_table()
    
    def _ensure_checkpoint_table(self):
        """Create checkpoint table if not exists"""
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.checkpoint_table} (
                pipeline_name STRING,
                checkpoint_key STRING,
                checkpoint_value STRING,
                updated_at TIMESTAMP
            )
        """)
    
    def get_checkpoint(self, pipeline_name, key):
        """Get checkpoint value"""
        result = spark.sql(f"""
            SELECT checkpoint_value
            FROM {self.checkpoint_table}
            WHERE pipeline_name = '{pipeline_name}'
                AND checkpoint_key = '{key}'
        """).collect()
        
        return result[0][0] if result else None
    
    def set_checkpoint(self, pipeline_name, key, value):
        """Set checkpoint value"""
        from pyspark.sql.functions import current_timestamp
        
        checkpoint_df = spark.createDataFrame([
            (pipeline_name, key, str(value), datetime.now())
        ], ["pipeline_name", "checkpoint_key", "checkpoint_value", "updated_at"])
        
        # Upsert checkpoint
        spark.sql(f"""
            MERGE INTO {self.checkpoint_table} target
            USING (SELECT * FROM checkpoint_df) source
            ON target.pipeline_name = source.pipeline_name
                AND target.checkpoint_key = source.checkpoint_key
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

# Usage
checkpoint_mgr = CheckpointManager()

# Get last processed date
last_date = checkpoint_mgr.get_checkpoint("customer_etl", "last_processed_date")
if last_date is None:
    last_date = "2024-01-01"

# Process new data
new_data = spark.table("bronze.customers").filter(col("date") > last_date)
# ... process ...

# Update checkpoint
checkpoint_mgr.set_checkpoint("customer_etl", "last_processed_date", "2024-12-09")
```

## Schema Evolution

### Handling Schema Changes

```python
def merge_with_schema_evolution(source_df, target_table, key_columns):
    """MERGE with automatic schema evolution"""
    from delta.tables import DeltaTable
    
    if not spark.catalog.tableExists(target_table):
        source_df.write.format("delta").saveAsTable(target_table)
        return
    
    delta_table = DeltaTable.forName(spark, target_table)
    
    # Add missing columns to target
    target_columns = set(delta_table.toDF().columns)
    source_columns = set(source_df.columns)
    new_columns = source_columns - target_columns
    
    for col_name in new_columns:
        col_type = source_df.schema[col_name].dataType.simpleString()
        spark.sql(f"ALTER TABLE {target_table} ADD COLUMN {col_name} {col_type}")
        logger.info(f"Added column {col_name} to {target_table}")
    
    # Perform merge
    merge_condition = " AND ".join([
        f"target.{col} = source.{col}" for col in key_columns
    ])
    
    (delta_table.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
```

## Performance Optimization

### Caching Strategy

```python
def process_with_caching(df):
    """Use caching for iterative operations"""
    # Cache if reused multiple times
    df_cached = df.cache()
    
    try:
        # Multiple operations on same data
        count = df_cached.count()
        summary = df_cached.groupBy("category").count()
        filtered = df_cached.filter(col("amount") > 100)
        
        return filtered
    finally:
        # Always unpersist
        df_cached.unpersist()
```

### Partition Pruning

```python
def read_with_partition_pruning(table_name, date_filter):
    """Read only necessary partitions"""
    df = spark.table(table_name).filter(col("date") >= date_filter)
    logger.info(f"Reading {table_name} with partition filter: date >= {date_filter}")
    return df
```

## Deployment Best Practices

### Configuration Management

```python
import json

class PipelineConfig:
    """Manage pipeline configuration"""
    
    def __init__(self, env="prod"):
        self.env = env
        self.config = self._load_config()
    
    def _load_config(self):
        """Load environment-specific config"""
        config_path = f"/dbfs/configs/{self.env}_config.json"
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def get(self, key, default=None):
        """Get config value"""
        return self.config.get(key, default)

# Usage
config = PipelineConfig(env="prod")
source_table = config.get("source_table")
batch_size = config.get("batch_size", 1000)
```

### Environment Variables

```python
import os

# Use environment variables for sensitive data
def get_secret(secret_name):
    """Get secret from environment or secret manager"""
    # Try environment variable first
    secret = os.getenv(secret_name)
    
    if secret is None:
        # In production, use Databricks Secrets
        secret = dbutils.secrets.get(scope="prod", key=secret_name)
    
    return secret

# Usage
api_key = get_secret("API_KEY")
db_password = get_secret("DB_PASSWORD")
```

## Testing in Production

### Canary Deployments

```python
def canary_deployment(new_pipeline, old_pipeline, canary_percentage=10):
    """Run new pipeline on subset of data"""
    import random
    
    # Route small percentage to new pipeline
    if random.random() * 100 < canary_percentage:
        logger.info("Routing to new pipeline (canary)")
        return new_pipeline()
    else:
        logger.info("Routing to old pipeline (stable)")
        return old_pipeline()
```

### Shadow Mode

```python
def shadow_mode(primary_pipeline, shadow_pipeline):
    """Run shadow pipeline without affecting production"""
    # Run primary pipeline
    primary_result = primary_pipeline()
    
    # Run shadow pipeline in background (don't block)
    try:
        shadow_result = shadow_pipeline()
        # Compare results, log differences
        logger.info("Shadow pipeline completed successfully")
    except Exception as e:
        logger.warning(f"Shadow pipeline failed (not affecting production): {e}")
    
    return primary_result
```

## Best Practices Summary

### DO ✅
- Implement comprehensive error handling
- Use structured logging
- Set up monitoring and alerting
- Make pipelines idempotent
- Handle schema evolution
- Use configuration management
- Test thoroughly before production
- Document everything
- Use version control
- Implement retry logic with backoff

### DON'T ❌
- Hardcode credentials or secrets
- Ignore errors silently
- Skip logging
- Assume data quality
- Deploy without testing
- Use production data for testing
- Ignore performance optimization
- Skip documentation
- Make breaking changes without migration plan
- Forget to monitor

## Exam Tips

### Key Concepts

1. **Error Handling**: Always use try-except blocks
2. **Retry Logic**: Implement exponential backoff
3. **Logging**: Use structured logging for production
4. **Idempotency**: Use MERGE for idempotent writes
5. **Monitoring**: Track metrics and set up alerts
6. **Schema Evolution**: Handle schema changes gracefully

### Common Exam Questions

**Q: What is idempotency?**
A: The ability to run an operation multiple times with the same result

**Q: How do you implement retry logic?**
A: Use decorators with exponential backoff

**Q: What's the best way to handle secrets?**
A: Use Databricks Secrets, never hardcode

**Q: How do you make writes idempotent?**
A: Use MERGE instead of INSERT

## Summary

Today you learned:
- ✅ Error handling and retry strategies
- ✅ Structured logging and monitoring
- ✅ Alerting and health checks
- ✅ Idempotent pipeline design
- ✅ Schema evolution handling
- ✅ Performance optimization
- ✅ Deployment best practices
- ✅ Testing strategies

## Next Steps

Tomorrow (Day 28) you'll take:
- Practice Exam 1 (45 questions)
- Full-length exam simulation
- Comprehensive topic coverage

## Additional Resources

- [Databricks Production Best Practices](https://docs.databricks.com/best-practices/index.html)
- [Delta Lake Best Practices](https://docs.delta.io/latest/best-practices.html)
- [Monitoring and Logging](https://docs.databricks.com/administration-guide/workspace/monitoring.html)

---

**Estimated Time**: 1.5 hours  
**Difficulty**: Advanced  
**Prerequisites**: Days 1-26
