# Day 27: Production Best Practices - Solutions
# Complete solutions with explanations

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
import logging
import time
import json
from functools import wraps
from datetime import datetime, timedelta
import random

spark = SparkSession.builder.getOrCreate()
logger = logging.getLogger(__name__)

# ============================================================================
# PART 1: ERROR HANDLING (10 exercises)
# ============================================================================

# Exercise 1: Create a function to read a table with try-except
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

# Exercise 2: Implement graceful degradation
def read_with_fallback(primary_table, fallback_table):
    """Try primary source, fall back to secondary"""
    try:
        return spark.table(primary_table)
    except AnalysisException:
        logger.warning(f"Primary table {primary_table} not found, using fallback")
        return spark.table(fallback_table)

# Exercise 3: Create custom exception class
class DataQualityException(Exception):
    """Raised when data quality checks fail"""
    pass

class SchemaValidationException(Exception):
    """Raised when schema validation fails"""
    pass

# Exercise 4: Validate data quality with custom exception
def validate_row_count(df, min_rows=1):
    """Validate minimum row count"""
    row_count = df.count()
    if row_count < min_rows:
        raise DataQualityException(
            f"Insufficient data: {row_count} rows (minimum: {min_rows})"
        )
    return True

# Exercise 5-10: Additional error handling patterns
def handle_multiple_exceptions(table_name):
    """Handle different exception types"""
    try:
        df = spark.table(table_name)
        return df
    except AnalysisException as e:
        logger.error(f"Table error: {e}")
        raise
    except ValueError as e:
        logger.error(f"Value error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

# ============================================================================
# PART 2: RETRY LOGIC (10 exercises)
# ============================================================================

# Exercise 11-12: Retry decorator with exponential backoff
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

# Exercise 13: Retry only specific exceptions
def retry_on_specific_errors(max_attempts=3, retryable_exceptions=None):
    """Retry only on specific exceptions"""
    if retryable_exceptions is None:
        retryable_exceptions = (ConnectionError, TimeoutError)
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    if attempt == max_attempts - 1:
                        raise
                    logger.warning(f"Retryable error on attempt {attempt + 1}: {e}")
                    time.sleep(2 ** attempt)
                except Exception as e:
                    logger.error(f"Non-retryable error: {e}")
                    raise
        return wrapper
    return decorator

# Usage example
@retry(max_attempts=3, delay=5, backoff=2)
def read_external_data(path):
    """Read data with retry logic"""
    return spark.read.parquet(path)

# ============================================================================
# PART 3: LOGGING (10 exercises)
# ============================================================================

# Exercise 21: Structured logging
class StructuredLogger:
    """Structured logging for production pipelines"""
    
    def __init__(self, name):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
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

# Exercise 22: Pipeline execution logging
class PipelineLogger:
    """Track pipeline execution metrics"""
    
    def __init__(self, pipeline_name):
        self.pipeline_name = pipeline_name
        self.start_time = None
        self.metrics = {}
    
    def start(self):
        """Start pipeline execution"""
        self.start_time = datetime.now()
        logger.info(f"Pipeline {self.pipeline_name} started at {self.start_time}")
    
    def log_metric(self, metric_name, value):
        """Log a metric"""
        self.metrics[metric_name] = value
        logger.info(f"Metric {metric_name}: {value}")
    
    def complete(self, status="success"):
        """Complete pipeline execution"""
        duration = (datetime.now() - self.start_time).total_seconds()
        logger.info(
            f"Pipeline {self.pipeline_name} completed: "
            f"status={status}, duration={duration}s, metrics={self.metrics}"
        )

# ============================================================================
# PART 4: MONITORING AND HEALTH CHECKS (10 exercises)
# ============================================================================

# Exercise 31-37: Health check class
class PipelineHealthCheck:
    """Monitor pipeline health"""
    
    def __init__(self, pipeline_name):
        self.pipeline_name = pipeline_name
        self.checks = []
    
    def check_data_freshness(self, table_name, max_age_hours=24):
        """Check if data is fresh"""
        df = spark.table(table_name)
        latest_timestamp = df.select(max("updated_at")).collect()[0][0]
        
        if latest_timestamp:
            latest_dt = datetime.strptime(latest_timestamp, "%Y-%m-%d %H:%M:%S")
            age_hours = (datetime.now() - latest_dt).total_seconds() / 3600
            is_fresh = age_hours <= max_age_hours
            
            self.checks.append({
                "check": "data_freshness",
                "table": table_name,
                "passed": is_fresh,
                "age_hours": age_hours
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
            "row_count": row_count
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
health.check_data_freshness("production_practice.customers", max_age_hours=24)
health.check_row_count("production_practice.customers", min_rows=1)
health.check_schema("production_practice.customers", ["customer_id", "name", "email"])

status = health.get_health_status()
print(f"Pipeline healthy: {status['healthy']}")

# ============================================================================
# PART 5: ALERTING (10 exercises)
# ============================================================================

# Exercise 41-44: Alert manager
class AlertManager:
    """Manage pipeline alerts"""
    
    def __init__(self, pipeline_name):
        self.pipeline_name = pipeline_name
        self.recent_alerts = {}
    
    def send_alert(self, severity, message, details=None):
        """Send alert"""
        alert = {
            "pipeline": self.pipeline_name,
            "severity": severity,
            "message": message,
            "details": details or {},
            "timestamp": datetime.now().isoformat()
        }
        
        logger.error(f"ALERT: {json.dumps(alert)}")
        
        # Store in recent alerts for throttling
        alert_key = f"{severity}:{message}"
        self.recent_alerts[alert_key] = datetime.now()
        
        return alert
    
    def should_send_alert(self, severity, message, throttle_minutes=60):
        """Check if alert should be sent (throttling)"""
        alert_key = f"{severity}:{message}"
        last_sent = self.recent_alerts.get(alert_key)
        
        if last_sent:
            minutes_since = (datetime.now() - last_sent).total_seconds() / 60
            return minutes_since >= throttle_minutes
        
        return True
    
    def alert_on_failure(self, func):
        """Decorator to alert on function failure"""
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                self.send_alert(
                    severity="high",
                    message=f"Pipeline failed: {func.__name__}",
                    details={"error": str(e)}
                )
                raise
        return wrapper

# Usage
alert_manager = AlertManager("customer_pipeline")

@alert_manager.alert_on_failure
def process_customers():
    """Process customer data"""
    df = spark.table("production_practice.customers")
    return df

# ============================================================================
# PART 6: IDEMPOTENCY (10 exercises)
# ============================================================================

# Exercise 51: Idempotent write using MERGE
def write_idempotent(df, table_name, key_columns):
    """Write data idempotently using MERGE"""
    from delta.tables import DeltaTable
    
    if not spark.catalog.tableExists(table_name):
        df.write.format("delta").saveAsTable(table_name)
        logger.info(f"Created table: {table_name}")
        return
    
    delta_table = DeltaTable.forName(spark, table_name)
    
    merge_condition = " AND ".join([
        f"target.{col} = source.{col}" for col in key_columns
    ])
    
    (delta_table.alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    
    logger.info(f"Idempotent write to {table_name} completed")

# Exercise 52-54: Checkpoint manager
class CheckpointManager:
    """Manage processing checkpoints"""
    
    def __init__(self, checkpoint_table="production_practice.pipeline_checkpoints"):
        self.checkpoint_table = checkpoint_table
    
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
        checkpoint_data = [(pipeline_name, key, str(value), datetime.now())]
        checkpoint_df = spark.createDataFrame(
            checkpoint_data,
            ["pipeline_name", "checkpoint_key", "checkpoint_value", "updated_at"]
        )
        
        checkpoint_df.createOrReplaceTempView("checkpoint_temp")
        
        spark.sql(f"""
            MERGE INTO {self.checkpoint_table} target
            USING checkpoint_temp source
            ON target.pipeline_name = source.pipeline_name
                AND target.checkpoint_key = source.checkpoint_key
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

# Usage
checkpoint_mgr = CheckpointManager()
last_date = checkpoint_mgr.get_checkpoint("customer_etl", "last_processed_date")
# Process data...
checkpoint_mgr.set_checkpoint("customer_etl", "last_processed_date", "2024-12-09")

# ============================================================================
# PART 7: SCHEMA EVOLUTION (10 exercises)
# ============================================================================

# Exercise 61-65: Schema evolution
def merge_with_schema_evolution(source_df, target_table, key_columns):
    """MERGE with automatic schema evolution"""
    from delta.tables import DeltaTable
    
    if not spark.catalog.tableExists(target_table):
        source_df.write.format("delta").saveAsTable(target_table)
        return
    
    delta_table = DeltaTable.forName(spark, target_table)
    
    # Add missing columns
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

# ============================================================================
# PART 8: CONFIGURATION MANAGEMENT (10 exercises)
# ============================================================================

# Exercise 71-73: Configuration management
class PipelineConfig:
    """Manage pipeline configuration"""
    
    def __init__(self, env="dev"):
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

# ============================================================================
# PART 9: PERFORMANCE OPTIMIZATION (10 exercises)
# ============================================================================

# Exercise 81-82: Performance optimization
def process_with_optimization(table_name, date_filter):
    """Process data with optimizations"""
    # Partition pruning
    df = spark.table(table_name).filter(col("order_date") >= date_filter)
    
    # Column pruning
    df = df.select("order_id", "customer_id", "amount")
    
    # Cache if reused
    df_cached = df.cache()
    
    try:
        # Multiple operations
        count = df_cached.count()
        summary = df_cached.groupBy("customer_id").agg(sum("amount"))
        return summary
    finally:
        df_cached.unpersist()

# ============================================================================
# PART 10: DEPLOYMENT AND TESTING (10 exercises)
# ============================================================================

# Exercise 91-94: Deployment patterns
def canary_deployment(new_pipeline, old_pipeline, canary_percentage=10):
    """Run new pipeline on subset of data"""
    if random.random() * 100 < canary_percentage:
        logger.info("Routing to new pipeline (canary)")
        return new_pipeline()
    else:
        logger.info("Routing to old pipeline (stable)")
        return old_pipeline()

def shadow_mode(primary_pipeline, shadow_pipeline):
    """Run shadow pipeline without affecting production"""
    primary_result = primary_pipeline()
    
    try:
        shadow_result = shadow_pipeline()
        logger.info("Shadow pipeline completed successfully")
    except Exception as e:
        logger.warning(f"Shadow pipeline failed: {e}")
    
    return primary_result

# ============================================================================
# COMPLETE PRODUCTION PIPELINE EXAMPLE
# ============================================================================

class ProductionPipeline:
    """Complete production pipeline with all best practices"""
    
    def __init__(self, pipeline_name, env="prod"):
        self.pipeline_name = pipeline_name
        self.config = PipelineConfig(env)
        self.logger = StructuredLogger(pipeline_name)
        self.health = PipelineHealthCheck(pipeline_name)
        self.alert_manager = AlertManager(pipeline_name)
        self.checkpoint_mgr = CheckpointManager()
    
    @retry(max_attempts=3, delay=5, backoff=2)
    def read_source_data(self):
        """Read source data with retry"""
        table_name = self.config.get("source_table")
        return read_table_safely(table_name)
    
    def validate_data(self, df):
        """Validate data quality"""
        try:
            validate_row_count(df, min_rows=1)
            self.logger.info("Data validation passed")
            return True
        except DataQualityException as e:
            self.alert_manager.send_alert("high", "Data quality check failed", {"error": str(e)})
            raise
    
    def process_data(self, df):
        """Process data"""
        # Add processing logic here
        return df.filter(col("customer_id").isNotNull())
    
    def write_data(self, df):
        """Write data idempotently"""
        target_table = self.config.get("target_table")
        write_idempotent(df, target_table, ["customer_id"])
    
    def run(self):
        """Run complete pipeline"""
        pipeline_logger = PipelineLogger(self.pipeline_name)
        pipeline_logger.start()
        
        try:
            # Read
            df = self.read_source_data()
            pipeline_logger.log_metric("input_rows", df.count())
            
            # Validate
            self.validate_data(df)
            
            # Process
            result_df = self.process_data(df)
            pipeline_logger.log_metric("output_rows", result_df.count())
            
            # Write
            self.write_data(result_df)
            
            # Health check
            self.health.check_row_count(self.config.get("target_table"), min_rows=1)
            
            pipeline_logger.complete(status="success")
            return result_df
            
        except Exception as e:
            self.alert_manager.send_alert("high", "Pipeline failed", {"error": str(e)})
            pipeline_logger.complete(status="failed")
            raise

# Usage
pipeline = ProductionPipeline("customer_etl", env="prod")
result = pipeline.run()

print("\n✅ All solutions completed!")
print("\nKey Takeaways:")
print("- Always implement error handling and retry logic")
print("- Use structured logging for observability")
print("- Monitor pipeline health continuously")
print("- Make pipelines idempotent")
print("- Handle schema evolution gracefully")
print("- Use configuration management")
print("- Optimize for performance")
print("- Test thoroughly before deployment")
