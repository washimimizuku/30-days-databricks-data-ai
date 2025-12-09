# Day 27: Production Best Practices - Setup

## Prerequisites

Before starting today's exercises, ensure you have:
- ✅ Completed Days 1-26
- ✅ Access to a Databricks workspace
- ✅ Understanding of error handling and logging
- ✅ Familiarity with production concepts

## Setup Instructions

### Step 1: Create Database

```sql
-- Create database for production exercises
CREATE DATABASE IF NOT EXISTS production_practice;
USE production_practice;
```

### Step 2: Create Sample Tables

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

spark = SparkSession.builder.getOrCreate()

# Create customers table
customers_data = [
    (1, "John Doe", "john@email.com", "2024-01-01", "2024-12-09 10:00:00"),
    (2, "Jane Smith", "jane@email.com", "2024-01-02", "2024-12-09 09:00:00"),
    (3, "Bob Johnson", "bob@email.com", "2024-01-03", "2024-12-08 15:00:00"),
    (4, "Alice Williams", "alice@email.com", "2024-01-04", "2024-12-07 12:00:00"),
    (5, "Charlie Brown", "charlie@email.com", "2024-01-05", "2024-12-09 08:00:00"),
]

customers_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("created_date", StringType(), False),
    StructField("updated_at", StringType(), False)
])

customers_df = spark.createDataFrame(customers_data, customers_schema)
customers_df.write.mode("overwrite").format("delta").saveAsTable("production_practice.customers")

print("✅ Created customers table")

# Create orders table with partitions
orders_data = []
for i in range(1, 101):
    date = (datetime(2024, 12, 1) + timedelta(days=i % 9)).strftime("%Y-%m-%d")
    orders_data.append((
        i,
        random.randint(1, 5),
        date,
        round(random.uniform(10, 500), 2),
        random.choice(["completed", "pending", "cancelled"])
    ))

orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_date", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("status", StringType(), False)
])

orders_df = spark.createDataFrame(orders_data, orders_schema)
orders_df.write.mode("overwrite").format("delta") \
    .partitionBy("order_date") \
    .saveAsTable("production_practice.orders")

print("✅ Created orders table (partitioned)")
```

### Step 3: Create Pipeline Metadata Tables

```python
# Create pipeline execution log table
spark.sql("""
    CREATE TABLE IF NOT EXISTS production_practice.pipeline_executions (
        execution_id STRING,
        pipeline_name STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        status STRING,
        rows_processed BIGINT,
        error_message STRING,
        metrics MAP<STRING, STRING>
    )
""")

# Create checkpoint table
spark.sql("""
    CREATE TABLE IF NOT EXISTS production_practice.pipeline_checkpoints (
        pipeline_name STRING,
        checkpoint_key STRING,
        checkpoint_value STRING,
        updated_at TIMESTAMP
    )
""")

# Create health check table
spark.sql("""
    CREATE TABLE IF NOT EXISTS production_practice.health_checks (
        check_id STRING,
        pipeline_name STRING,
        check_name STRING,
        check_status STRING,
        check_details STRING,
        checked_at TIMESTAMP
    )
""")

# Create alert log table
spark.sql("""
    CREATE TABLE IF NOT EXISTS production_practice.alert_log (
        alert_id STRING,
        pipeline_name STRING,
        severity STRING,
        message STRING,
        details STRING,
        created_at TIMESTAMP,
        resolved_at TIMESTAMP
    )
""")

print("✅ Created pipeline metadata tables")
```

### Step 4: Create Configuration File

```python
import json

# Create configuration directory
dbutils.fs.mkdirs("/dbfs/configs/")

# Development config
dev_config = {
    "environment": "dev",
    "source_table": "production_practice.customers",
    "target_table": "production_practice.customers_processed",
    "batch_size": 100,
    "max_retries": 3,
    "retry_delay": 5,
    "enable_monitoring": True,
    "alert_on_failure": False,
    "log_level": "DEBUG"
}

with open("/dbfs/configs/dev_config.json", "w") as f:
    json.dump(dev_config, f, indent=2)

# Production config
prod_config = {
    "environment": "prod",
    "source_table": "production_practice.customers",
    "target_table": "production_practice.customers_processed",
    "batch_size": 1000,
    "max_retries": 5,
    "retry_delay": 10,
    "enable_monitoring": True,
    "alert_on_failure": True,
    "log_level": "INFO"
}

with open("/dbfs/configs/prod_config.json", "w") as f:
    json.dump(prod_config, f, indent=2)

print("✅ Created configuration files")
```

### Step 5: Set Up Logging

```python
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("production_pipeline")
logger.info("Logging configured successfully")

print("✅ Logging configured")
```

### Step 6: Create Helper Classes

```python
# Structured Logger
class StructuredLogger:
    """Structured logging for production pipelines"""
    
    def __init__(self, name):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
    
    def log(self, level, message, **kwargs):
        """Log structured message"""
        import json
        from datetime import datetime
        
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

# Retry Decorator
def retry(max_attempts=3, delay=5, backoff=2):
    """Retry decorator with exponential backoff"""
    import time
    from functools import wraps
    
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

# Pipeline Config
class PipelineConfig:
    """Manage pipeline configuration"""
    
    def __init__(self, env="dev"):
        self.env = env
        self.config = self._load_config()
    
    def _load_config(self):
        """Load environment-specific config"""
        import json
        config_path = f"/dbfs/configs/{self.env}_config.json"
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def get(self, key, default=None):
        """Get config value"""
        return self.config.get(key, default)

# Checkpoint Manager
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
        from datetime import datetime
        
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

print("✅ Created helper classes")
```

### Step 7: Create Test Data with Issues

```python
# Create table with data quality issues for testing
problematic_data = [
    (1, "Valid Customer", "valid@email.com", "2024-12-09 10:00:00"),
    (2, None, "missing_name@email.com", "2024-12-09 09:00:00"),  # Missing name
    (3, "Duplicate", "dup@email.com", "2024-12-09 08:00:00"),
    (3, "Duplicate", "dup@email.com", "2024-12-09 08:00:00"),  # Duplicate
    (4, "Old Data", "old@email.com", "2024-01-01 10:00:00"),  # Very old
]

problematic_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("updated_at", StringType(), True)
])

problematic_df = spark.createDataFrame(problematic_data, problematic_schema)
problematic_df.write.mode("overwrite").format("delta") \
    .saveAsTable("production_practice.customers_problematic")

print("✅ Created test data with quality issues")
```

## Verification

Run these queries to verify your setup:

```sql
-- Check tables exist
SHOW TABLES IN production_practice;

-- Check data counts
SELECT 'customers' as table_name, COUNT(*) as row_count 
FROM production_practice.customers
UNION ALL
SELECT 'orders', COUNT(*) 
FROM production_practice.orders
UNION ALL
SELECT 'customers_problematic', COUNT(*) 
FROM production_practice.customers_problematic;

-- Check partitions
SHOW PARTITIONS production_practice.orders;

-- Check metadata tables
SELECT * FROM production_practice.pipeline_executions LIMIT 5;
SELECT * FROM production_practice.pipeline_checkpoints LIMIT 5;
SELECT * FROM production_practice.health_checks LIMIT 5;
SELECT * FROM production_practice.alert_log LIMIT 5;
```

Expected output:
- ✅ 7 tables created
- ✅ customers: 5 rows
- ✅ orders: 100 rows (partitioned by date)
- ✅ customers_problematic: 5 rows (with issues)
- ✅ 4 metadata tables (empty)
- ✅ Configuration files created
- ✅ Helper classes defined

## Troubleshooting

### Issue: Configuration files not found

**Solution**: Ensure `/dbfs/configs/` directory exists:
```python
dbutils.fs.mkdirs("/dbfs/configs/")
```

### Issue: Tables already exist

**Solution**: Drop and recreate:
```sql
DROP DATABASE IF EXISTS production_practice CASCADE;
```

### Issue: Logging not working

**Solution**: Restart Python kernel and reconfigure logging.

## Clean Up (Optional)

To remove all objects created today:

```sql
DROP DATABASE IF EXISTS production_practice CASCADE;
```

```python
# Remove config files
dbutils.fs.rm("/dbfs/configs/", recurse=True)
```

## Ready to Start!

Once you've completed the setup:
1. ✅ Database and tables created
2. ✅ Metadata tables created
3. ✅ Configuration files created
4. ✅ Helper classes defined
5. ✅ Test data with issues created
6. ✅ Verification queries successful

You're ready to begin the exercises in `exercise.py`!

---

**Setup Time**: 10-15 minutes  
**Total Tables Created**: 7 tables  
**Total Rows**: ~110 rows
