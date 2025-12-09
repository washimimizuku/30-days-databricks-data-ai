# Databricks notebook source
# MAGIC %md
# MAGIC # Day 23: Databricks Jobs - Solutions
# MAGIC 
# MAGIC Complete solutions for all exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Job Basics - Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1.1: Create a Simple Notebook

# COMMAND ----------

# Create notebook: /Workspace/exercises/hello_job
from datetime import datetime

print("Hello from Databricks Jobs!")
print(f"Current timestamp: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1.2: Add Notebook Parameters

# COMMAND ----------

# Add widget for parameter
dbutils.widgets.text("name", "World")

# Get parameter value
name = dbutils.widgets.get("name")

# Print personalized message
print(f"Hello {name} from Databricks Jobs!")
print(f"Current timestamp: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1.3-1.10: Job Creation Steps
# MAGIC 
# MAGIC **Steps to create job via UI:**
# MAGIC 1. Click "Workflows" in left sidebar
# MAGIC 2. Click "Create Job"
# MAGIC 3. Configure:
# MAGIC    - Name: "Hello Job"
# MAGIC    - Task name: "hello_task"
# MAGIC    - Type: Notebook
# MAGIC    - Path: /Workspace/exercises/hello_job
# MAGIC    - Cluster: New job cluster
# MAGIC 4. Click "Create"
# MAGIC 
# MAGIC **To run manually:**
# MAGIC - Click "Run now" button
# MAGIC 
# MAGIC **To pass parameters:**
# MAGIC - Click "Run now with different parameters"
# MAGIC - Add: name = "DataEngineer"
# MAGIC 
# MAGIC **To configure timeout:**
# MAGIC - Edit job → Advanced → Timeout: 300 seconds
# MAGIC 
# MAGIC **To add notifications:**
# MAGIC - Edit job → Notifications → Add email on failure

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Multi-Task Jobs - Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 2.1: Data Generation Notebook

# COMMAND ----------

# Notebook: /Workspace/exercises/generate_data
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

# Generate sample data
data = []
for i in range(100):
    data.append((
        f"ID{i:04d}",
        f"Customer{random.randint(1, 50)}",
        random.randint(1, 100),
        round(random.uniform(10.0, 1000.0), 2),
        datetime.now() - timedelta(days=random.randint(0, 30))
    ))

df = spark.createDataFrame(data, ["id", "customer", "quantity", "amount", "date"])

# Write to location
output_path = "/tmp/exercises/raw_data/"
df.write.mode("overwrite").parquet(output_path)

record_count = df.count()
print(f"✅ Generated {record_count} records")

# Return count for next task
dbutils.notebook.exit(f'{{"record_count": {record_count}}}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 2.2: Data Processing Notebook

# COMMAND ----------

# Notebook: /Workspace/exercises/process_data
from pyspark.sql.functions import *

# Read raw data
input_path = "/tmp/exercises/raw_data/"
df = spark.read.parquet(input_path)

print(f"Read {df.count()} records")

# Process data
processed_df = df \
    .filter(col("amount") > 0) \
    .filter(col("quantity") > 0) \
    .withColumn("total_value", col("amount") * col("quantity")) \
    .withColumn("processed_timestamp", current_timestamp())

# Write processed data
output_path = "/tmp/exercises/processed_data/"
processed_df.write.mode("overwrite").parquet(output_path)

record_count = processed_df.count()
print(f"✅ Processed {record_count} records")

dbutils.notebook.exit(f'{{"processed_count": {record_count}}}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 2.3: Data Aggregation Notebook

# COMMAND ----------

# Notebook: /Workspace/exercises/aggregate_data
from pyspark.sql.functions import *

# Read processed data
input_path = "/tmp/exercises/processed_data/"
df = spark.read.parquet(input_path)

# Create aggregates
aggregated_df = df \
    .groupBy("customer") \
    .agg(
        count("*").alias("order_count"),
        sum("amount").alias("total_amount"),
        sum("total_value").alias("total_value"),
        avg("amount").alias("avg_amount")
    ) \
    .orderBy(desc("total_value"))

# Write aggregated data
output_path = "/tmp/exercises/aggregated_data/"
aggregated_df.write.mode("overwrite").parquet(output_path)

# Display results
display(aggregated_df)

customer_count = aggregated_df.count()
print(f"✅ Aggregated data for {customer_count} customers")

dbutils.notebook.exit(f'{{"customer_count": {customer_count}}}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 2.4-2.15: Multi-Task Job Configuration

# COMMAND ----------

# Job configuration JSON
job_config = {
    "name": "Data Pipeline",
    "tasks": [
        {
            "task_key": "generate_data",
            "description": "Generate sample data",
            "notebook_task": {
                "notebook_path": "/Workspace/exercises/generate_data"
            },
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2
            },
            "timeout_seconds": 600,
            "max_retries": 3
        },
        {
            "task_key": "process_data",
            "description": "Process generated data",
            "depends_on": [{"task_key": "generate_data"}],
            "notebook_task": {
                "notebook_path": "/Workspace/exercises/process_data"
            },
            "job_cluster_key": "shared_cluster",
            "timeout_seconds": 900,
            "max_retries": 2
        },
        {
            "task_key": "aggregate_data",
            "description": "Aggregate processed data",
            "depends_on": [{"task_key": "process_data"}],
            "notebook_task": {
                "notebook_path": "/Workspace/exercises/aggregate_data"
            },
            "job_cluster_key": "shared_cluster",
            "timeout_seconds": 600,
            "max_retries": 1
        }
    ],
    "job_clusters": [
        {
            "job_cluster_key": "shared_cluster",
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2,
                "autoscale": {
                    "min_workers": 2,
                    "max_workers": 4
                }
            }
        }
    ]
}

print("✅ Multi-task job configuration created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Job Scheduling - Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 3.1-3.10: Cron Expressions

# COMMAND ----------

# Common cron expressions
cron_expressions = {
    "Daily at 2 AM": "0 0 2 * * ?",
    "Every hour": "0 0 * * * ?",
    "Every Monday at 9 AM": "0 0 9 ? * MON",
    "Every 15 minutes": "0 */15 * * * ?",
    "First day of month": "0 0 0 1 * ?",
    "Weekdays at 8 AM": "0 0 8 ? * MON-FRI",
    "Every 30 minutes": "0 */30 * * * ?",
    "Twice daily (6 AM, 6 PM)": "0 0 6,18 * * ?"
}

for description, expression in cron_expressions.items():
    print(f"{description}: {expression}")

# COMMAND ----------

# Schedule configuration example
schedule_config = {
    "schedule": {
        "quartz_cron_expression": "0 0 2 * * ?",
        "timezone_id": "America/New_York",
        "pause_status": "UNPAUSED"
    }
}

print("✅ Schedule configuration created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Job Monitoring - Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 4.1-4.10: Monitoring Functions

# COMMAND ----------

from databricks.sdk import WorkspaceClient

def monitor_job(job_id):
    """Comprehensive job monitoring"""
    w = WorkspaceClient()
    
    # Get job details
    job = w.jobs.get(job_id)
    print(f"Job: {job.settings.name}")
    print(f"Created: {job.created_time}")
    
    # Get recent runs
    runs = list(w.jobs.list_runs(job_id=job_id, limit=10))
    
    # Calculate metrics
    total_runs = len(runs)
    successful_runs = sum(1 for r in runs if r.state.result_state == "SUCCESS")
    failed_runs = sum(1 for r in runs if r.state.result_state == "FAILED")
    
    success_rate = (successful_runs / total_runs * 100) if total_runs > 0 else 0
    
    print(f"\nMetrics (last {total_runs} runs):")
    print(f"  Success Rate: {success_rate:.1f}%")
    print(f"  Successful: {successful_runs}")
    print(f"  Failed: {failed_runs}")
    
    # Calculate average duration
    durations = []
    for run in runs:
        if run.end_time and run.start_time:
            duration = (run.end_time - run.start_time) / 1000 / 60
            durations.append(duration)
    
    if durations:
        avg_duration = sum(durations) / len(durations)
        print(f"  Avg Duration: {avg_duration:.2f} minutes")
    
    return {
        "success_rate": success_rate,
        "total_runs": total_runs,
        "avg_duration": avg_duration if durations else 0
    }

print("✅ Monitoring functions created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Advanced Patterns - Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 5.1: Idempotent Job Pattern

# COMMAND ----------

def idempotent_processing(date_param):
    """Process data idempotently - safe to re-run"""
    from pyspark.sql.functions import *
    
    # Delete existing data for this date
    spark.sql(f"""
        DELETE FROM target_table
        WHERE date = '{date_param}'
    """)
    
    # Process new data
    new_data = spark.table("source_table") \
        .filter(col("date") == date_param)
    
    # Insert processed data
    new_data.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("target_table")
    
    print(f"✅ Processed {new_data.count()} records for {date_param}")

# Alternative: Use MERGE for upsert
def idempotent_merge(date_param):
    """Use MERGE for idempotent processing"""
    spark.sql(f"""
        MERGE INTO target_table t
        USING (
            SELECT * FROM source_table WHERE date = '{date_param}'
        ) s
        ON t.id = s.id AND t.date = s.date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

print("✅ Idempotent patterns created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 5.2-5.3: Data Quality Check Task

# COMMAND ----------

def data_quality_check(table_name, date_param):
    """Validate data quality before processing"""
    from pyspark.sql.functions import *
    
    df = spark.table(table_name).filter(col("date") == date_param)
    
    # Quality checks
    total_records = df.count()
    null_records = df.filter(col("amount").isNull()).count()
    negative_amounts = df.filter(col("amount") < 0).count()
    
    # Calculate quality score
    quality_score = ((total_records - null_records - negative_amounts) / total_records * 100) if total_records > 0 else 0
    
    print(f"Data Quality Report:")
    print(f"  Total Records: {total_records}")
    print(f"  Null Values: {null_records}")
    print(f"  Negative Amounts: {negative_amounts}")
    print(f"  Quality Score: {quality_score:.2f}%")
    
    # Fail if quality is poor
    if quality_score < 95:
        raise Exception(f"Data quality check failed! Score: {quality_score:.2f}%")
    
    print("✅ Data quality check passed")
    return quality_score

# Use in notebook
# quality_score = data_quality_check("bronze_sales", "2024-01-15")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 5.4: Dynamic Parameters

# COMMAND ----------

# Job configuration with dynamic parameters
dynamic_job_config = {
    "name": "Dynamic Parameter Job",
    "tasks": [
        {
            "task_key": "process_daily",
            "notebook_task": {
                "notebook_path": "/Workspace/pipelines/process_data",
                "base_parameters": {
                    "date": "{{job.start_time.iso_date}}",
                    "run_id": "{{job.run_id}}",
                    "job_id": "{{job.id}}",
                    "user": "{{job.trigger.user_name}}"
                }
            },
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2
            }
        }
    ]
}

# In notebook, access parameters:
# date = dbutils.widgets.get("date")
# run_id = dbutils.widgets.get("run_id")

print("✅ Dynamic parameter configuration created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 5.7: Create Job via API

# COMMAND ----------

def create_job_via_api(job_config):
    """Create a job using Databricks API"""
    from databricks.sdk import WorkspaceClient
    
    w = WorkspaceClient()
    
    # Create job
    job = w.jobs.create(
        name=job_config["name"],
        tasks=job_config["tasks"],
        job_clusters=job_config.get("job_clusters", []),
        email_notifications=job_config.get("email_notifications", {}),
        timeout_seconds=job_config.get("timeout_seconds", 0),
        max_concurrent_runs=job_config.get("max_concurrent_runs", 1)
    )
    
    print(f"✅ Created job with ID: {job.job_id}")
    return job.job_id

# Example usage:
# job_id = create_job_via_api(job_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 5.8: Trigger Job from Another Job

# COMMAND ----------

def trigger_downstream_job(job_id, parameters=None):
    """Trigger another job from current job"""
    from databricks.sdk import WorkspaceClient
    
    w = WorkspaceClient()
    
    # Trigger job
    run = w.jobs.run_now(
        job_id=job_id,
        notebook_params=parameters or {}
    )
    
    print(f"✅ Triggered job {job_id}, run ID: {run.run_id}")
    
    # Optionally wait for completion
    # w.jobs.wait_get_run_job_terminated_or_skipped(run.run_id)
    
    return run.run_id

# Use in notebook:
# downstream_job_id = 123
# trigger_downstream_job(downstream_job_id, {"date": "2024-01-15"})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 5.9: Job Chaining Pattern

# COMMAND ----------

# Job A configuration
job_a_config = {
    "name": "Job A - Data Ingestion",
    "tasks": [
        {
            "task_key": "ingest",
            "notebook_task": {
                "notebook_path": "/Workspace/pipelines/ingest"
            },
            "new_cluster": {...}
        },
        {
            "task_key": "trigger_job_b",
            "depends_on": [{"task_key": "ingest"}],
            "notebook_task": {
                "notebook_path": "/Workspace/pipelines/trigger_next_job",
                "base_parameters": {
                    "next_job_id": "456"  # Job B ID
                }
            },
            "job_cluster_key": "shared_cluster"
        }
    ]
}

# In trigger_next_job notebook:
# next_job_id = int(dbutils.widgets.get("next_job_id"))
# trigger_downstream_job(next_job_id)

print("✅ Job chaining pattern created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 5.10: Continuous Job Configuration

# COMMAND ----------

continuous_job_config = {
    "name": "Continuous Streaming Job",
    "tasks": [
        {
            "task_key": "stream_processing",
            "notebook_task": {
                "notebook_path": "/Workspace/streaming/process_events"
            },
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2
            }
        }
    ],
    "continuous": {
        "pause_status": "UNPAUSED"
    },
    "max_concurrent_runs": 1
}

print("✅ Continuous job configuration created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Production Best Practices - Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 6.1-6.10: Production Job Template

# COMMAND ----------

def create_production_job(
    name,
    notebooks,
    schedule_cron,
    notification_emails,
    tags=None
):
    """Create a production-ready job with best practices"""
    
    # Build tasks with dependencies
    tasks = []
    for i, notebook in enumerate(notebooks):
        task = {
            "task_key": notebook["task_key"],
            "description": notebook.get("description", ""),
            "notebook_task": {
                "notebook_path": notebook["path"],
                "base_parameters": notebook.get("parameters", {})
            },
            "timeout_seconds": notebook.get("timeout", 3600),
            "max_retries": notebook.get("retries", 2),
            "min_retry_interval_millis": 60000,
            "retry_on_timeout": True
        }
        
        # Add dependencies
        if i > 0:
            task["depends_on"] = [{"task_key": notebooks[i-1]["task_key"]}]
            task["job_cluster_key"] = "shared_cluster"
        else:
            task["new_cluster"] = {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2,
                "autoscale": {
                    "min_workers": 2,
                    "max_workers": 8
                },
                "spark_conf": {
                    "spark.databricks.delta.preview.enabled": "true",
                    "spark.databricks.delta.retentionDurationCheck.enabled": "false"
                }
            }
        
        tasks.append(task)
    
    # Complete job configuration
    job_config = {
        "name": name,
        "description": f"Production job: {name}",
        "tags": tags or {"environment": "production"},
        "tasks": tasks,
        "job_clusters": [
            {
                "job_cluster_key": "shared_cluster",
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "node_type_id": "i3.xlarge",
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    },
                    "aws_attributes": {
                        "availability": "SPOT_WITH_FALLBACK",
                        "spot_bid_price_percent": 100
                    }
                }
            }
        ],
        "schedule": {
            "quartz_cron_expression": schedule_cron,
            "timezone_id": "America/New_York",
            "pause_status": "UNPAUSED"
        },
        "email_notifications": {
            "on_failure": notification_emails,
            "on_duration_warning_threshold_exceeded": notification_emails,
            "no_alert_for_skipped_runs": False
        },
        "timeout_seconds": 7200,
        "max_concurrent_runs": 1,
        "format": "MULTI_TASK"
    }
    
    return job_config

# Example usage
production_job = create_production_job(
    name="Production Sales Pipeline",
    notebooks=[
        {
            "task_key": "bronze_ingestion",
            "path": "/Workspace/production/bronze_ingestion",
            "description": "Ingest raw sales data",
            "parameters": {"date": "{{job.start_time.iso_date}}"},
            "timeout": 1800,
            "retries": 3
        },
        {
            "task_key": "silver_cleaning",
            "path": "/Workspace/production/silver_cleaning",
            "description": "Clean and validate data",
            "parameters": {"date": "{{job.start_time.iso_date}}"},
            "timeout": 1800,
            "retries": 2
        },
        {
            "task_key": "gold_aggregation",
            "path": "/Workspace/production/gold_aggregation",
            "description": "Create business aggregates",
            "parameters": {"date": "{{job.start_time.iso_date}}"},
            "timeout": 1200,
            "retries": 1
        }
    ],
    schedule_cron="0 0 2 * * ?",  # Daily at 2 AM
    notification_emails=["data-team@company.com"],
    tags={
        "environment": "production",
        "team": "data-engineering",
        "criticality": "high",
        "domain": "sales"
    }
)

print("✅ Production job template created")
print(f"Job name: {production_job['name']}")
print(f"Tasks: {len(production_job['tasks'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus: Complete Production Framework

# COMMAND ----------

class JobManager:
    """Complete job management framework"""
    
    def __init__(self):
        from databricks.sdk import WorkspaceClient
        self.client = WorkspaceClient()
    
    def create_job(self, config):
        """Create a new job"""
        job = self.client.jobs.create(**config)
        print(f"✅ Created job: {job.job_id}")
        return job.job_id
    
    def run_job(self, job_id, parameters=None):
        """Trigger a job run"""
        run = self.client.jobs.run_now(
            job_id=job_id,
            notebook_params=parameters or {}
        )
        print(f"✅ Started run: {run.run_id}")
        return run.run_id
    
    def monitor_run(self, run_id):
        """Monitor a job run"""
        run = self.client.jobs.get_run(run_id)
        return {
            "state": run.state.life_cycle_state,
            "result": run.state.result_state if run.state.result_state else "RUNNING"
        }
    
    def get_job_metrics(self, job_id, days=7):
        """Get job performance metrics"""
        from datetime import datetime, timedelta
        
        # Get recent runs
        runs = list(self.client.jobs.list_runs(
            job_id=job_id,
            limit=100
        ))
        
        # Filter by date
        cutoff = datetime.now() - timedelta(days=days)
        recent_runs = [r for r in runs if r.start_time and r.start_time > cutoff.timestamp() * 1000]
        
        # Calculate metrics
        total = len(recent_runs)
        successful = sum(1 for r in recent_runs if r.state.result_state == "SUCCESS")
        failed = sum(1 for r in recent_runs if r.state.result_state == "FAILED")
        
        durations = []
        for run in recent_runs:
            if run.end_time and run.start_time:
                duration = (run.end_time - run.start_time) / 1000 / 60
                durations.append(duration)
        
        return {
            "total_runs": total,
            "successful_runs": successful,
            "failed_runs": failed,
            "success_rate": (successful / total * 100) if total > 0 else 0,
            "avg_duration_minutes": sum(durations) / len(durations) if durations else 0,
            "max_duration_minutes": max(durations) if durations else 0
        }
    
    def health_check(self, job_id):
        """Comprehensive job health check"""
        metrics = self.get_job_metrics(job_id)
        
        print(f"\nJob Health Report (Job ID: {job_id})")
        print("=" * 50)
        print(f"Success Rate: {metrics['success_rate']:.1f}%")
        print(f"Total Runs: {metrics['total_runs']}")
        print(f"Avg Duration: {metrics['avg_duration_minutes']:.2f} minutes")
        print(f"Max Duration: {metrics['max_duration_minutes']:.2f} minutes")
        
        # Health status
        if metrics['success_rate'] >= 95:
            status = "HEALTHY ✅"
        elif metrics['success_rate'] >= 80:
            status = "WARNING ⚠️"
        else:
            status = "CRITICAL ❌"
        
        print(f"\nStatus: {status}")
        
        return status

# Usage
manager = JobManager()
print("✅ Job management framework ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC ✅ Completed all solutions:
# MAGIC - Job basics and creation
# MAGIC - Multi-task workflows
# MAGIC - Scheduling and monitoring
# MAGIC - Advanced patterns
# MAGIC - Production best practices
# MAGIC - Complete management framework
# MAGIC 
# MAGIC **Key Takeaways:**
# MAGIC 1. Always use job clusters for production
# MAGIC 2. Implement idempotent processing
# MAGIC 3. Configure appropriate retries and timeouts
# MAGIC 4. Monitor job health metrics
# MAGIC 5. Use shared clusters for efficiency
# MAGIC 6. Tag and document jobs properly
# MAGIC 7. Set up proper notifications
# MAGIC 8. Test in dev before production
