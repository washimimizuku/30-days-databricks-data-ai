# Day 23 Setup: Databricks Jobs

## Overview

This setup creates notebooks and resources for practicing Databricks Jobs. You'll build a complete multi-task job that processes data through a medallion architecture.

## Prerequisites

- Databricks workspace access
- Ability to create jobs (requires appropriate permissions)
- Completed Days 1-22 (understanding of Delta Lake, streaming, etc.)

## Setup Instructions

### Step 1: Create Database

```sql
-- Create database for jobs practice
CREATE DATABASE IF NOT EXISTS jobs_demo;
USE jobs_demo;
```

### Step 2: Create Sample Tables

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# Create Bronze table
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_sales (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    amount DECIMAL(10,2),
    order_date DATE,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
USING DELTA
PARTITIONED BY (order_date)
""")

# Create Silver table
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_sales (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    amount DECIMAL(10,2),
    order_date DATE,
    processed_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (order_date)
""")

# Create Gold table
spark.sql("""
CREATE TABLE IF NOT EXISTS gold_daily_sales (
    sale_date DATE,
    total_orders INT,
    total_revenue DECIMAL(12,2),
    unique_customers INT,
    avg_order_value DECIMAL(10,2),
    last_updated TIMESTAMP
)
USING DELTA
PARTITIONED BY (sale_date)
""")

print("✅ Tables created successfully")
```

### Step 3: Create Source Data Directory

```python
# Create directory for landing files
dbutils.fs.mkdirs("/tmp/jobs_demo/landing/")

# Function to generate sample sales data
def generate_sales_data(date, num_records=100):
    """Generate sample sales data for a given date"""
    data = []
    for i in range(num_records):
        data.append((
            f"ORD{date.strftime('%Y%m%d')}{i:04d}",
            f"CUST{random.randint(1, 50):04d}",
            f"PROD{random.randint(1, 20):03d}",
            random.randint(1, 10),
            round(random.uniform(10.0, 500.0), 2),
            date,
            datetime.now(),
            f"sales_{date.strftime('%Y%m%d')}.csv"
        ))
    
    return spark.createDataFrame(data, [
        "order_id", "customer_id", "product_id", "quantity", 
        "amount", "order_date", "ingestion_timestamp", "source_file"
    ])

# Generate sample data for last 7 days
from datetime import date, timedelta

for i in range(7):
    current_date = date.today() - timedelta(days=i)
    df = generate_sales_data(current_date, 100)
    
    # Write to landing directory
    file_path = f"/tmp/jobs_demo/landing/sales_{current_date.strftime('%Y%m%d')}.csv"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(file_path)
    
    print(f"✅ Generated data for {current_date}")

print("\n✅ Sample data generated in /tmp/jobs_demo/landing/")
```

### Step 4: Create Job Notebooks

Create the following notebooks in your workspace:

#### Notebook 1: `/Workspace/jobs_demo/bronze_ingestion`

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Ingestion
# MAGIC Ingests raw sales data from landing zone to Bronze table

# COMMAND ----------

# Get parameters
dbutils.widgets.text("date", "")
dbutils.widgets.text("source_path", "/tmp/jobs_demo/landing/")

date_param = dbutils.widgets.get("date")
source_path = dbutils.widgets.get("source_path")

print(f"Processing date: {date_param}")
print(f"Source path: {source_path}")

# COMMAND ----------

from pyspark.sql.functions import *

# Read source files
if date_param:
    # Process specific date
    file_pattern = f"{source_path}sales_{date_param.replace('-', '')}.csv"
else:
    # Process all files
    file_pattern = f"{source_path}*.csv"

print(f"Reading from: {file_pattern}")

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(file_pattern) \
    .withColumn("ingestion_timestamp", current_timestamp())

# COMMAND ----------

# Write to Bronze table
df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("order_date") \
    .saveAsTable("jobs_demo.bronze_sales")

record_count = df.count()
print(f"✅ Ingested {record_count} records to Bronze")

# COMMAND ----------

# Return metrics for job monitoring
dbutils.notebook.exit(f"{{\"records_ingested\": {record_count}}}")
```

#### Notebook 2: `/Workspace/jobs_demo/silver_cleaning`

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Cleaning
# MAGIC Cleans and validates data from Bronze to Silver

# COMMAND ----------

# Get parameters
dbutils.widgets.text("date", "")
date_param = dbutils.widgets.get("date")

print(f"Processing date: {date_param}")

# COMMAND ----------

from pyspark.sql.functions import *

# Read from Bronze
if date_param:
    df = spark.table("jobs_demo.bronze_sales") \
        .filter(col("order_date") == date_param)
else:
    df = spark.table("jobs_demo.bronze_sales")

print(f"Read {df.count()} records from Bronze")

# COMMAND ----------

# Data quality checks and cleaning
cleaned_df = df \
    .filter(col("order_id").isNotNull()) \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("amount") > 0) \
    .filter(col("quantity") > 0) \
    .dropDuplicates(["order_id"]) \
    .select(
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "amount",
        "order_date",
        current_timestamp().alias("processed_timestamp")
    )

# COMMAND ----------

# Write to Silver table (overwrite partition)
if date_param:
    # Overwrite specific partition
    cleaned_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"order_date = '{date_param}'") \
        .partitionBy("order_date") \
        .saveAsTable("jobs_demo.silver_sales")
else:
    # Append all
    cleaned_df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("order_date") \
        .saveAsTable("jobs_demo.silver_sales")

record_count = cleaned_df.count()
print(f"✅ Cleaned {record_count} records to Silver")

# COMMAND ----------

dbutils.notebook.exit(f"{{\"records_cleaned\": {record_count}}}")
```

#### Notebook 3: `/Workspace/jobs_demo/gold_aggregation`

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Aggregation
# MAGIC Creates business-level aggregates from Silver data

# COMMAND ----------

# Get parameters
dbutils.widgets.text("date", "")
date_param = dbutils.widgets.get("date")

print(f"Processing date: {date_param}")

# COMMAND ----------

from pyspark.sql.functions import *

# Read from Silver
if date_param:
    df = spark.table("jobs_demo.silver_sales") \
        .filter(col("order_date") == date_param)
else:
    df = spark.table("jobs_demo.silver_sales")

# COMMAND ----------

# Create daily aggregates
daily_summary = df \
    .groupBy("order_date") \
    .agg(
        count("*").alias("total_orders"),
        sum("amount").alias("total_revenue"),
        countDistinct("customer_id").alias("unique_customers"),
        avg("amount").alias("avg_order_value")
    ) \
    .withColumn("last_updated", current_timestamp()) \
    .withColumnRenamed("order_date", "sale_date")

# COMMAND ----------

# Write to Gold table (overwrite partition)
if date_param:
    daily_summary.write \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"sale_date = '{date_param}'") \
        .partitionBy("sale_date") \
        .saveAsTable("jobs_demo.gold_daily_sales")
else:
    daily_summary.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("sale_date") \
        .saveAsTable("jobs_demo.gold_daily_sales")

record_count = daily_summary.count()
print(f"✅ Created {record_count} daily summaries in Gold")

# COMMAND ----------

# Show results
display(daily_summary.orderBy(desc("sale_date")))

# COMMAND ----------

dbutils.notebook.exit(f"{{\"dates_processed\": {record_count}}}")
```

### Step 5: Create Job Configuration File

Create a file `job_config.json`:

```json
{
  "name": "Daily Sales ETL Pipeline",
  "description": "Processes daily sales data through Bronze → Silver → Gold layers",
  "tags": {
    "environment": "development",
    "team": "data-engineering"
  },
  "tasks": [
    {
      "task_key": "bronze_ingestion",
      "description": "Ingest raw sales data to Bronze layer",
      "notebook_task": {
        "notebook_path": "/Workspace/jobs_demo/bronze_ingestion",
        "base_parameters": {
          "date": "{{job.start_time.iso_date}}",
          "source_path": "/tmp/jobs_demo/landing/"
        }
      },
      "new_cluster": {
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2,
        "spark_conf": {
          "spark.databricks.delta.preview.enabled": "true"
        }
      },
      "timeout_seconds": 1800,
      "max_retries": 2
    },
    {
      "task_key": "silver_cleaning",
      "description": "Clean and validate data in Silver layer",
      "depends_on": [
        {
          "task_key": "bronze_ingestion"
        }
      ],
      "notebook_task": {
        "notebook_path": "/Workspace/jobs_demo/silver_cleaning",
        "base_parameters": {
          "date": "{{job.start_time.iso_date}}"
        }
      },
      "job_cluster_key": "shared_cluster",
      "timeout_seconds": 1800,
      "max_retries": 2
    },
    {
      "task_key": "gold_aggregation",
      "description": "Create business aggregates in Gold layer",
      "depends_on": [
        {
          "task_key": "silver_cleaning"
        }
      ],
      "notebook_task": {
        "notebook_path": "/Workspace/jobs_demo/gold_aggregation",
        "base_parameters": {
          "date": "{{job.start_time.iso_date}}"
        }
      },
      "job_cluster_key": "shared_cluster",
      "timeout_seconds": 1800,
      "max_retries": 2
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
  ],
  "email_notifications": {
    "on_failure": ["your-email@company.com"]
  },
  "timeout_seconds": 7200,
  "max_concurrent_runs": 1,
  "format": "MULTI_TASK"
}
```

### Step 6: Helper Functions

```python
# Function to check job status
def check_job_status(job_id):
    """Check the status of a job"""
    from databricks.sdk import WorkspaceClient
    
    w = WorkspaceClient()
    job = w.jobs.get(job_id)
    
    print(f"Job Name: {job.settings.name}")
    print(f"Job ID: {job.job_id}")
    print(f"Created: {job.created_time}")
    print(f"Creator: {job.creator_user_name}")
    
    return job

# Function to get recent runs
def get_recent_runs(job_id, limit=5):
    """Get recent runs for a job"""
    from databricks.sdk import WorkspaceClient
    
    w = WorkspaceClient()
    runs = w.jobs.list_runs(job_id=job_id, limit=limit)
    
    for run in runs:
        print(f"\nRun ID: {run.run_id}")
        print(f"Status: {run.state.life_cycle_state}")
        print(f"Start Time: {run.start_time}")
        if run.end_time:
            duration = (run.end_time - run.start_time) / 1000 / 60
            print(f"Duration: {duration:.2f} minutes")
    
    return runs

# Function to trigger job run
def trigger_job(job_id, parameters=None):
    """Trigger a job run"""
    from databricks.sdk import WorkspaceClient
    
    w = WorkspaceClient()
    
    if parameters:
        run = w.jobs.run_now(job_id=job_id, notebook_params=parameters)
    else:
        run = w.jobs.run_now(job_id=job_id)
    
    print(f"✅ Triggered job run: {run.run_id}")
    return run.run_id

print("✅ Helper functions loaded")
```

## Verification

Run these commands to verify your setup:

```python
# Check tables exist
tables = spark.sql("SHOW TABLES IN jobs_demo").collect()
print(f"Created {len(tables)} tables:")
for table in tables:
    print(f"  - {table.tableName}")

# Check landing files
files = dbutils.fs.ls("/tmp/jobs_demo/landing/")
print(f"\nLanding files: {len(files)}")

# Check Bronze data
bronze_count = spark.table("jobs_demo.bronze_sales").count()
print(f"\nBronze records: {bronze_count}")
```

## Next Steps

1. Create the three notebooks in your workspace
2. Create a job using the UI or API
3. Configure the job with the three tasks
4. Set up task dependencies
5. Run the job and monitor execution
6. Complete the exercises in `exercise.py`

---

**Note**: Make sure you have the necessary permissions to create and run jobs in your Databricks workspace.
