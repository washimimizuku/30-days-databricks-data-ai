# Day 23: Databricks Jobs

## Overview

Today you'll learn how to create and manage production data pipelines using Databricks Jobs. Jobs are the primary way to schedule and orchestrate automated workloads in Databricks.

**Time**: 2 hours  
**Focus**: Create multi-task jobs, configure scheduling, manage dependencies

## Learning Objectives

By the end of today, you will be able to:
- Create and configure Databricks Jobs
- Build multi-task workflows with dependencies
- Configure job clusters vs all-purpose clusters
- Set up job scheduling and triggers
- Monitor job runs and handle failures
- Use job parameters and notifications
- Implement retry logic and timeout settings
- Understand job permissions and access control

## What are Databricks Jobs?

Databricks Jobs are automated workflows that run notebooks, Python scripts, JAR files, or SQL queries on a schedule or on-demand. They're essential for production data pipelines.

### Key Concepts

**Job**: A workflow that runs one or more tasks
**Task**: A single unit of work (notebook, script, SQL, etc.)
**Job Cluster**: Ephemeral cluster created for the job
**Run**: A single execution of a job
**Trigger**: What causes a job to run (schedule, file arrival, etc.)

## Job Types

### 1. Single-Task Jobs
Simple jobs that run one notebook or script:
```python
# Single task - runs one notebook
Task: Process Daily Sales
  - Notebook: /Workspace/pipelines/process_sales
  - Cluster: Job cluster (auto-created)
  - Schedule: Daily at 2 AM
```

### 2. Multi-Task Jobs (Workflows)
Complex workflows with multiple dependent tasks:
```python
# Multi-task workflow
Job: Daily ETL Pipeline
  ├── Task 1: Ingest Data (no dependencies)
  ├── Task 2: Clean Data (depends on Task 1)
  ├── Task 3: Transform Data (depends on Task 2)
  └── Task 4: Load to Gold (depends on Task 3)
```

### 3. Continuous Jobs
Jobs that run continuously (for streaming):
```python
# Continuous job for streaming
Task: Process Streaming Events
  - Type: Continuous
  - Notebook: /Workspace/streaming/process_events
  - Restarts automatically on failure
```

## Job Clusters vs All-Purpose Clusters

### Job Clusters (Recommended for Production)
- **Created**: When job starts
- **Terminated**: When job completes
- **Cost**: Lower (only pay for job duration)
- **Use Case**: Production automated workloads
- **Isolation**: Each job gets its own cluster

### All-Purpose Clusters
- **Created**: Manually by user
- **Terminated**: Manually or by auto-termination
- **Cost**: Higher (pay for entire uptime)
- **Use Case**: Interactive development and testing
- **Sharing**: Multiple users can share

**Best Practice**: Always use job clusters for production jobs!

## Creating a Job (UI)

### Step 1: Navigate to Workflows
1. Click "Workflows" in the left sidebar
2. Click "Create Job"

### Step 2: Configure Basic Settings
```yaml
Job Name: Daily Sales Pipeline
Description: Processes daily sales data through Bronze → Silver → Gold
Tags: production, sales, daily
```

### Step 3: Add Tasks
```yaml
Task 1:
  Name: Ingest to Bronze
  Type: Notebook
  Notebook Path: /Workspace/pipelines/bronze_ingestion
  Cluster: New job cluster
  
Task 2:
  Name: Clean to Silver
  Type: Notebook
  Notebook Path: /Workspace/pipelines/silver_cleaning
  Depends On: Ingest to Bronze
  Cluster: Same as Task 1
  
Task 3:
  Name: Aggregate to Gold
  Type: Notebook
  Notebook Path: /Workspace/pipelines/gold_aggregation
  Depends On: Clean to Silver
  Cluster: Same as Task 1
```

### Step 4: Configure Cluster
```yaml
Cluster Configuration:
  Cluster Mode: Standard
  Databricks Runtime: 13.3 LTS
  Node Type: i3.xlarge
  Workers: 2-8 (autoscaling)
  Auto Termination: Immediate (after job completes)
```

### Step 5: Set Schedule
```yaml
Schedule:
  Type: Cron
  Expression: 0 2 * * * (Daily at 2 AM UTC)
  Timezone: America/New_York
  Pause Status: Active
```

## Creating a Job (API)

### Using Databricks REST API
```python
import requests
import json

# API endpoint
url = "https://<databricks-instance>/api/2.1/jobs/create"

# Job configuration
job_config = {
    "name": "Daily Sales Pipeline",
    "tasks": [
        {
            "task_key": "ingest_bronze",
            "notebook_task": {
                "notebook_path": "/Workspace/pipelines/bronze_ingestion",
                "base_parameters": {
                    "date": "{{job.start_time.iso_date}}"
                }
            },
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2
            }
        },
        {
            "task_key": "clean_silver",
            "depends_on": [{"task_key": "ingest_bronze"}],
            "notebook_task": {
                "notebook_path": "/Workspace/pipelines/silver_cleaning"
            },
            "job_cluster_key": "shared_cluster"
        },
        {
            "task_key": "aggregate_gold",
            "depends_on": [{"task_key": "clean_silver"}],
            "notebook_task": {
                "notebook_path": "/Workspace/pipelines/gold_aggregation"
            },
            "job_cluster_key": "shared_cluster"
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
                    "max_workers": 8
                }
            }
        }
    ],
    "schedule": {
        "quartz_cron_expression": "0 0 2 * * ?",
        "timezone_id": "America/New_York"
    },
    "email_notifications": {
        "on_failure": ["data-team@company.com"],
        "on_success": ["data-team@company.com"]
    },
    "timeout_seconds": 7200,
    "max_concurrent_runs": 1
}

# Create job
response = requests.post(
    url,
    headers={"Authorization": f"Bearer {token}"},
    json=job_config
)

job_id = response.json()["job_id"]
print(f"Created job with ID: {job_id}")
```

### Using Databricks CLI
```bash
# Create job from JSON file
databricks jobs create --json-file job_config.json

# Run job
databricks jobs run-now --job-id 123

# List all jobs
databricks jobs list

# Get job details
databricks jobs get --job-id 123
```

## Job Parameters

### Passing Parameters to Notebooks
```python
# In the job configuration
"base_parameters": {
    "date": "2024-01-15",
    "environment": "production",
    "batch_size": "1000"
}

# In the notebook
dbutils.widgets.text("date", "")
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("batch_size", "100")

date = dbutils.widgets.get("date")
environment = dbutils.widgets.get("environment")
batch_size = int(dbutils.widgets.get("batch_size"))

print(f"Processing date: {date}")
print(f"Environment: {environment}")
print(f"Batch size: {batch_size}")
```

### Dynamic Parameters
```python
# Use job runtime variables
"base_parameters": {
    "run_date": "{{job.start_time.iso_date}}",
    "run_id": "{{job.run_id}}",
    "job_id": "{{job.id}}"
}
```

## Task Dependencies

### Linear Dependencies
```python
# Task A → Task B → Task C
Task A (no dependencies)
  ↓
Task B (depends on A)
  ↓
Task C (depends on B)
```

### Parallel Tasks
```python
# Multiple tasks run in parallel
Task A (no dependencies) ─┐
Task B (no dependencies) ─┼→ Task D (depends on A, B, C)
Task C (no dependencies) ─┘
```

### Complex DAG
```python
# Directed Acyclic Graph
        Task A
       ↙     ↘
   Task B   Task C
       ↘     ↙
        Task D
          ↓
        Task E
```

## Scheduling Options

### Cron Expressions
```python
# Every day at 2 AM
"0 0 2 * * ?"

# Every hour
"0 0 * * * ?"

# Every Monday at 9 AM
"0 0 9 ? * MON"

# First day of month at midnight
"0 0 0 1 * ?"

# Every 15 minutes
"0 */15 * * * ?"
```

### Trigger Types

**1. Scheduled (Cron)**
```python
"schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "America/New_York",
    "pause_status": "UNPAUSED"
}
```

**2. File Arrival (Trigger)**
```python
"trigger": {
    "file_arrival": {
        "url": "dbfs:/mnt/landing/sales/*.csv",
        "min_time_between_triggers_seconds": 300
    }
}
```

**3. Manual (On-Demand)**
```python
# Run via UI, API, or CLI
databricks jobs run-now --job-id 123
```

**4. Continuous**
```python
"continuous": {
    "pause_status": "UNPAUSED"
}
```

## Retry Logic and Timeouts

### Task-Level Retry
```python
"retry_on_timeout": true,
"max_retries": 3,
"min_retry_interval_millis": 60000,  # 1 minute
"timeout_seconds": 3600  # 1 hour
```

### Job-Level Timeout
```python
"timeout_seconds": 7200,  # 2 hours for entire job
"max_concurrent_runs": 1  # Only one run at a time
```

### Retry Strategy
```python
# Exponential backoff
Attempt 1: Immediate
Attempt 2: Wait 1 minute
Attempt 3: Wait 2 minutes
Attempt 4: Wait 4 minutes
```

## Notifications

### Email Notifications
```python
"email_notifications": {
    "on_start": ["team@company.com"],
    "on_success": ["team@company.com"],
    "on_failure": ["team@company.com", "oncall@company.com"],
    "on_duration_warning_threshold_exceeded": ["team@company.com"],
    "no_alert_for_skipped_runs": false
}
```

### Webhook Notifications
```python
"webhook_notifications": {
    "on_failure": [
        {
            "id": "slack-webhook-id"
        }
    ]
}
```

## Monitoring Jobs

### Job Run Status
- **PENDING**: Waiting to start
- **RUNNING**: Currently executing
- **TERMINATING**: Shutting down
- **TERMINATED**: Completed successfully
- **SKIPPED**: Skipped due to conditions
- **INTERNAL_ERROR**: Databricks error
- **FAILED**: Task failed

### Viewing Job Runs
```python
# Get recent runs
GET /api/2.1/jobs/runs/list?job_id=123&limit=25

# Get specific run details
GET /api/2.1/jobs/runs/get?run_id=456

# Get run output
GET /api/2.1/jobs/runs/get-output?run_id=456
```

### Metrics to Monitor
- **Success Rate**: % of successful runs
- **Duration**: Average and p95 run time
- **Failure Rate**: % of failed runs
- **Queue Time**: Time waiting to start
- **Cost**: DBU consumption per run

## Job Permissions

### Permission Levels
- **CAN VIEW**: See job configuration and runs
- **CAN MANAGE RUN**: Trigger runs, cancel runs
- **CAN MANAGE**: Edit job, change permissions
- **IS OWNER**: Full control, delete job

### Setting Permissions
```python
"access_control_list": [
    {
        "user_name": "data.engineer@company.com",
        "permission_level": "IS_OWNER"
    },
    {
        "group_name": "data-team",
        "permission_level": "CAN_MANAGE"
    },
    {
        "group_name": "analysts",
        "permission_level": "CAN_VIEW"
    }
]
```

## Best Practices

### 1. Use Job Clusters
```python
# ✅ Good: Job cluster
"new_cluster": {
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 2
}

# ❌ Bad: All-purpose cluster
"existing_cluster_id": "0123-456789-abc123"
```

### 2. Implement Idempotency
```python
# Make jobs safe to re-run
def process_data(date):
    # Delete existing data for this date
    spark.sql(f"DELETE FROM target WHERE date = '{date}'")
    
    # Process and insert new data
    new_data = extract_data(date)
    new_data.write.mode("append").saveAsTable("target")
```

### 3. Use Shared Job Clusters
```python
# Share cluster across tasks to save startup time
"job_clusters": [
    {
        "job_cluster_key": "shared_cluster",
        "new_cluster": {...}
    }
],
"tasks": [
    {
        "task_key": "task1",
        "job_cluster_key": "shared_cluster"
    },
    {
        "task_key": "task2",
        "job_cluster_key": "shared_cluster"
    }
]
```

### 4. Set Appropriate Timeouts
```python
# Task timeout: 2x expected duration
"timeout_seconds": 3600  # 1 hour if task usually takes 30 min

# Job timeout: Sum of all tasks + buffer
"timeout_seconds": 7200  # 2 hours for 3 tasks
```

### 5. Configure Retries Wisely
```python
# Retry transient failures
"max_retries": 3,
"retry_on_timeout": true,

# Don't retry data quality failures
# (handle in notebook logic instead)
```

### 6. Use Parameters for Flexibility
```python
# Parameterize dates, environments, etc.
"base_parameters": {
    "date": "{{job.start_time.iso_date}}",
    "environment": "production"
}
```

### 7. Monitor and Alert
```python
# Set up notifications
"email_notifications": {
    "on_failure": ["oncall@company.com"],
    "on_duration_warning_threshold_exceeded": ["team@company.com"]
},
"timeout_seconds": 7200
```

### 8. Document Your Jobs
```python
# Use clear names and descriptions
"name": "Daily Sales ETL - Bronze to Gold",
"description": "Processes daily sales data through medallion architecture. Runs at 2 AM EST daily.",
"tags": {
    "team": "data-engineering",
    "criticality": "high",
    "domain": "sales"
}
```

## Common Patterns

### Pattern 1: Medallion Architecture Pipeline
```python
Job: Medallion ETL
  ├── Bronze Ingestion (parallel)
  │   ├── Ingest Orders
  │   ├── Ingest Customers
  │   └── Ingest Products
  ├── Silver Cleaning (depends on Bronze)
  │   ├── Clean Orders
  │   ├── Clean Customers
  │   └── Clean Products
  └── Gold Aggregation (depends on Silver)
      ├── Daily Sales Summary
      └── Customer Lifetime Value
```

### Pattern 2: Incremental Processing
```python
Job: Incremental Update
  ├── Check for New Data
  ├── Process New Data (if exists)
  └── Update Downstream Tables
```

### Pattern 3: Data Quality Pipeline
```python
Job: Data Quality Check
  ├── Run Data Quality Tests
  ├── If Pass: Promote to Production
  └── If Fail: Send Alert and Stop
```

## Troubleshooting

### Common Issues

**1. Job Stuck in PENDING**
- Cause: No available clusters
- Solution: Check cluster quotas, use job clusters

**2. Task Fails Immediately**
- Cause: Notebook path incorrect
- Solution: Verify notebook path exists

**3. Timeout Errors**
- Cause: Task takes longer than timeout
- Solution: Increase timeout or optimize task

**4. Dependency Errors**
- Cause: Circular dependencies
- Solution: Review task dependencies (must be DAG)

**5. Parameter Not Found**
- Cause: Widget not defined in notebook
- Solution: Add dbutils.widgets in notebook

## Exam Tips

### Key Points to Remember
1. **Job clusters** are cheaper and recommended for production
2. **Task dependencies** must form a DAG (no cycles)
3. **Cron expressions** use Quartz format
4. **max_concurrent_runs** controls parallel executions
5. **Retries** help with transient failures
6. **Timeouts** prevent runaway jobs
7. **Permissions** control who can view/manage jobs

### Common Exam Questions
- When to use job cluster vs all-purpose cluster?
- How to create task dependencies?
- What's the format for cron expressions?
- How to pass parameters to notebooks?
- What are the job run statuses?

## Summary

Today you learned:
- ✅ Creating single and multi-task jobs
- ✅ Configuring job clusters
- ✅ Setting up task dependencies
- ✅ Scheduling with cron expressions
- ✅ Passing parameters to tasks
- ✅ Implementing retry logic
- ✅ Setting up notifications
- ✅ Monitoring job runs
- ✅ Best practices for production jobs

## Next Steps

1. Complete the hands-on exercises
2. Create a multi-task job for your pipeline
3. Configure scheduling and notifications
4. Take the quiz
5. Move on to Day 24: Databricks Repos & Version Control

---

**Production Tip**: Always test jobs in a development environment before deploying to production. Use job parameters to switch between environments!
