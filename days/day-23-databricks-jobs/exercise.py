# Databricks notebook source
# MAGIC %md
# MAGIC # Day 23: Databricks Jobs - Exercises
# MAGIC 
# MAGIC ## Overview
# MAGIC Complete these exercises to master Databricks Jobs.
# MAGIC 
# MAGIC **Topics Covered**:
# MAGIC - Creating single-task jobs
# MAGIC - Building multi-task workflows
# MAGIC - Configuring job clusters
# MAGIC - Setting up schedules
# MAGIC - Managing dependencies
# MAGIC - Implementing retry logic
# MAGIC - Monitoring jobs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the setup from `setup.md` first

# COMMAND ----------

# Verify setup
USE jobs_demo;
SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Job Basics (10 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.1: Create a Simple Notebook
# MAGIC Create a notebook that prints "Hello from Databricks Jobs!"

# COMMAND ----------

# TODO: Create a simple notebook
# Path: /Workspace/exercises/hello_job
# Content: Print message and current timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.2: Add Notebook Parameters
# MAGIC Modify the notebook to accept a "name" parameter

# COMMAND ----------

# TODO: Add widget for "name" parameter
# Print: "Hello {name} from Databricks Jobs!"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.3: Create Job via UI
# MAGIC Create a job that runs your hello notebook
# MAGIC - Name: "Hello Job"
# MAGIC - Task: Run hello_job notebook
# MAGIC - Cluster: New job cluster

# COMMAND ----------

# TODO: Document the steps you took to create the job

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.4: Run Job Manually
# MAGIC Trigger the job manually and observe the output

# COMMAND ----------

# TODO: Run the job and note the run ID

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.5: Pass Parameters to Job
# MAGIC Run the job again with parameter: name="DataEngineer"

# COMMAND ----------

# TODO: Run job with parameters

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.6: View Job Run History
# MAGIC Check the run history for your job

# COMMAND ----------

# TODO: Navigate to job runs and document what you see

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.7: Check Job Run Output
# MAGIC View the output of your job run

# COMMAND ----------

# TODO: Find and view the notebook output

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.8: Configure Job Timeout
# MAGIC Set a timeout of 300 seconds for your job

# COMMAND ----------

# TODO: Edit job settings to add timeout

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.9: Add Email Notification
# MAGIC Configure email notification on job failure

# COMMAND ----------

# TODO: Add your email to failure notifications

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.10: Clone a Job
# MAGIC Create a copy of your job with a different name

# COMMAND ----------

# TODO: Clone the job to "Hello Job - Copy"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Multi-Task Jobs (15 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.1: Create Task 1 - Data Generation
# MAGIC Create a notebook that generates sample data

# COMMAND ----------

# TODO: Create notebook that generates 100 records
# Path: /Workspace/exercises/generate_data
# Write to: /tmp/exercises/raw_data/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.2: Create Task 2 - Data Processing
# MAGIC Create a notebook that processes the generated data

# COMMAND ----------

# TODO: Create notebook that reads and processes data
# Path: /Workspace/exercises/process_data
# Read from: /tmp/exercises/raw_data/
# Write to: /tmp/exercises/processed_data/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.3: Create Task 3 - Data Aggregation
# MAGIC Create a notebook that aggregates processed data

# COMMAND ----------

# TODO: Create notebook that aggregates data
# Path: /Workspace/exercises/aggregate_data
# Read from: /tmp/exercises/processed_data/
# Write to: /tmp/exercises/aggregated_data/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.4: Create Multi-Task Job
# MAGIC Create a job with all three tasks

# COMMAND ----------

# TODO: Create job "Data Pipeline" with 3 tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.5: Configure Linear Dependencies
# MAGIC Set up dependencies: Task1 → Task2 → Task3

# COMMAND ----------

# TODO: Configure task dependencies

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.6: Use Shared Job Cluster
# MAGIC Configure tasks 2 and 3 to use the same job cluster

# COMMAND ----------

# TODO: Set up shared job cluster

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.7: Run Multi-Task Job
# MAGIC Execute the complete pipeline

# COMMAND ----------

# TODO: Run the job and monitor all tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.8: View Task Dependencies Graph
# MAGIC Visualize the task dependency graph

# COMMAND ----------

# TODO: Find and view the DAG visualization

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.9: Add Parallel Tasks
# MAGIC Add two tasks that run in parallel after Task 1

# COMMAND ----------

# TODO: Add Task 2A and Task 2B that both depend on Task 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.10: Create Fan-In Pattern
# MAGIC Add a task that depends on both parallel tasks

# COMMAND ----------

# TODO: Add Task 3 that depends on both Task 2A and Task 2B

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.11: Pass Data Between Tasks
# MAGIC Use notebook exit values to pass data between tasks

# COMMAND ----------

# TODO: Task 1 returns record count
# Task 2 uses that count in processing

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.12: Configure Task-Level Retries
# MAGIC Set different retry counts for different tasks

# COMMAND ----------

# TODO: Task 1: 3 retries, Task 2: 2 retries, Task 3: 1 retry

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.13: Set Task Timeouts
# MAGIC Configure appropriate timeouts for each task

# COMMAND ----------

# TODO: Set timeouts based on expected duration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.14: Add Task Descriptions
# MAGIC Add clear descriptions to all tasks

# COMMAND ----------

# TODO: Add meaningful descriptions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.15: Test Failure Handling
# MAGIC Intentionally fail a task and observe behavior

# COMMAND ----------

# TODO: Make Task 2 fail and see what happens to Task 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Job Scheduling (10 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.1: Schedule Daily Job
# MAGIC Schedule job to run daily at 2 AM

# COMMAND ----------

# TODO: Add cron schedule: 0 0 2 * * ?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.2: Schedule Hourly Job
# MAGIC Create a job that runs every hour

# COMMAND ----------

# TODO: Cron expression for hourly execution

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.3: Schedule Weekly Job
# MAGIC Schedule job for every Monday at 9 AM

# COMMAND ----------

# TODO: Cron expression for weekly Monday execution

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.4: Schedule with Timezone
# MAGIC Set schedule timezone to your local timezone

# COMMAND ----------

# TODO: Configure timezone in schedule

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.5: Pause a Schedule
# MAGIC Temporarily pause the job schedule

# COMMAND ----------

# TODO: Pause the schedule without deleting it

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.6: Resume a Schedule
# MAGIC Resume the paused schedule

# COMMAND ----------

# TODO: Unpause the schedule

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.7: Schedule Every 15 Minutes
# MAGIC Create a schedule that runs every 15 minutes

# COMMAND ----------

# TODO: Cron expression for 15-minute intervals

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.8: Schedule First Day of Month
# MAGIC Schedule job for 1st of every month at midnight

# COMMAND ----------

# TODO: Cron expression for monthly execution

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.9: View Next Run Time
# MAGIC Check when the job is scheduled to run next

# COMMAND ----------

# TODO: Find next scheduled run time

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.10: Modify Existing Schedule
# MAGIC Change the schedule from daily to weekly

# COMMAND ----------

# TODO: Update the cron expression

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Job Monitoring (10 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.1: View Active Runs
# MAGIC List all currently running jobs

# COMMAND ----------

# TODO: Find active job runs in the UI

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.2: Check Run Duration
# MAGIC Find the duration of the last job run

# COMMAND ----------

# TODO: Get duration from run history

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.3: View Task-Level Metrics
# MAGIC Check metrics for individual tasks

# COMMAND ----------

# TODO: View metrics for each task in a run

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.4: Export Run Logs
# MAGIC Download logs from a job run

# COMMAND ----------

# TODO: Export logs for analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.5: Check Cluster Metrics
# MAGIC View cluster utilization during job run

# COMMAND ----------

# TODO: Check CPU, memory usage

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.6: Calculate Success Rate
# MAGIC Calculate success rate for last 10 runs

# COMMAND ----------

# TODO: Count successful vs failed runs

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.7: Find Failed Runs
# MAGIC List all failed runs in the last week

# COMMAND ----------

# TODO: Filter run history for failures

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.8: Analyze Failure Reasons
# MAGIC Identify common failure patterns

# COMMAND ----------

# TODO: Review error messages from failed runs

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.9: Track Job Costs
# MAGIC Estimate DBU cost for job runs

# COMMAND ----------

# TODO: Calculate approximate cost based on cluster size and duration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.10: Set Up Monitoring Dashboard
# MAGIC Create a dashboard to monitor job health

# COMMAND ----------

# TODO: Document key metrics to track

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Advanced Patterns (10 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.1: Implement Idempotent Job
# MAGIC Make a job safe to re-run

# COMMAND ----------

# TODO: Use DELETE + INSERT or MERGE pattern

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.2: Create Conditional Task
# MAGIC Add a task that only runs if previous task succeeds

# COMMAND ----------

# TODO: Use task dependencies and conditions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.3: Implement Data Quality Check
# MAGIC Add a task that validates data quality

# COMMAND ----------

# TODO: Create validation task that fails if quality is poor

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.4: Use Dynamic Parameters
# MAGIC Pass runtime variables to tasks

# COMMAND ----------

# TODO: Use {{job.start_time.iso_date}} and other variables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.5: Create Notification Webhook
# MAGIC Set up Slack/Teams notification

# COMMAND ----------

# TODO: Configure webhook for job notifications

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.6: Implement Exponential Backoff
# MAGIC Configure retry with increasing delays

# COMMAND ----------

# TODO: Set min_retry_interval_millis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.7: Create Job from API
# MAGIC Use REST API to create a job

# COMMAND ----------

# TODO: Write Python code to create job via API

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.8: Trigger Job from Another Job
# MAGIC Create a job that triggers another job

# COMMAND ----------

# TODO: Use dbutils.notebook.run or API call

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.9: Implement Job Chaining
# MAGIC Create a series of dependent jobs

# COMMAND ----------

# TODO: Job A completes → triggers Job B → triggers Job C

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.10: Create Continuous Job
# MAGIC Set up a job that runs continuously

# COMMAND ----------

# TODO: Configure continuous execution for streaming

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Production Best Practices (10 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.1: Use Job Clusters
# MAGIC Convert all-purpose cluster to job cluster

# COMMAND ----------

# TODO: Replace existing_cluster_id with new_cluster config

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.2: Configure Autoscaling
# MAGIC Enable autoscaling for job cluster

# COMMAND ----------

# TODO: Set min_workers and max_workers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.3: Set Max Concurrent Runs
# MAGIC Prevent multiple simultaneous runs

# COMMAND ----------

# TODO: Set max_concurrent_runs = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.4: Add Job Tags
# MAGIC Tag jobs for organization

# COMMAND ----------

# TODO: Add tags: team, environment, criticality

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.5: Document Job Purpose
# MAGIC Add comprehensive job description

# COMMAND ----------

# TODO: Write clear description with purpose, schedule, dependencies

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.6: Set Up Alerting
# MAGIC Configure alerts for job failures

# COMMAND ----------

# TODO: Set up email and webhook notifications

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.7: Implement Health Checks
# MAGIC Add pre-flight checks before main processing

# COMMAND ----------

# TODO: Create task that validates prerequisites

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.8: Use Spot Instances
# MAGIC Configure job cluster to use spot instances

# COMMAND ----------

# TODO: Enable spot instances for cost savings

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.9: Set Job Permissions
# MAGIC Configure appropriate access control

# COMMAND ----------

# TODO: Set permissions for team members

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.10: Create Job Documentation
# MAGIC Document the complete job workflow

# COMMAND ----------

# TODO: Create README with:
# - Job purpose
# - Task descriptions
# - Dependencies
# - Schedule
# - Troubleshooting guide

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Challenges (5 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 1: Create Complete Medallion Pipeline Job
# MAGIC Build a production-ready medallion architecture job

# COMMAND ----------

# TODO: Bronze → Silver → Gold with all best practices

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 2: Implement SLA Monitoring
# MAGIC Track if jobs complete within SLA

# COMMAND ----------

# TODO: Monitor duration and alert if exceeds threshold

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 3: Create Job Template
# MAGIC Build reusable job configuration template

# COMMAND ----------

# TODO: Create parameterized JSON template

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 4: Implement Circuit Breaker
# MAGIC Stop downstream tasks if upstream fails repeatedly

# COMMAND ----------

# TODO: Add logic to prevent cascading failures

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 5: Build Job Orchestration Framework
# MAGIC Create a framework for managing multiple jobs

# COMMAND ----------

# TODO: Build system to manage job lifecycle

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC You've completed 65 exercises covering:
# MAGIC - ✅ Job basics (10 exercises)
# MAGIC - ✅ Multi-task workflows (15 exercises)
# MAGIC - ✅ Job scheduling (10 exercises)
# MAGIC - ✅ Job monitoring (10 exercises)
# MAGIC - ✅ Advanced patterns (10 exercises)
# MAGIC - ✅ Production best practices (10 exercises)
# MAGIC - ✅ Bonus challenges (5 exercises)
# MAGIC 
# MAGIC Next steps:
# MAGIC 1. Review your solutions
# MAGIC 2. Take the quiz
# MAGIC 3. Move on to Day 24: Databricks Repos & Version Control
