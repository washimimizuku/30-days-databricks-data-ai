-- Day 2: Clusters & Compute - Exercises
-- =========================================================
-- Note: Some exercises are UI-based. Follow instructions in comments.

-- =========================================================
-- PART 1: CLUSTER CREATION (UI Exercises)
-- =========================================================

-- Exercise 1: Create Your First Cluster
-- UI Task: Create a cluster with these specifications:
-- - Name: learning-cluster
-- - Mode: Single Node (Community Edition) or Standard
-- - Runtime: Latest LTS version
-- - Auto-termination: 30 minutes
-- 
-- After creation, note the following information:
-- Cluster ID: _______________
-- Spark Version: _______________
-- Databricks Runtime: _______________

-- Exercise 2: Verify Cluster Information
-- Run this query to check your cluster details:

SELECT 
    current_version() as spark_version,
    current_database() as current_db,
    current_user() as user_name,
    current_timestamp() as current_time;


-- Exercise 3: Check Spark Configuration
-- TODO: Write a query to display Spark configuration
-- Hint: Use spark.conf.get() in Python or SHOW CONF in SQL

-- YOUR CODE HERE (Python cell):
-- %python
-- spark.conf.get("spark.databricks.clusterUsageTags.clusterName")


-- =========================================================
-- PART 2: CLUSTER MODES UNDERSTANDING
-- =========================================================

-- Exercise 4: Cluster Mode Comparison
-- Create a markdown cell explaining the differences:
-- %md
-- ## Cluster Modes Comparison
-- 
-- | Feature | Standard | High Concurrency | Single Node |
-- |---------|----------|------------------|-------------|
-- | Users | [Fill in] | [Fill in] | [Fill in] |
-- | Languages | [Fill in] | [Fill in] | [Fill in] |
-- | Workers | [Fill in] | [Fill in] | [Fill in] |
-- | Use Case | [Fill in] | [Fill in] | [Fill in] |


-- Exercise 5: When to Use Each Mode
-- TODO: Write SQL comments explaining when to use each cluster mode
-- Consider: cost, performance, security, use case

-- Standard Mode:
-- Use when: _______________

-- High Concurrency Mode:
-- Use when: _______________

-- Single Node Mode:
-- Use when: _______________


-- =========================================================
-- PART 3: RUNTIME VERSIONS
-- =========================================================

-- Exercise 6: Runtime Version Selection
-- %python
-- # TODO: Display available Databricks Runtime information
-- # Run this to see your current runtime
-- print(f"Databricks Runtime: {spark.conf.get('spark.databricks.clusterUsageTags.sparkVersion')}")
-- print(f"Spark Version: {spark.version}")

-- Exercise 7: Runtime Types
-- Create a table comparing runtime types:

SELECT * FROM VALUES
    ('Standard', 'Core Spark + Databricks features', 'General workloads'),
    ('ML', 'Standard + ML libraries', 'Machine Learning'),
    ('Photon', 'Optimized query engine', 'SQL analytics'),
    ('Light', 'Minimal libraries', 'Job clusters')
AS runtime_types(type, features, use_case);


-- =========================================================
-- PART 4: AUTOSCALING CONFIGURATION
-- =========================================================

-- Exercise 8: Autoscaling Calculation
-- A cluster has min_workers=2, max_workers=8
-- TODO: Calculate the following scenarios:

-- Scenario 1: Light load (10% CPU usage)
-- Expected workers: _______________

-- Scenario 2: Medium load (60% CPU usage)
-- Expected workers: _______________

-- Scenario 3: Heavy load (95% CPU usage)
-- Expected workers: _______________


-- Exercise 9: Autoscaling Configuration (UI Task)
-- UI Task: Modify your cluster to enable autoscaling:
-- - Min workers: 1
-- - Max workers: 3
-- 
-- After configuration, answer:
-- What happens when workload increases? _______________
-- What happens when workload decreases? _______________
-- How long before scale down? _______________


-- =========================================================
-- PART 5: NODE TYPES
-- =========================================================

-- Exercise 10: Node Type Selection
-- Create a comparison of node types:

SELECT * FROM VALUES
    ('General Purpose', 'Balanced CPU/Memory', 'm5.xlarge', 'Most workloads'),
    ('Memory Optimized', 'High memory', 'r5.xlarge', 'Large datasets'),
    ('Compute Optimized', 'High CPU', 'c5.xlarge', 'Processing heavy'),
    ('Storage Optimized', 'High I/O', 'i3.xlarge', 'Data intensive'),
    ('GPU', 'GPU acceleration', 'p3.2xlarge', 'ML training')
AS node_types(category, characteristics, example_aws, use_case);


-- Exercise 11: Cost Optimization
-- TODO: Write a query that calculates estimated costs
-- Assumptions:
-- - DBU cost: $0.15 per DBU
-- - Instance cost: $0.50 per hour
-- - Runtime: 8 hours per day
-- - Workers: 2

SELECT 
    2 as num_workers,
    8 as hours_per_day,
    0.50 as instance_cost_per_hour,
    0.15 as dbu_cost,
    -- TODO: Calculate daily cost
    (2 * 8 * 0.50) as daily_instance_cost,
    -- TODO: Calculate monthly cost (30 days)
    (2 * 8 * 0.50 * 30) as monthly_instance_cost;


-- =========================================================
-- PART 6: AUTO-TERMINATION
-- =========================================================

-- Exercise 12: Auto-Termination Settings
-- TODO: Answer these questions about auto-termination:

-- Question 1: What is the default auto-termination period?
-- Answer: _______________

-- Question 2: What happens to running queries when cluster terminates?
-- Answer: _______________

-- Question 3: Can you restart a terminated cluster?
-- Answer: _______________

-- Question 4: Recommended auto-termination for development?
-- Answer: _______________

-- Question 5: Recommended auto-termination for production?
-- Answer: _______________


-- Exercise 13: Auto-Termination Configuration (UI Task)
-- UI Task: Set auto-termination to 15 minutes
-- 1. Go to cluster configuration
-- 2. Find "Auto Termination" setting
-- 3. Set to 15 minutes
-- 4. Save configuration
-- 
-- Verification: Run a simple query, then wait 15 minutes
-- Did the cluster terminate? _______________


-- =========================================================
-- PART 7: CLUSTER MONITORING
-- =========================================================

-- Exercise 14: Monitor Cluster Metrics (UI Task)
-- UI Task: Explore cluster monitoring
-- 1. Go to your cluster page
-- 2. Click on "Metrics" tab
-- 3. Observe the following:
--    - CPU Usage: _______________
--    - Memory Usage: _______________
--    - Active Executors: _______________


-- Exercise 15: Spark UI Exploration (UI Task)
-- UI Task: Access Spark UI
-- 1. Run a query (any query from previous exercises)
-- 2. Go to cluster page
-- 3. Click "Spark UI"
-- 4. Explore:
--    - Jobs tab: Number of jobs: _______________
--    - Stages tab: Number of stages: _______________
--    - Storage tab: Cached tables: _______________
--    - Executors tab: Number of executors: _______________


-- =========================================================
-- PART 8: CLUSTER POLICIES (Advanced)
-- =========================================================

-- Exercise 16: Understanding Cluster Policies
-- Create a table showing policy use cases:

SELECT * FROM VALUES
    ('Cost Control', 'Limit instance types and sizes', 'Prevent expensive clusters'),
    ('Security', 'Enforce encryption and access', 'Compliance requirements'),
    ('Standardization', 'Consistent configurations', 'Team consistency'),
    ('Resource Management', 'Limit cluster count', 'Prevent resource exhaustion')
AS policies(policy_type, description, benefit);


-- =========================================================
-- PART 9: CLUSTER TYPES COMPARISON
-- =========================================================

-- Exercise 17: All-Purpose vs Job Clusters
-- TODO: Complete this comparison table

SELECT * FROM VALUES
    ('All-Purpose', '[Cost]', '[Lifetime]', '[Use Case]', '[Sharing]'),
    ('Job', '[Cost]', '[Lifetime]', '[Use Case]', '[Sharing]')
AS cluster_comparison(type, cost, lifetime, use_case, sharing);

-- Fill in the blanks:
-- All-Purpose Cost: _______________
-- All-Purpose Lifetime: _______________
-- All-Purpose Use Case: _______________
-- All-Purpose Sharing: _______________
-- 
-- Job Cost: _______________
-- Job Lifetime: _______________
-- Job Use Case: _______________
-- Job Sharing: _______________


-- Exercise 18: Cluster Selection Decision Tree
-- TODO: For each scenario, choose the appropriate cluster type

-- Scenario 1: Interactive data exploration by data scientist
-- Cluster Type: _______________
-- Cluster Mode: _______________
-- Reasoning: _______________

-- Scenario 2: Scheduled ETL job running nightly
-- Cluster Type: _______________
-- Cluster Mode: _______________
-- Reasoning: _______________

-- Scenario 3: Multiple analysts running SQL queries
-- Cluster Type: _______________
-- Cluster Mode: _______________
-- Reasoning: _______________

-- Scenario 4: Learning Databricks on Community Edition
-- Cluster Type: _______________
-- Cluster Mode: _______________
-- Reasoning: _______________


-- =========================================================
-- PART 10: PRACTICAL EXERCISES
-- =========================================================

-- Exercise 19: Cluster Performance Test
-- Run this query and note the execution time:

SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT id) as unique_ids,
    AVG(value) as avg_value
FROM (
    SELECT 
        id,
        CAST(RAND() * 1000 AS INT) as value
    FROM RANGE(1000000)
);

-- Execution time: _______________
-- Number of tasks: _______________
-- Number of stages: _______________


-- Exercise 20: Cluster Restart Practice (UI Task)
-- UI Task: Practice cluster lifecycle management
-- 1. Terminate your cluster
-- 2. Wait for termination to complete
-- 3. Restart the cluster
-- 4. Note the startup time
-- 
-- Termination time: _______________
-- Startup time: _______________
-- Total downtime: _______________


-- =========================================================
-- REFLECTION QUESTIONS
-- =========================================================
-- Answer these in a markdown cell:
--
-- 1. What is the difference between all-purpose and job clusters?
-- 2. When should you enable autoscaling?
-- 3. What are the benefits of auto-termination?
-- 4. Which cluster mode should you use for shared analytics?
-- 5. What is the purpose of cluster policies?
-- 6. How do you choose the right node type?
-- 7. What is the difference between Standard and ML runtime?
-- 8. Why is LTS runtime recommended for production?
-- 9. How does High Concurrency mode provide isolation?
-- 10. What happens to data when a cluster terminates?


-- =========================================================
-- BONUS CHALLENGES
-- =========================================================

-- Bonus 1: Cost Optimization Strategy
-- TODO: Design a cost optimization strategy for a team with:
-- - 5 data engineers (interactive work)
-- - 10 data analysts (SQL queries)
-- - 3 scheduled ETL jobs
-- 
-- Your strategy:
-- _______________


-- Bonus 2: Cluster Configuration Template
-- TODO: Create a JSON-like configuration for an optimal cluster
-- Consider: runtime, autoscaling, auto-termination, node type

-- %md
-- ```json
-- {
--   "cluster_name": "_______________",
--   "spark_version": "_______________",
--   "node_type_id": "_______________",
--   "autoscale": {
--     "min_workers": ___,
--     "max_workers": ___
--   },
--   "autotermination_minutes": ___
-- }
-- ```


-- Bonus 3: Troubleshooting Scenarios
-- TODO: For each problem, suggest a solution

-- Problem 1: Cluster takes 10 minutes to start
-- Solution: _______________

-- Problem 2: Queries are running slowly
-- Solution: _______________

-- Problem 3: Cluster costs are too high
-- Solution: _______________

-- Problem 4: Out of memory errors
-- Solution: _______________

-- Problem 5: Cluster terminates unexpectedly
-- Solution: _______________


-- =========================================================
-- NOTES
-- =========================================================
-- Key Takeaways:
-- 1. All-purpose clusters for interactive work, job clusters for automation
-- 2. Autoscaling optimizes cost and performance
-- 3. Auto-termination prevents wasted resources
-- 4. Choose cluster mode based on use case and security needs
-- 5. LTS runtime for stability, latest for new features
-- 6. Monitor cluster metrics to optimize performance
-- 7. Use cluster policies to enforce standards
-- 8. Right-size node types for workload
