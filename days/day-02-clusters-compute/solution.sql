-- Day 2: Clusters & Compute - Solutions
-- =========================================================

-- =========================================================
-- PART 1: CLUSTER CREATION (UI Exercises)
-- =========================================================

-- Exercise 1: Create Your First Cluster
-- Solution: UI-based task
-- Expected configuration:
-- - Name: learning-cluster
-- - Mode: Single Node (Community Edition) or Standard
-- - Runtime: 13.3 LTS or latest available
-- - Auto-termination: 30 minutes
-- 
-- Example values:
-- Cluster ID: 0123-456789-abcd1234
-- Spark Version: 3.4.1
-- Databricks Runtime: 13.3 LTS

-- Exercise 2: Verify Cluster Information
SELECT 
    current_version() as spark_version,
    current_database() as current_db,
    current_user() as user_name,
    current_timestamp() as current_time;

-- Expected output:
-- spark_version: 3.4.1
-- current_db: default
-- user_name: your-email@example.com
-- current_time: 2024-12-08 10:30:00

-- Exercise 3: Check Spark Configuration
%python
# Solution: Display cluster configuration
print(f"Cluster Name: {spark.conf.get('spark.databricks.clusterUsageTags.clusterName')}")
print(f"Spark Version: {spark.version}")
print(f"Databricks Runtime: {spark.conf.get('spark.databricks.clusterUsageTags.sparkVersion')}")
print(f"Cluster ID: {spark.conf.get('spark.databricks.clusterUsageTags.clusterId')}")

-- Alternative SQL approach:
SHOW CONF;


-- =========================================================
-- PART 2: CLUSTER MODES UNDERSTANDING
-- =========================================================

-- Exercise 4: Cluster Mode Comparison
%md
## Cluster Modes Comparison

| Feature | Standard | High Concurrency | Single Node |
|---------|----------|------------------|-------------|
| Users | Single or multiple | Multiple (shared) | Single |
| Languages | SQL, Python, Scala, R | SQL, Python | SQL, Python, Scala, R |
| Workers | 1+ workers | 1+ workers | 0 workers (driver only) |
| Use Case | Data engineering | Shared analytics | Development, learning |
| Process Isolation | No | Yes | N/A |
| Table ACLs | No | Yes | No |
| Cost | Moderate | Higher | Lowest |

-- Exercise 5: When to Use Each Mode
-- Solution:

-- Standard Mode:
-- Use when: 
-- - Single user or team needs full Spark API access
-- - Data engineering workloads
-- - Need Scala or advanced Spark features
-- - Cost optimization is important
-- - Don't need process isolation

-- High Concurrency Mode:
-- Use when:
-- - Multiple users sharing cluster
-- - Need process isolation for security
-- - SQL and Python workloads only
-- - Table-level access control required
-- - Shared analytics environment

-- Single Node Mode:
-- Use when:
-- - Learning and development
-- - Small datasets
-- - Lightweight workloads
-- - Cost minimization is critical
-- - Community Edition (only option)


-- =========================================================
-- PART 3: RUNTIME VERSIONS
-- =========================================================

-- Exercise 6: Runtime Version Selection
%python
# Solution: Display runtime information
print(f"Databricks Runtime: {spark.conf.get('spark.databricks.clusterUsageTags.sparkVersion')}")
print(f"Spark Version: {spark.version}")
print(f"Scala Version: {spark.conf.get('spark.databricks.scalaVersion')}")
print(f"Python Version: {spark.conf.get('spark.databricks.pythonVersion')}")

-- Exercise 7: Runtime Types
SELECT * FROM VALUES
    ('Standard', 'Core Spark + Databricks features', 'General workloads'),
    ('ML', 'Standard + ML libraries (TensorFlow, PyTorch, etc.)', 'Machine Learning'),
    ('Photon', 'Optimized query engine (Premium/Enterprise)', 'SQL analytics, BI'),
    ('Light', 'Minimal libraries, faster startup', 'Job clusters, automation')
AS runtime_types(type, features, use_case);

-- Additional notes:
-- - LTS (Long Term Support): Stable, recommended for production
-- - Latest: Newest features, may have bugs
-- - ML Runtime: Includes scikit-learn, TensorFlow, PyTorch, XGBoost
-- - Photon: 2-3x faster for SQL queries (requires Premium/Enterprise)


-- =========================================================
-- PART 4: AUTOSCALING CONFIGURATION
-- =========================================================

-- Exercise 8: Autoscaling Calculation
-- Solution:

-- Scenario 1: Light load (10% CPU usage)
-- Expected workers: 2 (minimum)
-- Reasoning: Cluster maintains minimum workers even with light load

-- Scenario 2: Medium load (60% CPU usage)
-- Expected workers: 4-5 (mid-range)
-- Reasoning: Cluster scales up to handle increased load

-- Scenario 3: Heavy load (95% CPU usage)
-- Expected workers: 8 (maximum)
-- Reasoning: Cluster scales to maximum to handle heavy load

-- Autoscaling behavior:
-- - Scale up: Immediate when tasks are pending
-- - Scale down: After 10 minutes of low utilization
-- - Graceful: Waits for tasks to complete before removing workers

-- Exercise 9: Autoscaling Configuration (UI Task)
-- Solution: UI-based configuration

-- What happens when workload increases?
-- Answer: Cluster automatically adds workers (up to max_workers) to handle increased load

-- What happens when workload decreases?
-- Answer: Cluster removes workers (down to min_workers) after 10 minutes of low utilization

-- How long before scale down?
-- Answer: 10 minutes of low utilization (default)


-- =========================================================
-- PART 5: NODE TYPES
-- =========================================================

-- Exercise 10: Node Type Selection
SELECT * FROM VALUES
    ('General Purpose', 'Balanced CPU/Memory', 'm5.xlarge', 'Most workloads'),
    ('Memory Optimized', 'High memory (8:1 ratio)', 'r5.xlarge', 'Large datasets, caching'),
    ('Compute Optimized', 'High CPU (2:1 ratio)', 'c5.xlarge', 'Processing heavy, transformations'),
    ('Storage Optimized', 'High I/O, NVMe SSD', 'i3.xlarge', 'Data intensive, shuffle heavy'),
    ('GPU', 'GPU acceleration', 'p3.2xlarge', 'ML training, deep learning')
AS node_types(category, characteristics, example_aws, use_case);

-- Node type selection guide:
-- - General Purpose (m5): Default choice, balanced workloads
-- - Memory Optimized (r5): Large DataFrames, aggregations, caching
-- - Compute Optimized (c5): Complex transformations, UDFs
-- - Storage Optimized (i3): Large shuffles, joins, sorts
-- - GPU (p3, g4): Deep learning, ML model training

-- Exercise 11: Cost Optimization
SELECT 
    2 as num_workers,
    8 as hours_per_day,
    0.50 as instance_cost_per_hour,
    0.15 as dbu_cost_per_hour,
    -- Daily instance cost
    (2 * 8 * 0.50) as daily_instance_cost,
    -- Daily DBU cost (assuming 1 DBU per instance-hour)
    (2 * 8 * 0.15) as daily_dbu_cost,
    -- Total daily cost
    (2 * 8 * (0.50 + 0.15)) as total_daily_cost,
    -- Monthly cost (30 days)
    (2 * 8 * (0.50 + 0.15) * 30) as monthly_cost,
    -- Annual cost
    (2 * 8 * (0.50 + 0.15) * 365) as annual_cost;

-- Results:
-- Daily instance cost: $8.00
-- Daily DBU cost: $2.40
-- Total daily cost: $10.40
-- Monthly cost: $312.00
-- Annual cost: $3,796.00

-- Cost optimization strategies:
-- 1. Use job clusters for scheduled workloads (cheaper)
-- 2. Enable autoscaling to scale down during low usage
-- 3. Set aggressive auto-termination (15-30 minutes)
-- 4. Use spot/preemptible instances (50-70% savings)
-- 5. Right-size node types (don't over-provision)
-- 6. Use cluster pools for faster startup without idle cost


-- =========================================================
-- PART 6: AUTO-TERMINATION
-- =========================================================

-- Exercise 12: Auto-Termination Settings
-- Solutions:

-- Question 1: What is the default auto-termination period?
-- Answer: 120 minutes (2 hours) for all-purpose clusters

-- Question 2: What happens to running queries when cluster terminates?
-- Answer: Running queries are cancelled and fail. Results are lost.

-- Question 3: Can you restart a terminated cluster?
-- Answer: Yes, you can restart a terminated cluster. It will start with the same configuration.

-- Question 4: Recommended auto-termination for development?
-- Answer: 15-30 minutes (to minimize costs while allowing for breaks)

-- Question 5: Recommended auto-termination for production?
-- Answer: Disable auto-termination for all-purpose clusters, or use job clusters that terminate after completion

-- Additional notes:
-- - Auto-termination timer resets with any activity
-- - Scheduled jobs are not affected by auto-termination
-- - Cluster state (variables, temp views) is lost on termination
-- - Persistent data (Delta tables, files) is preserved

-- Exercise 13: Auto-Termination Configuration (UI Task)
-- Solution: UI-based configuration
-- Steps:
-- 1. Navigate to Compute → Your Cluster
-- 2. Click "Edit"
-- 3. Find "Auto Termination" section
-- 4. Set to 15 minutes
-- 5. Click "Confirm"

-- Verification: 
-- Did the cluster terminate? Yes, after 15 minutes of inactivity


-- =========================================================
-- PART 7: CLUSTER MONITORING
-- =========================================================

-- Exercise 14: Monitor Cluster Metrics (UI Task)
-- Solution: UI-based observation
-- Example values:
-- - CPU Usage: 25% (varies by workload)
-- - Memory Usage: 40% (varies by workload)
-- - Active Executors: 2 (depends on cluster size)

-- Metrics to monitor:
-- - CPU: High (>80%) indicates need for more compute
-- - Memory: High (>85%) indicates need for memory-optimized nodes
-- - Disk: High indicates need for storage-optimized nodes
-- - Network: High indicates shuffle-heavy operations

-- Exercise 15: Spark UI Exploration (UI Task)
-- Solution: UI-based exploration
-- Example observations:
-- - Jobs tab: Shows all Spark jobs executed
-- - Stages tab: Shows stages within each job
-- - Storage tab: Shows cached DataFrames/tables
-- - Executors tab: Shows executor metrics (CPU, memory, disk)

-- Spark UI is essential for:
-- - Debugging slow queries
-- - Understanding query execution plans
-- - Identifying bottlenecks
-- - Monitoring resource usage


-- =========================================================
-- PART 8: CLUSTER POLICIES (Advanced)
-- =========================================================

-- Exercise 16: Understanding Cluster Policies
SELECT * FROM VALUES
    ('Cost Control', 'Limit instance types and sizes', 'Prevent expensive clusters'),
    ('Security', 'Enforce encryption and access controls', 'Compliance requirements'),
    ('Standardization', 'Consistent configurations across teams', 'Team consistency'),
    ('Resource Management', 'Limit cluster count and size', 'Prevent resource exhaustion')
AS policies(policy_type, description, benefit);

-- Cluster policy examples:
-- 1. Development Policy: Small instances, auto-termination required
-- 2. Production Policy: Specific runtime versions, no auto-termination
-- 3. Cost-Conscious Policy: Spot instances only, max 5 workers
-- 4. Compliance Policy: Encryption required, specific regions only


-- =========================================================
-- PART 9: CLUSTER TYPES COMPARISON
-- =========================================================

-- Exercise 17: All-Purpose vs Job Clusters
SELECT * FROM VALUES
    ('All-Purpose', 'Higher (charged when running)', 'Manual lifecycle', 'Interactive work, development', 'Can be shared'),
    ('Job', 'Lower (only during job)', 'Auto-created and terminated', 'Scheduled jobs, automation', 'Dedicated to job')
AS cluster_comparison(type, cost, lifetime, use_case, sharing);

-- Detailed comparison:

-- All-Purpose Clusters:
-- Cost: Higher - charged for entire runtime (even if idle)
-- Lifetime: Manual - start/stop controlled by user
-- Use Case: Interactive notebooks, ad-hoc queries, development
-- Sharing: Can be shared among users (High Concurrency mode)
-- Startup: Slower (3-5 minutes)
-- Best for: Exploration, development, interactive analytics

-- Job Clusters:
-- Cost: Lower - only charged during job execution
-- Lifetime: Automatic - created for job, terminated after completion
-- Use Case: Scheduled ETL, automated pipelines, production jobs
-- Sharing: Dedicated to single job (not shared)
-- Startup: Slower initially, but can use cluster pools
-- Best for: Production workloads, scheduled jobs, cost optimization

-- Exercise 18: Cluster Selection Decision Tree
-- Solutions:

-- Scenario 1: Interactive data exploration by data scientist
-- Cluster Type: All-Purpose
-- Cluster Mode: Standard
-- Reasoning: Need interactive environment with full Spark API access for exploration and experimentation

-- Scenario 2: Scheduled ETL job running nightly
-- Cluster Type: Job Cluster
-- Cluster Mode: Standard
-- Reasoning: Automated workload, cost-effective to create cluster only when needed, terminates after completion

-- Scenario 3: Multiple analysts running SQL queries
-- Cluster Type: All-Purpose
-- Cluster Mode: High Concurrency
-- Reasoning: Shared environment, process isolation for security, SQL workloads, cost-effective for multiple users

-- Scenario 4: Learning Databricks on Community Edition
-- Cluster Type: All-Purpose (only option)
-- Cluster Mode: Single Node (only option)
-- Reasoning: Community Edition only supports single-node all-purpose clusters, perfect for learning


-- =========================================================
-- PART 10: PRACTICAL EXERCISES
-- =========================================================

-- Exercise 19: Cluster Performance Test
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

-- Expected results:
-- total_rows: 1,000,000
-- unique_ids: 1,000,000
-- avg_value: ~500 (varies due to randomness)

-- Performance metrics (example):
-- Execution time: 2-5 seconds (varies by cluster size)
-- Number of tasks: 8-16 (depends on partitions)
-- Number of stages: 2-3

-- To view detailed metrics:
-- 1. Check Spark UI after query completes
-- 2. Look at Jobs tab for execution time
-- 3. Look at Stages tab for task distribution

-- Exercise 20: Cluster Restart Practice (UI Task)
-- Solution: UI-based practice
-- Example timings:
-- Termination time: 30-60 seconds
-- Startup time: 3-5 minutes
-- Total downtime: 4-6 minutes

-- Best practices:
-- - Save work before terminating
-- - Use cluster pools for faster restarts
-- - Consider keeping cluster running for active work
-- - Use auto-termination to prevent forgotten clusters


-- =========================================================
-- REFLECTION QUESTIONS - ANSWERS
-- =========================================================

%md
## Reflection Answers

1. **What is the difference between all-purpose and job clusters?**
   - All-purpose: Manual lifecycle, interactive work, can be shared, higher cost
   - Job: Auto-created/terminated, scheduled work, dedicated, lower cost

2. **When should you enable autoscaling?**
   - Variable workloads with unpredictable resource needs
   - Cost optimization (scale down during low usage)
   - When you don't know exact resource requirements
   - NOT for: Predictable workloads, streaming (use fixed size)

3. **What are the benefits of auto-termination?**
   - Cost savings (prevents forgotten running clusters)
   - Resource management (frees up capacity)
   - Automatic cleanup (no manual intervention)
   - Best practice for development environments

4. **Which cluster mode should you use for shared analytics?**
   - High Concurrency mode
   - Provides process isolation, table ACLs, and efficient resource sharing

5. **What is the purpose of cluster policies?**
   - Enforce organizational standards
   - Control costs (limit instance types/sizes)
   - Ensure security compliance
   - Standardize configurations across teams

6. **How do you choose the right node type?**
   - General Purpose: Default, balanced workloads
   - Memory Optimized: Large datasets, aggregations
   - Compute Optimized: Complex transformations
   - Storage Optimized: Shuffle-heavy operations
   - GPU: Machine learning, deep learning

7. **What is the difference between Standard and ML runtime?**
   - Standard: Core Spark + Databricks features
   - ML: Standard + pre-installed ML libraries (TensorFlow, PyTorch, scikit-learn, etc.)

8. **Why is LTS runtime recommended for production?**
   - Stability (thoroughly tested)
   - Long-term support (bug fixes, security patches)
   - Predictable behavior (no breaking changes)
   - Recommended by Databricks for production workloads

9. **How does High Concurrency mode provide isolation?**
   - Process-level isolation between users
   - Table access controls (ACLs)
   - Prevents one user from affecting others
   - Secure multi-tenant environment

10. **What happens to data when a cluster terminates?**
    - Persistent data (Delta tables, files in DBFS): Preserved
    - Cluster state (variables, temp views, cached data): Lost
    - Running queries: Cancelled
    - Cluster configuration: Preserved (can restart with same config)


-- =========================================================
-- BONUS CHALLENGES - SOLUTIONS
-- =========================================================

-- Bonus 1: Cost Optimization Strategy
-- Solution:

%md
## Cost Optimization Strategy

**Team Composition:**
- 5 data engineers (interactive work)
- 10 data analysts (SQL queries)
- 3 scheduled ETL jobs

**Recommended Setup:**

1. **Data Engineers (5 users)**
   - 2 Standard mode all-purpose clusters (2-3 engineers per cluster)
   - Autoscaling: 1-4 workers
   - Auto-termination: 30 minutes
   - Runtime: Latest LTS
   - Node type: General purpose (m5.xlarge)
   - Estimated cost: $500-800/month

2. **Data Analysts (10 users)**
   - 1 High Concurrency all-purpose cluster (shared)
   - Autoscaling: 2-8 workers
   - Auto-termination: 60 minutes
   - Runtime: Latest LTS
   - Node type: General purpose (m5.xlarge)
   - Estimated cost: $600-1000/month

3. **ETL Jobs (3 jobs)**
   - 3 Job clusters (one per job)
   - Fixed size: 2-4 workers (based on job needs)
   - Runtime: Latest LTS
   - Node type: Compute optimized (c5.xlarge)
   - Use spot instances (50% savings)
   - Estimated cost: $200-400/month

**Total Estimated Cost: $1,300-2,200/month**

**Additional Optimizations:**
- Use cluster pools for faster job startup
- Implement cluster policies to prevent over-provisioning
- Monitor usage and adjust configurations monthly
- Consider reserved instances for predictable workloads (40% savings)


-- Bonus 2: Cluster Configuration Template
-- Solution:

%md
## Optimal Cluster Configuration

### Development Cluster
```json
{
  "cluster_name": "dev-cluster",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "i3.xlarge",
  "driver_node_type_id": "i3.xlarge",
  "autoscale": {
    "min_workers": 1,
    "max_workers": 3
  },
  "autotermination_minutes": 30,
  "spark_conf": {
    "spark.databricks.delta.preview.enabled": "true"
  },
  "custom_tags": {
    "Environment": "Development",
    "Team": "Data Engineering"
  }
}
```

### Production Cluster (for Jobs)
```json
{
  "cluster_name": "prod-etl-cluster",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "c5.2xlarge",
  "driver_node_type_id": "c5.2xlarge",
  "num_workers": 4,
  "autotermination_minutes": 0,
  "spark_conf": {
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true"
  },
  "aws_attributes": {
    "availability": "SPOT_WITH_FALLBACK",
    "spot_bid_price_percent": 100
  },
  "custom_tags": {
    "Environment": "Production",
    "CostCenter": "DataPlatform"
  }
}
```

### Shared Analytics Cluster
```json
{
  "cluster_name": "analytics-shared",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "m5.xlarge",
  "driver_node_type_id": "m5.xlarge",
  "autoscale": {
    "min_workers": 2,
    "max_workers": 8
  },
  "autotermination_minutes": 60,
  "cluster_mode": "HIGH_CONCURRENCY",
  "spark_conf": {
    "spark.databricks.repl.allowedLanguages": "sql,python"
  },
  "custom_tags": {
    "Environment": "Shared",
    "Team": "Analytics"
  }
}
```


-- Bonus 3: Troubleshooting Scenarios
-- Solutions:

%md
## Troubleshooting Guide

### Problem 1: Cluster takes 10 minutes to start
**Solutions:**
- Use cluster pools (pre-warmed instances, 30-60 second startup)
- Choose smaller instance types (faster provisioning)
- Check cloud provider capacity in your region
- Use existing cluster instead of creating new one
- Consider different availability zone

### Problem 2: Queries are running slowly
**Solutions:**
- Check Spark UI for bottlenecks (skew, shuffles)
- Increase cluster size or enable autoscaling
- Optimize queries (partitioning, caching, broadcast joins)
- Use appropriate node type (memory/compute optimized)
- Check for data skew and repartition if needed
- Enable Photon engine (if available)

### Problem 3: Cluster costs are too high
**Solutions:**
- Use job clusters instead of all-purpose for scheduled work
- Enable aggressive auto-termination (15-30 minutes)
- Enable autoscaling to scale down during low usage
- Use spot/preemptible instances (50-70% savings)
- Right-size node types (don't over-provision)
- Implement cluster policies to prevent expensive configurations
- Use cluster pools to reduce idle time

### Problem 4: Out of memory errors
**Solutions:**
- Use memory-optimized node types (r5 family)
- Increase cluster size (more workers = more memory)
- Optimize queries to reduce memory usage:
  - Use incremental processing instead of loading all data
  - Avoid collect() on large datasets
  - Use persist() strategically
  - Increase spark.sql.shuffle.partitions
- Repartition data to distribute load evenly
- Use Delta Lake optimization (OPTIMIZE, Z-ORDER)

### Problem 5: Cluster terminates unexpectedly
**Solutions:**
- Check auto-termination settings (may be too aggressive)
- Check for cloud provider spot instance interruptions
- Review cluster event log for termination reason
- Check for workspace or account-level policies
- Verify cloud provider quotas and limits
- Check for cost limits or budget alerts
- Review cluster logs for errors before termination


-- =========================================================
-- KEY LEARNINGS SUMMARY
-- =========================================================

%md
## Day 2 Key Learnings

### Cluster Types
1. **All-Purpose**: Interactive work, manual lifecycle, can be shared
2. **Job**: Automated work, auto-created/terminated, cost-effective

### Cluster Modes
1. **Standard**: Full Spark API, single/multiple users
2. **High Concurrency**: Shared, process isolation, SQL/Python only
3. **Single Node**: No workers, development/learning

### Runtime Versions
1. **Standard**: Core Spark + Databricks features
2. **ML**: Standard + ML libraries
3. **Photon**: Optimized query engine
4. **Light**: Minimal libraries, faster startup
5. **LTS**: Recommended for production (stability)

### Cost Optimization
1. Use job clusters for scheduled workloads
2. Enable autoscaling (scale down during low usage)
3. Set aggressive auto-termination (15-30 min for dev)
4. Use spot/preemptible instances (50-70% savings)
5. Right-size node types (don't over-provision)
6. Use cluster pools for faster startup

### Best Practices
1. LTS runtime for production, latest for development
2. High Concurrency mode for shared analytics
3. Standard mode for data engineering
4. Job clusters for production ETL
5. Monitor cluster metrics regularly
6. Implement cluster policies for governance
7. Use appropriate node types for workload

**Next**: Day 3 - Delta Lake Fundamentals
