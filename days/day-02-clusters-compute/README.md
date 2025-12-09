# Day 2: Clusters & Compute

## Learning Objectives

By the end of today, you will:
- Understand different cluster types and their use cases
- Create and configure clusters
- Know cluster modes (Standard, High Concurrency, Single Node)
- Configure autoscaling and auto-termination
- Understand Databricks Runtime versions
- Monitor cluster performance and costs

## Topics Covered

### 1. Cluster Types

**All-Purpose Clusters**:
- Interactive workloads
- Notebook development and exploration
- Ad-hoc queries
- Can be shared among users (High Concurrency mode)
- More expensive but flexible

**Job Clusters**:
- Automated workloads
- Created for specific jobs
- Terminated after job completion
- More cost-effective for production
- Cannot be reused for interactive work

**SQL Warehouses** (formerly SQL Endpoints):
- Optimized for SQL queries
- BI tool integration
- Serverless option available
- Automatic scaling

### 2. Cluster Modes

**Standard Mode**:
- Single user or multiple users
- Supports Scala, Python, SQL, R
- Full Spark API access
- Best for data engineering

**High Concurrency Mode**:
- Multiple users simultaneously
- Process isolation for security
- Table access control
- Limited to SQL and Python
- Best for shared analytics

**Single Node**:
- No workers, only driver
- Lightweight workloads
- Learning and development
- Community Edition only supports this

### 3. Cluster Configuration

**Key Settings**:
- **Cluster Name**: Descriptive identifier
- **Cluster Mode**: Standard, High Concurrency, or Single Node
- **Databricks Runtime**: Version of Spark and libraries
- **Node Type**: Instance type (CPU, memory, storage)
- **Workers**: Number of worker nodes (min/max for autoscaling)
- **Autoscaling**: Dynamic worker allocation
- **Auto Termination**: Automatic shutdown after inactivity

**Node Types**:
- **General Purpose**: Balanced CPU and memory
- **Memory Optimized**: High memory for large datasets
- **Compute Optimized**: High CPU for processing
- **Storage Optimized**: High I/O for data-intensive tasks
- **GPU**: For machine learning workloads

### 4. Databricks Runtime

**Runtime Types**:
- **Standard**: Core Spark and Databricks features
- **ML**: Pre-installed ML libraries (TensorFlow, PyTorch, etc.)
- **Photon**: Optimized query engine (Premium/Enterprise)
- **Light**: Minimal libraries for jobs

**Version Selection**:
- **LTS (Long Term Support)**: Stable, recommended for production
- **Latest**: Newest features, may have bugs
- **Legacy**: Older versions for compatibility

**Example Versions**:
- 13.3 LTS (Spark 3.4.1)
- 14.0 (Spark 3.5.0)
- 13.3 LTS ML (includes ML libraries)

### 5. Autoscaling

**Benefits**:
- Cost optimization
- Automatic resource adjustment
- Handle variable workloads

**Configuration**:
- **Min Workers**: Minimum number of workers
- **Max Workers**: Maximum number of workers
- **Scale Down**: Remove workers when idle
- **Scale Up**: Add workers when needed

**Best Practices**:
- Set min workers to 1-2 for consistent performance
- Set max workers based on budget and workload
- Use autoscaling for variable workloads
- Disable for predictable workloads

### 6. Auto Termination

**Purpose**:
- Reduce costs by stopping idle clusters
- Prevent forgotten clusters from running

**Configuration**:
- Set inactivity period (e.g., 30 minutes)
- Cluster automatically terminates after period
- Can be restarted manually

**Recommendations**:
- Development: 30-60 minutes
- Production: Disable (use job clusters instead)
- Learning: 15-30 minutes

### 7. Cluster Policies

**Purpose**:
- Enforce organizational standards
- Control costs
- Ensure security compliance

**Policy Controls**:
- Allowed instance types
- Maximum cluster size
- Required tags
- Auto-termination settings
- Runtime versions

### 8. Monitoring and Costs

**Monitoring**:
- Spark UI: Job execution details
- Metrics: CPU, memory, disk usage
- Event Log: Cluster lifecycle events
- Ganglia: System metrics (if enabled)

**Cost Optimization**:
- Use job clusters for production
- Enable autoscaling
- Set auto-termination
- Use spot/preemptible instances
- Right-size node types

## Hands-On Exercises

See `exercise.sql` for practical exercises.

## Key Takeaways

1. All-purpose clusters for interactive work, job clusters for automation
2. Standard mode for full Spark access, High Concurrency for shared use
3. Autoscaling optimizes costs and performance
4. Auto-termination prevents wasted resources
5. Choose LTS runtime for stability
6. Monitor cluster usage to optimize costs

## Exam Tips

- Know the difference between cluster types and when to use each
- Understand cluster modes and their limitations
- Be familiar with autoscaling configuration
- Know how to optimize cluster costs
- Understand Databricks Runtime versions (Standard, ML, Photon)

## Common Exam Questions

1. When should you use a job cluster vs. all-purpose cluster?
2. What are the limitations of High Concurrency mode?
3. How does autoscaling work?
4. What is the difference between Standard and ML runtime?
5. How do you optimize cluster costs?

## Additional Resources

- [Databricks Clusters Documentation](https://docs.databricks.com/clusters/index.html)
- [Cluster Configuration Best Practices](https://docs.databricks.com/clusters/cluster-config-best-practices.html)
- [Databricks Runtime Release Notes](https://docs.databricks.com/release-notes/runtime/index.html)

## Next Steps

Tomorrow: [Day 3 - Delta Lake Fundamentals](../day-03-delta-lake-fundamentals/README.md)
