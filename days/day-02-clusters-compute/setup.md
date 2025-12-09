# Day 2 Setup: Creating Your First Cluster

## Prerequisites

- Completed Day 1 setup
- Active Databricks account
- Access to Databricks workspace

## Setup Steps

### Step 1: Navigate to Compute

1. Log in to your Databricks workspace
2. Click **Compute** in the left sidebar
3. You'll see the cluster management page

### Step 2: Create All-Purpose Cluster

1. Click **Create Cluster** button
2. Configure the cluster:

**Basic Settings**:
- **Cluster name**: `learning-cluster`
- **Cluster mode**: Single Node (for Community Edition) or Standard
- **Databricks Runtime Version**: Select latest LTS (e.g., 13.3 LTS)

**Advanced Settings** (if available):
- **Autopilot Options**:
  - Enable autoscaling: Yes (if not Single Node)
  - Min workers: 1
  - Max workers: 2
- **Auto Termination**: 30 minutes of inactivity

**Node Configuration**:
- **Worker type**: Smallest available (e.g., i3.xlarge on AWS, Standard_DS3_v2 on Azure)
- **Driver type**: Same as worker

3. Click **Create Cluster**
4. Wait 3-5 minutes for cluster to start (status will change to "Running")

### Step 3: Verify Cluster is Running

1. Check cluster status shows green "Running" indicator
2. Note the cluster ID and Spark version
3. Click on cluster name to see details

### Step 4: Attach Notebook to Cluster

1. Navigate to **Workspace** → Your user folder
2. Open the notebook you created on Day 1
3. At the top of the notebook, click the dropdown that says "Detached"
4. Select your `learning-cluster`
5. Wait for connection (status will show "Connected")

### Step 5: Test Cluster Connection

Run this in a notebook cell:

```sql
SELECT 
    current_version() as spark_version,
    current_database() as current_db,
    current_user() as user_name;
```

Or in Python:

```python
print(f"Spark version: {spark.version}")
print(f"Databricks Runtime: {spark.conf.get('spark.databricks.clusterUsageTags.sparkVersion')}")
print("Cluster is working!")
```

### Step 6: Explore Cluster UI

1. Click on your cluster name in Compute page
2. Explore the tabs:
   - **Configuration**: View/edit cluster settings
   - **Event Log**: See cluster lifecycle events
   - **Spark UI**: Access Spark web UI (when running jobs)
   - **Driver Logs**: View driver logs
   - **Metrics**: CPU, memory usage (if available)

### Step 7: Practice Starting and Stopping

1. Click **Terminate** to stop the cluster
2. Wait for status to change to "Terminated"
3. Click **Start** to restart the cluster
4. Note the startup time

## Verification Checklist

- [ ] Successfully created a cluster
- [ ] Cluster is in "Running" state
- [ ] Attached notebook to cluster
- [ ] Ran test query successfully
- [ ] Explored cluster configuration
- [ ] Practiced starting and stopping cluster

## Troubleshooting

### Cluster won't start
- **Check cloud credits**: Ensure you have available credits/trial
- **Try smaller instance**: Select the smallest available node type
- **Check quotas**: You may have reached instance limits
- **Wait and retry**: Sometimes cloud providers have temporary capacity issues

### Can't attach notebook to cluster
- **Refresh page**: Try refreshing the browser
- **Check cluster status**: Ensure cluster is "Running"
- **Permissions**: Verify you have access to the cluster

### Cluster starts then terminates
- **Check logs**: View Event Log for error messages
- **Instance type**: Try a different node type
- **Region**: May be capacity issues in your region

## Cost Management Tips

**For Community Edition**:
- Single node only, limited resources
- Free tier, no cost concerns
- Auto-terminates after inactivity

**For Trial/Paid Accounts**:
- Always set auto-termination (30 min recommended)
- Stop clusters when not in use
- Use smallest instance types for learning
- Monitor usage in Account Console
- Consider using spot/preemptible instances

## Cluster Configuration Examples

### Development Cluster (Small)
```
Name: dev-cluster
Mode: Single Node
Runtime: 13.3 LTS
Node: i3.xlarge (AWS) or Standard_DS3_v2 (Azure)
Auto-terminate: 30 minutes
```

### Learning Cluster (Minimal)
```
Name: learning-cluster
Mode: Single Node
Runtime: Latest LTS
Node: Smallest available
Auto-terminate: 15 minutes
```

### Shared Analytics Cluster (if available)
```
Name: analytics-cluster
Mode: High Concurrency
Runtime: 13.3 LTS
Workers: 1-3 (autoscaling)
Auto-terminate: 60 minutes
```

## What's Next?

You're now ready to run all the Day 2 exercises! Your cluster will be used for all remaining days of the bootcamp.

**Remember**: Always stop your cluster when done to save costs (for paid accounts).
