# Quick Start Guide

Get started with the 30 Days of Databricks Data Engineer Associate bootcamp in 5 minutes.

## Prerequisites

- Basic SQL knowledge
- Basic Python knowledge
- Understanding of data engineering concepts
- Computer with internet connection

## Setup Steps

### 1. Create Databricks Account (5 minutes)

**Option A: Community Edition (Free, Limited)**
1. Go to [Databricks Community Edition](https://community.cloud.databricks.com/)
2. Click "Sign Up"
3. Fill in your details
4. Verify your email
5. Log in to your workspace

**Option B: Trial Account (14 days, Full Features)**
1. Go to [Databricks Trial](https://databricks.com/try-databricks)
2. Choose your cloud provider (AWS, Azure, or GCP)
3. Fill in your details
4. Verify your email
5. Complete cloud provider setup
6. Log in to your workspace

### 2. Create Your First Cluster (3 minutes)

1. In Databricks workspace, click "Compute" in the left sidebar
2. Click "Create Cluster"
3. Configure:
   - **Cluster name**: `learning-cluster`
   - **Cluster mode**: Single Node
   - **Databricks Runtime**: Latest LTS version
   - **Node type**: Smallest available (e.g., i3.xlarge on AWS)
   - **Terminate after**: 30 minutes of inactivity
4. Click "Create Cluster"
5. Wait 3-5 minutes for cluster to start

### 3. Create Your First Notebook (2 minutes)

1. Click "Workspace" in the left sidebar
2. Navigate to your user folder
3. Click the dropdown arrow → "Create" → "Notebook"
4. Configure:
   - **Name**: `Day 1 - Getting Started`
   - **Language**: SQL or Python
   - **Cluster**: Select your `learning-cluster`
5. Click "Create"

### 4. Run Your First Query (1 minute)

In your notebook, type and run:

```sql
SELECT "Hello Databricks!" as greeting
```

Or in Python:

```python
print("Hello Databricks!")
spark.sql("SELECT 'Hello Databricks!' as greeting").show()
```

Click the "Run" button or press `Shift + Enter`.

## Daily Workflow

### Morning Routine (5 minutes)
1. Log in to Databricks
2. Start your cluster (if not running)
3. Open today's folder in this repo
4. Read the `README.md` for today's topic

### Study Session (1.5-2 hours)
1. Read the `setup.md` for environment setup
2. Follow along with examples in `README.md`
3. Complete the `exercise.sql` or `exercise.py`
4. Check your work against `solution.sql` or `solution.py`
5. Take the `quiz.md` to test your knowledge

### Evening Routine (5 minutes)
1. Review what you learned
2. Update your progress tracker
3. Stop your cluster to save costs
4. Preview tomorrow's topic

## Folder Structure

```
30-days-databricks-data-ai/
├── days/
│   ├── day-01-databricks-architecture/
│   │   ├── README.md          # Topic overview and learning objectives
│   │   ├── setup.md           # Environment setup instructions
│   │   ├── exercise.sql       # Hands-on exercises
│   │   ├── solution.sql       # Exercise solutions
│   │   └── quiz.md            # Knowledge check questions
│   ├── day-02-clusters-compute/
│   └── ...
├── data/                      # Sample datasets
├── docs/                      # Additional documentation
├── tools/                     # Helper scripts
├── README.md                  # Main bootcamp guide
├── CURRICULUM.md              # Detailed curriculum
└── QUICKSTART.md             # This file
```

## Tips for Success

1. **Consistency is key**: Study 1.5-2 hours every day
2. **Hands-on practice**: Don't just read, actually run the code
3. **Take notes**: Keep a learning journal
4. **Join community**: Databricks Community forums for help
5. **Manage costs**: Stop clusters when not in use

## Common Issues

### Cluster won't start
- Check your cloud credits/trial status
- Try a smaller node type
- Wait a few minutes and retry

### Can't find sample data
- Use Databricks sample datasets: `/databricks-datasets/`
- Check the `data/` folder in this repo
- Follow setup instructions in each day's `setup.md`

### Code doesn't run
- Ensure cluster is running
- Check you're using the correct language (SQL vs Python)
- Verify you've completed the setup steps

## Getting Help

- **Databricks Documentation**: [docs.databricks.com](https://docs.databricks.com)
- **Community Forums**: [community.databricks.com](https://community.databricks.com)
- **Stack Overflow**: Tag questions with `databricks`
- **This Repo**: Open an issue for bootcamp-specific questions

## Ready to Start?

Head to [Day 1: Databricks Architecture & Workspace](days/day-01-databricks-architecture/README.md) and begin your journey!

Good luck! 🚀
