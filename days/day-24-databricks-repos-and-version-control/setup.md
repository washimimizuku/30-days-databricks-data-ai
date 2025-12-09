# Day 24 Setup: Databricks Repos & Version Control

## Overview

This setup guides you through configuring Git integration with Databricks and creating a sample repository for practice.

## Prerequisites

- Databricks workspace access
- Git account (GitHub, GitLab, Bitbucket, or Azure DevOps)
- Basic Git knowledge

## Step 1: Create a Git Repository

### Option A: GitHub

1. Go to https://github.com
2. Click "New repository"
3. Name: `databricks-pipeline-demo`
4. Description: "Practice repository for Databricks Repos"
5. Select "Public" or "Private"
6. Check "Add a README file"
7. Add `.gitignore`: Python
8. Click "Create repository"

### Option B: GitLab

1. Go to https://gitlab.com
2. Click "New project"
3. Select "Create blank project"
4. Name: `databricks-pipeline-demo`
5. Set visibility level
6. Check "Initialize repository with a README"
7. Click "Create project"

## Step 2: Generate Personal Access Token

### GitHub PAT

```
1. Go to Settings → Developer settings
2. Click "Personal access tokens" → "Tokens (classic)"
3. Click "Generate new token (classic)"
4. Name: "Databricks Integration"
5. Select scopes:
   ✓ repo (full control)
   ✓ read:org
6. Click "Generate token"
7. Copy token (save it securely!)
```

### GitLab PAT

```
1. Go to User Settings → Access Tokens
2. Name: "Databricks Integration"
3. Select scopes:
   ✓ api
   ✓ read_repository
   ✓ write_repository
4. Click "Create personal access token"
5. Copy token
```

### Bitbucket App Password

```
1. Go to Personal settings → App passwords
2. Click "Create app password"
3. Label: "Databricks Integration"
4. Permissions:
   ✓ Repositories: Read, Write
5. Click "Create"
6. Copy password
```

## Step 3: Configure Databricks Git Integration

```
1. In Databricks workspace, click your username (top right)
2. Select "User Settings"
3. Go to "Git Integration" tab
4. Click "Add Git provider"
5. Select your provider (GitHub/GitLab/Bitbucket)
6. Enter:
   - Git provider username
   - Personal access token
7. Click "Save"
```

## Step 4: Create Sample Repository Structure

Clone your repository locally and create this structure:

```bash
# Clone repository
git clone https://github.com/your-username/databricks-pipeline-demo.git
cd databricks-pipeline-demo

# Create directory structure
mkdir -p notebooks/bronze
mkdir -p notebooks/silver
mkdir -p notebooks/gold
mkdir -p src/utils
mkdir -p src/config
mkdir -p tests
mkdir -p jobs

# Create files
touch notebooks/bronze/ingest_data.py
touch notebooks/silver/clean_data.py
touch notebooks/gold/aggregate_data.py
touch src/utils/data_quality.py
touch src/config/settings.py
touch tests/test_pipeline.py
touch jobs/daily_pipeline.json
```

## Step 5: Add Sample Files

### .gitignore

```gitignore
# Databricks
.databricks/
*.pyc
__pycache__/
_checkpoints/
*.checkpoint

# Python
*.py[cod]
*$py.class
.Python
venv/
.venv/
ENV/

# IDE
.idea/
.vscode/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Logs
*.log

# Environment
.env
```

### README.md

```markdown
# Databricks Pipeline Demo

Sample data pipeline using Databricks and Git version control.

## Structure

- `notebooks/` - Databricks notebooks
  - `bronze/` - Raw data ingestion
  - `silver/` - Data cleaning and transformation
  - `gold/` - Business aggregations
- `src/` - Python modules
  - `utils/` - Utility functions
  - `config/` - Configuration management
- `tests/` - Unit tests
- `jobs/` - Job configurations

## Setup

1. Clone this repository into Databricks Repos
2. Configure environment variables
3. Run setup notebook
4. Execute pipeline

## Development

1. Create feature branch
2. Make changes
3. Test locally
4. Create pull request
5. Merge after review
```

### notebooks/bronze/ingest_data.py

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Data Ingestion
# MAGIC 
# MAGIC Ingests raw data from source systems

# COMMAND ----------

# Parameters
dbutils.widgets.text("date", "")
dbutils.widgets.text("source_path", "/mnt/landing/")

date_param = dbutils.widgets.get("date")
source_path = dbutils.widgets.get("source_path")

# COMMAND ----------

from pyspark.sql.functions import *

# Read source data
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{source_path}*.csv") \
    .withColumn("ingestion_timestamp", current_timestamp())

# Write to Bronze
df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("date") \
    .saveAsTable("bronze.raw_data")

print(f"✅ Ingested {df.count()} records")
```

### src/utils/data_quality.py

```python
"""Data quality utilities"""

def check_nulls(df, columns):
    """Check for null values in specified columns"""
    null_counts = {}
    for col in columns:
        null_count = df.filter(df[col].isNull()).count()
        null_counts[col] = null_count
    return null_counts

def check_duplicates(df, key_columns):
    """Check for duplicate records"""
    total_count = df.count()
    distinct_count = df.select(key_columns).distinct().count()
    duplicate_count = total_count - distinct_count
    return duplicate_count

def validate_schema(df, expected_schema):
    """Validate DataFrame schema matches expected"""
    actual_columns = set(df.columns)
    expected_columns = set(expected_schema.keys())
    
    missing = expected_columns - actual_columns
    extra = actual_columns - expected_columns
    
    return {
        "valid": len(missing) == 0 and len(extra) == 0,
        "missing_columns": list(missing),
        "extra_columns": list(extra)
    }
```

### src/config/settings.py

```python
"""Configuration management"""

class Config:
    """Environment-specific configuration"""
    
    def __init__(self, environment="dev"):
        self.env = environment
        self.configs = {
            "dev": {
                "database": "dev_db",
                "data_path": "/mnt/dev/data",
                "checkpoint_path": "/mnt/dev/checkpoints"
            },
            "staging": {
                "database": "staging_db",
                "data_path": "/mnt/staging/data",
                "checkpoint_path": "/mnt/staging/checkpoints"
            },
            "prod": {
                "database": "prod_db",
                "data_path": "/mnt/prod/data",
                "checkpoint_path": "/mnt/prod/checkpoints"
            }
        }
    
    def get(self, key):
        """Get configuration value"""
        return self.configs[self.env].get(key)
    
    def get_database(self):
        return self.get("database")
    
    def get_data_path(self):
        return self.get("data_path")
    
    def get_checkpoint_path(self):
        return self.get("checkpoint_path")
```

### tests/test_pipeline.py

```python
"""Unit tests for pipeline"""

import pytest
from src.utils.data_quality import check_nulls, check_duplicates

def test_check_nulls():
    """Test null checking function"""
    # Mock DataFrame would go here
    pass

def test_check_duplicates():
    """Test duplicate checking function"""
    # Mock DataFrame would go here
    pass

def test_validate_schema():
    """Test schema validation"""
    # Mock DataFrame would go here
    pass
```

## Step 6: Commit and Push

```bash
# Add all files
git add .

# Commit
git commit -m "Initial project structure"

# Push to remote
git push origin main
```

## Step 7: Clone Repository in Databricks

### Via UI

```
1. In Databricks, click "Repos" in left sidebar
2. Click "Add Repo"
3. Enter repository URL:
   https://github.com/your-username/databricks-pipeline-demo
4. Select branch: main
5. Click "Create Repo"
```

### Via API

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

repo = w.repos.create(
    url="https://github.com/your-username/databricks-pipeline-demo",
    provider="github",
    path="/Repos/your-username/databricks-pipeline-demo"
)

print(f"✅ Repository cloned to: {repo.path}")
```

## Step 8: Create Development Branch

```
1. In Databricks Repos, open your repository
2. Click branch dropdown (shows "main")
3. Type "develop"
4. Click "Create branch: develop"
```

## Step 9: Verify Setup

```python
# Run this in a Databricks notebook

# Check current repo path
import os
current_path = os.getcwd()
print(f"Current path: {current_path}")

# List files
files = dbutils.fs.ls(f"file:{current_path}")
for file in files:
    print(f"  {file.name}")

# Import custom module
from src.config.settings import Config

config = Config("dev")
print(f"Database: {config.get_database()}")
print(f"Data path: {config.get_data_path()}")

print("\n✅ Setup complete!")
```

## Step 10: Create Feature Branch for Exercises

```
1. In Databricks Repos, click branch dropdown
2. Type "feature/day24-exercises"
3. Click "Create branch"
4. You're now on the feature branch
```

## Verification Checklist

- [ ] Git repository created
- [ ] Personal access token generated
- [ ] Databricks Git integration configured
- [ ] Repository cloned in Databricks
- [ ] Can see all files in Repos
- [ ] Can import custom modules
- [ ] Can create and switch branches
- [ ] Can commit changes

## Troubleshooting

### Can't Clone Repository

**Issue**: Authentication failed  
**Solution**: 
- Verify PAT is correct
- Check PAT has required scopes
- Re-enter credentials in Git Integration

### Can't Import Modules

**Issue**: ModuleNotFoundError  
**Solution**:
- Ensure you're in the repo directory
- Check file paths are correct
- Restart Python kernel

### Branch Not Showing

**Issue**: New branch not visible  
**Solution**:
- Refresh the page
- Pull latest changes
- Check branch was pushed to remote

## Next Steps

1. Complete the exercises in `exercise.py`
2. Practice creating branches
3. Make commits and push changes
4. Create a pull request on GitHub/GitLab
5. Take the quiz

---

**Note**: Keep your Personal Access Token secure and never commit it to Git!
