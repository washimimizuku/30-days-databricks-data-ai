# Day 24: Databricks Repos & Version Control

## Overview

Today you'll learn how to integrate Git version control with Databricks using Databricks Repos. This enables collaborative development, version tracking, and CI/CD workflows for your data pipelines.

**Time**: 1.5 hours  
**Focus**: Git integration, branching strategies, collaborative development

## Learning Objectives

By the end of today, you will be able to:
- Connect Databricks to Git repositories (GitHub, GitLab, Bitbucket, Azure DevOps)
- Clone repositories into Databricks Repos
- Create and switch between branches
- Commit and push changes from Databricks
- Pull updates from remote repositories
- Resolve merge conflicts
- Implement branching strategies for teams
- Set up CI/CD workflows with Repos

## What are Databricks Repos?

Databricks Repos is a visual Git client integrated into the Databricks workspace. It allows you to:
- Sync notebooks and code with Git repositories
- Collaborate with team members using Git workflows
- Version control your data pipelines
- Implement CI/CD for production deployments

### Key Benefits

**Version Control**: Track all changes to notebooks and code  
**Collaboration**: Multiple developers working on same codebase  
**Code Review**: Use pull requests for quality control  
**CI/CD**: Automate testing and deployment  
**Rollback**: Easily revert to previous versions  

## Supported Git Providers

Databricks Repos supports:
- **GitHub** (github.com and GitHub Enterprise)
- **GitLab** (gitlab.com and self-hosted)
- **Bitbucket** (Cloud and Server)
- **Azure DevOps** (Azure Repos)
- **AWS CodeCommit**

## Setting Up Databricks Repos

### Step 1: Configure Git Integration

#### Personal Access Token (PAT)
```python
# Generate PAT from your Git provider:
# GitHub: Settings → Developer settings → Personal access tokens
# GitLab: User Settings → Access Tokens
# Bitbucket: Personal settings → App passwords

# Required scopes:
# - repo (full control of private repositories)
# - read:org (for organization repos)
```

#### Add Git Credentials in Databricks
```
1. Click User Settings (top right)
2. Go to "Git Integration" tab
3. Select Git provider
4. Enter username and PAT
5. Save
```

### Step 2: Clone a Repository

#### Via UI
```
1. Click "Repos" in left sidebar
2. Click "Add Repo"
3. Enter Git repository URL
4. Select branch (default: main/master)
5. Click "Create Repo"
```

#### Via API
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

repo = w.repos.create(
    url="https://github.com/your-org/your-repo",
    provider="github",
    path="/Repos/your-username/your-repo"
)

print(f"Created repo at: {repo.path}")
```

## Working with Repos

### Repository Structure
```
/Repos/
├── username/
│   ├── repo1/
│   │   ├── notebooks/
│   │   │   ├── etl_pipeline.py
│   │   │   └── data_quality.py
│   │   ├── src/
│   │   │   ├── utils.py
│   │   │   └── config.py
│   │   ├── tests/
│   │   │   └── test_pipeline.py
│   │   ├── .gitignore
│   │   └── README.md
│   └── repo2/
└── team/
    └── shared-repo/
```

### Viewing Repository Status

```python
# Check current branch
current_branch = dbutils.notebook.entry_point.getDbutils() \
    .notebook().getContext().notebookPath().get()

print(f"Current path: {current_branch}")

# View Git status (via UI)
# Click on repo name → Shows current branch and uncommitted changes
```

### Creating a Branch

#### Via UI
```
1. Open your repo
2. Click branch dropdown (shows current branch)
3. Type new branch name
4. Click "Create branch"
```

#### Via API
```python
# Create and checkout new branch
w.repos.update(
    repo_id=repo.id,
    branch="feature/new-pipeline"
)
```

### Switching Branches

#### Via UI
```
1. Click branch dropdown
2. Select branch from list
3. Confirm switch
```

#### Via API
```python
# Switch to existing branch
w.repos.update(
    repo_id=repo.id,
    branch="develop"
)
```

### Committing Changes

#### Via UI
```
1. Make changes to notebooks/files
2. Click repo name → "Git" tab
3. Review changed files
4. Enter commit message
5. Click "Commit & Push"
```

#### Best Practices for Commit Messages
```
# Good commit messages:
✅ "Add customer segmentation notebook"
✅ "Fix null handling in data cleaning pipeline"
✅ "Update bronze layer ingestion logic"

# Bad commit messages:
❌ "Update"
❌ "Fix bug"
❌ "Changes"
```

### Pulling Changes

#### Via UI
```
1. Click repo name → "Git" tab
2. Click "Pull" button
3. Review incoming changes
4. Confirm pull
```

#### Handling Conflicts
```
1. Pull shows conflicts
2. Open conflicted files
3. Resolve conflicts manually
4. Mark as resolved
5. Commit merge
```

### Pushing Changes

```
1. Commit changes locally
2. Click "Push" button
3. Changes pushed to remote
```

## Branching Strategies

### 1. Git Flow (Recommended for Teams)

```
main (production)
  ↑
develop (integration)
  ↑
feature/* (new features)
hotfix/* (urgent fixes)
release/* (release preparation)
```

**Workflow**:
```
1. Create feature branch from develop
2. Develop and test feature
3. Create pull request to develop
4. Code review and merge
5. Release branch from develop
6. Merge to main for production
```

### 2. GitHub Flow (Simpler)

```
main (production)
  ↑
feature/* (all changes)
```

**Workflow**:
```
1. Create feature branch from main
2. Develop and test
3. Create pull request
4. Code review and merge to main
5. Deploy from main
```

### 3. Trunk-Based Development

```
main (always deployable)
  ↑
short-lived feature branches
```

**Workflow**:
```
1. Create short-lived branch
2. Small, frequent commits
3. Quick review and merge
4. Continuous deployment
```

## Collaborative Development

### Pull Request Workflow

```python
# 1. Create feature branch
git checkout -b feature/new-pipeline

# 2. Make changes in Databricks
# Edit notebooks, add new files

# 3. Commit and push
# Via Databricks UI: Commit & Push

# 4. Create pull request
# On GitHub/GitLab: Create PR from feature to develop

# 5. Code review
# Team reviews changes, adds comments

# 6. Address feedback
# Make changes, push updates

# 7. Merge
# Merge PR after approval
```

### Code Review Best Practices

**For Authors**:
- Keep PRs small and focused
- Write clear descriptions
- Add tests for new features
- Update documentation

**For Reviewers**:
- Review promptly
- Be constructive
- Check for best practices
- Test the changes

## CI/CD with Databricks Repos

### Basic CI/CD Pipeline

```yaml
# .github/workflows/databricks-ci.yml
name: Databricks CI/CD

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main, develop]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      
      - name: Install dependencies
        run: |
          pip install pytest databricks-cli
      
      - name: Run tests
        run: pytest tests/
      
      - name: Deploy to Databricks
        if: github.ref == 'refs/heads/main'
        run: |
          databricks repos update \
            --path /Repos/production/pipeline \
            --branch main
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
```

### Deployment Strategy

```
Development Environment:
  - Branch: develop
  - Repo: /Repos/dev/pipeline
  - Auto-deploy on commit

Staging Environment:
  - Branch: release/*
  - Repo: /Repos/staging/pipeline
  - Deploy on PR merge

Production Environment:
  - Branch: main
  - Repo: /Repos/production/pipeline
  - Deploy after approval
```

## Best Practices

### 1. Repository Organization

```
repo/
├── notebooks/          # Databricks notebooks
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── src/               # Python modules
│   ├── utils/
│   └── config/
├── tests/             # Unit tests
├── jobs/              # Job configurations
├── .gitignore         # Ignore patterns
├── requirements.txt   # Dependencies
└── README.md          # Documentation
```

### 2. .gitignore for Databricks

```gitignore
# Databricks
.databricks/
*.pyc
__pycache__/

# Checkpoints
_checkpoints/
*.checkpoint

# Logs
*.log

# Environment
.env
.venv/

# IDE
.idea/
.vscode/

# OS
.DS_Store
Thumbs.db
```

### 3. Notebook Best Practices

```python
# Use relative imports
from src.utils import data_quality
from src.config import settings

# Parameterize notebooks
dbutils.widgets.text("environment", "dev")
env = dbutils.widgets.get("environment")

# Avoid hardcoded paths
# ❌ Bad
path = "/mnt/production/data"

# ✅ Good
path = settings.get_data_path(env)
```

### 4. Testing Strategy

```python
# tests/test_pipeline.py
import pytest
from src.pipeline import transform_data

def test_transform_data():
    """Test data transformation logic"""
    input_data = [{"id": 1, "value": 100}]
    result = transform_data(input_data)
    assert len(result) == 1
    assert result[0]["value"] == 100

def test_null_handling():
    """Test null value handling"""
    input_data = [{"id": 1, "value": None}]
    result = transform_data(input_data)
    assert result[0]["value"] == 0  # Default value
```

### 5. Environment Management

```python
# src/config/settings.py
import os

class Config:
    def __init__(self, environment):
        self.env = environment
    
    def get_data_path(self):
        paths = {
            "dev": "/mnt/dev/data",
            "staging": "/mnt/staging/data",
            "prod": "/mnt/prod/data"
        }
        return paths.get(self.env)
    
    def get_database(self):
        return f"{self.env}_database"

# Usage in notebook
config = Config(dbutils.widgets.get("environment"))
data_path = config.get_data_path()
```

## Common Workflows

### Workflow 1: Feature Development

```
1. Pull latest from develop
2. Create feature branch
3. Develop in Databricks
4. Test locally
5. Commit and push
6. Create pull request
7. Code review
8. Merge to develop
```

### Workflow 2: Hotfix

```
1. Create hotfix branch from main
2. Fix issue in Databricks
3. Test fix
4. Commit and push
5. Create PR to main
6. Fast-track review
7. Merge and deploy
```

### Workflow 3: Release

```
1. Create release branch from develop
2. Final testing
3. Update version numbers
4. Create PR to main
5. Merge after approval
6. Tag release
7. Deploy to production
```

## Troubleshooting

### Issue 1: Authentication Failed
```
Problem: Can't connect to Git repository
Solution:
  1. Verify PAT is valid
  2. Check PAT has correct scopes
  3. Re-enter credentials in Git Integration
```

### Issue 2: Merge Conflicts
```
Problem: Conflicts when pulling changes
Solution:
  1. Pull latest changes
  2. Open conflicted files
  3. Resolve conflicts manually
  4. Test changes
  5. Commit merge
```

### Issue 3: Detached HEAD
```
Problem: Not on any branch
Solution:
  1. Create new branch from current state
  2. Or checkout existing branch
  3. Commit changes
```

### Issue 4: Large Files
```
Problem: Can't push large files
Solution:
  1. Add to .gitignore
  2. Use Git LFS for large files
  3. Store data in cloud storage, not Git
```

## Exam Tips

### Key Concepts to Remember

1. **Repos Location**: `/Repos/username/repo-name`
2. **Supported Providers**: GitHub, GitLab, Bitbucket, Azure DevOps
3. **Authentication**: Personal Access Token (PAT)
4. **Branching**: Create, switch, merge branches
5. **Collaboration**: Pull requests, code review
6. **CI/CD**: Automated testing and deployment

### Common Exam Questions

- How to connect Databricks to Git?
- Where are repos stored in workspace?
- How to create a new branch?
- What authentication method is used?
- How to resolve merge conflicts?
- Best practices for repo organization?

## Summary

Today you learned:
- ✅ Setting up Git integration with Databricks
- ✅ Cloning and managing repositories
- ✅ Creating and switching branches
- ✅ Committing and pushing changes
- ✅ Collaborative development workflows
- ✅ Branching strategies for teams
- ✅ CI/CD integration
- ✅ Best practices for version control

## Next Steps

1. Complete the hands-on exercises
2. Set up a Git repository for your project
3. Practice branching and merging
4. Implement a CI/CD pipeline
5. Take the quiz
6. Move on to Day 25: Unity Catalog Basics

---

**Production Tip**: Always use feature branches for development and protect your main branch with required reviews!
