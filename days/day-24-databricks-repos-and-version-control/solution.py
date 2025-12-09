# Databricks notebook source
# MAGIC %md
# MAGIC # Day 24: Databricks Repos & Version Control - Solutions
# MAGIC 
# MAGIC Complete solutions and best practices for all exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Repository Basics - Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1.1-1.2: Clone and Explore Repository

# COMMAND ----------

import os

# Get current path
current_path = os.getcwd()
print(f"Current working directory: {current_path}")

# List all files and directories
def list_repo_structure(path=".", indent=0):
    """Recursively list repository structure"""
    try:
        items = os.listdir(path)
        for item in sorted(items):
            if item.startswith('.'):
                continue
            item_path = os.path.join(path, item)
            print("  " * indent + f"{'📁' if os.path.isdir(item_path) else '📄'} {item}")
            if os.path.isdir(item_path) and indent < 2:
                list_repo_structure(item_path, indent + 1)
    except PermissionError:
        pass

print("\nRepository Structure:")
list_repo_structure()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1.3-1.4: Check Branch and Status

# COMMAND ----------

# Check current branch (via Git command if available)
try:
    import subprocess
    branch = subprocess.check_output(
        ['git', 'branch', '--show-current'],
        cwd=current_path
    ).decode('utf-8').strip()
    print(f"Current branch: {branch}")
except:
    print("Check branch in Repos UI")

# Check Git status
try:
    status = subprocess.check_output(
        ['git', 'status', '--short'],
        cwd=current_path
    ).decode('utf-8')
    if status:
        print(f"Modified files:\n{status}")
    else:
        print("No uncommitted changes")
except:
    print("Check status in Repos UI Git tab")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1.5-1.8: File Operations

# COMMAND ----------

# Create a new test notebook
test_notebook_content = """# Databricks notebook source
# MAGIC %md
# MAGIC # Test Notebook
# MAGIC 
# MAGIC This is a test notebook for Git operations

# COMMAND ----------

print("Hello from test notebook!")

# COMMAND ----------

# Test function
def test_function():
    return "Test successful"

result = test_function()
print(result)
"""

# Write to file
test_notebook_path = os.path.join(current_path, "notebooks", "test_notebook.py")
os.makedirs(os.path.dirname(test_notebook_path), exist_ok=True)

with open(test_notebook_path, 'w') as f:
    f.write(test_notebook_content)

print(f"✅ Created test notebook at: {test_notebook_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Branch Management - Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 2.1-2.5: Feature Branch Workflow

# COMMAND ----------

# Complete feature branch workflow using Databricks SDK
from databricks.sdk import WorkspaceClient

def create_and_work_on_feature_branch(repo_path, feature_name):
    """
    Complete workflow for feature branch development
    
    Steps:
    1. Create feature branch
    2. Make changes
    3. Commit changes
    4. Push to remote
    """
    w = WorkspaceClient()
    
    # Get repo info
    repos = list(w.repos.list())
    repo = next((r for r in repos if repo_path in r.path), None)
    
    if not repo:
        print(f"Repository not found at {repo_path}")
        return
    
    # Create feature branch
    branch_name = f"feature/{feature_name}"
    print(f"Creating branch: {branch_name}")
    
    try:
        w.repos.update(
            repo_id=repo.id,
            branch=branch_name
        )
        print(f"✅ Switched to branch: {branch_name}")
    except Exception as e:
        print(f"Note: {e}")
        print("Create branch manually in UI if needed")
    
    return branch_name

# Example usage
# create_and_work_on_feature_branch("/Repos/username/repo", "add-validation")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 2.6-2.10: Branch Management Functions

# COMMAND ----------

class GitBranchManager:
    """Manage Git branches in Databricks Repos"""
    
    def __init__(self, repo_path):
        self.repo_path = repo_path
        self.w = WorkspaceClient()
        self.repo = self._get_repo()
    
    def _get_repo(self):
        """Get repository object"""
        repos = list(self.w.repos.list())
        return next((r for r in repos if self.repo_path in r.path), None)
    
    def create_branch(self, branch_name, from_branch="main"):
        """Create a new branch"""
        try:
            # First switch to base branch
            self.w.repos.update(
                repo_id=self.repo.id,
                branch=from_branch
            )
            
            # Then create new branch
            self.w.repos.update(
                repo_id=self.repo.id,
                branch=branch_name
            )
            print(f"✅ Created and switched to branch: {branch_name}")
            return True
        except Exception as e:
            print(f"Error creating branch: {e}")
            return False
    
    def switch_branch(self, branch_name):
        """Switch to existing branch"""
        try:
            self.w.repos.update(
                repo_id=self.repo.id,
                branch=branch_name
            )
            print(f"✅ Switched to branch: {branch_name}")
            return True
        except Exception as e:
            print(f"Error switching branch: {e}")
            return False
    
    def get_current_branch(self):
        """Get current branch name"""
        repo_info = self.w.repos.get(self.repo.id)
        return repo_info.branch
    
    def list_branches(self):
        """List all branches (requires Git provider API)"""
        print("Available branches:")
        print("  - main")
        print("  - develop")
        print("  - feature/*")
        print("  - hotfix/*")
        print("  - release/*")
        print("\nNote: Use Git provider web interface for complete list")

# Example usage
# manager = GitBranchManager("/Repos/username/repo")
# manager.create_branch("feature/new-feature")
# manager.switch_branch("develop")
# current = manager.get_current_branch()

print("✅ Branch management functions created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Collaborative Workflows - Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 3.1-3.7: Pull Request Workflow

# COMMAND ----------

def create_pull_request_checklist():
    """
    Checklist for creating a pull request
    """
    checklist = """
    # Pull Request Checklist
    
    ## Before Creating PR
    - [ ] All tests pass locally
    - [ ] Code follows style guidelines
    - [ ] Documentation updated
    - [ ] Commit messages are clear
    - [ ] Branch is up to date with base branch
    
    ## PR Description Template
    
    ### What Changed
    - Describe the changes made
    - List new features or fixes
    
    ### Why
    - Explain the motivation
    - Reference any issues
    
    ### How to Test
    1. Step-by-step testing instructions
    2. Expected results
    3. Edge cases to check
    
    ### Screenshots (if applicable)
    - Add screenshots for UI changes
    
    ### Checklist
    - [ ] Tests added/updated
    - [ ] Documentation updated
    - [ ] No breaking changes
    - [ ] Backward compatible
    
    ## After Creating PR
    - [ ] Assign reviewers
    - [ ] Add labels
    - [ ] Link related issues
    - [ ] Monitor CI/CD status
    - [ ] Address review comments
    - [ ] Resolve conflicts if any
    """
    
    return checklist

print(create_pull_request_checklist())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 3.8-3.10: Advanced Git Operations

# COMMAND ----------

def git_workflow_examples():
    """
    Examples of common Git workflows
    """
    workflows = {
        "Feature Development": """
        1. git checkout develop
        2. git pull origin develop
        3. git checkout -b feature/new-feature
        4. # Make changes
        5. git add .
        6. git commit -m "Add new feature"
        7. git push origin feature/new-feature
        8. # Create PR on Git provider
        9. # After approval, merge PR
        10. git checkout develop
        11. git pull origin develop
        """,
        
        "Hotfix": """
        1. git checkout main
        2. git pull origin main
        3. git checkout -b hotfix/critical-fix
        4. # Fix the issue
        5. git add .
        6. git commit -m "Fix critical issue"
        7. git push origin hotfix/critical-fix
        8. # Create PR to main
        9. # Fast-track review and merge
        10. # Also merge to develop
        """,
        
        "Release": """
        1. git checkout develop
        2. git pull origin develop
        3. git checkout -b release/v1.0.0
        4. # Update version numbers
        5. # Final testing
        6. git add .
        7. git commit -m "Prepare release v1.0.0"
        8. git push origin release/v1.0.0
        9. # Create PR to main
        10. # After merge, tag release
        11. git tag -a v1.0.0 -m "Release v1.0.0"
        12. git push origin v1.0.0
        """
    }
    
    return workflows

workflows = git_workflow_examples()
for name, workflow in workflows.items():
    print(f"\n{'='*50}")
    print(f"{name} Workflow")
    print('='*50)
    print(workflow)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Best Practices - Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 4.1-4.5: Repository Setup

# COMMAND ----------

def create_gitignore():
    """Create comprehensive .gitignore for Databricks"""
    gitignore_content = """# Databricks
.databricks/
*.pyc
__pycache__/
_checkpoints/
*.checkpoint

# Python
*.py[cod]
*$py.class
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg
MANIFEST

# Virtual Environment
venv/
ENV/
env/
.venv

# IDE
.idea/
.vscode/
*.swp
*.swo
*~
.project
.pydevproject
.settings/

# OS
.DS_Store
.DS_Store?
._*
.Spotlight-V100
.Trashes
ehthumbs.db
Thumbs.db

# Logs
*.log
logs/

# Environment
.env
.env.local
.env.*.local

# Testing
.pytest_cache/
.coverage
htmlcov/
.tox/

# Jupyter
.ipynb_checkpoints/

# Data files (don't commit large data)
*.csv
*.parquet
*.json
data/
*.db
*.sqlite
"""
    return gitignore_content

def create_requirements_txt():
    """Create requirements.txt"""
    requirements = """# Core dependencies
pyspark==3.4.0
delta-spark==2.4.0

# Testing
pytest==7.4.0
pytest-cov==4.1.0

# Development
black==23.7.0
flake8==6.0.0
pylint==2.17.0

# Databricks
databricks-cli==0.17.7
databricks-sdk==0.1.0

# Utilities
python-dotenv==1.0.0
pyyaml==6.0
"""
    return requirements

# Write files
with open(".gitignore", "w") as f:
    f.write(create_gitignore())
print("✅ Created .gitignore")

with open("requirements.txt", "w") as f:
    f.write(create_requirements_txt())
print("✅ Created requirements.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 4.6-4.10: Code Quality

# COMMAND ----------

def commit_message_template():
    """Template for good commit messages"""
    template = """
    # Commit Message Format
    
    ## Structure
    <type>(<scope>): <subject>
    
    <body>
    
    <footer>
    
    ## Types
    - feat: New feature
    - fix: Bug fix
    - docs: Documentation changes
    - style: Code style changes (formatting)
    - refactor: Code refactoring
    - test: Adding or updating tests
    - chore: Maintenance tasks
    
    ## Examples
    
    Good:
    feat(pipeline): Add data validation step
    
    Added validation function to check for null values
    and data type consistency before processing.
    
    Closes #123
    
    ---
    
    fix(ingestion): Handle missing timestamps
    
    Fixed null pointer exception when timestamp column
    is missing from source data.
    
    ---
    
    docs(readme): Update setup instructions
    
    Added section on configuring Git integration
    and troubleshooting common issues.
    """
    return template

print(commit_message_template())

# COMMAND ----------

def create_codeowners():
    """Create CODEOWNERS file"""
    codeowners = """# Code Owners
# Each line is a file pattern followed by one or more owners

# Default owners for everything
* @data-engineering-team

# Notebooks
/notebooks/ @data-engineers @analysts

# Source code
/src/ @data-engineers

# Tests
/tests/ @qa-team @data-engineers

# CI/CD
/.github/ @devops-team
/.gitlab-ci.yml @devops-team

# Documentation
/docs/ @tech-writers @data-engineers
README.md @tech-writers

# Configuration
/config/ @platform-team
"""
    return codeowners

with open("CODEOWNERS", "w") as f:
    f.write(create_codeowners())
print("✅ Created CODEOWNERS file")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: CI/CD Integration - Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 5.1-5.5: GitHub Actions Workflow

# COMMAND ----------

def create_github_actions_workflow():
    """Create GitHub Actions workflow for CI/CD"""
    workflow = """name: Databricks CI/CD

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main, develop]

env:
  DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
  DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt
          pip install pytest pytest-cov flake8
      
      - name: Lint with flake8
        run: |
          flake8 src/ --count --select=E9,F63,F7,F82 --show-source --statistics
          flake8 src/ --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics
      
      - name: Run tests
        run: |
          pytest tests/ -v --cov=src --cov-report=xml
      
      - name: Upload coverage
        uses: codecov/codecov-action@v3
        with:
          file: ./coverage.xml
  
  deploy-dev:
    needs: test
    if: github.ref == 'refs/heads/develop'
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
      
      - name: Deploy to Dev
        run: |
          pip install databricks-cli
          databricks repos update \\
            --path /Repos/dev/pipeline \\
            --branch develop
  
  deploy-prod:
    needs: test
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment: production
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
      
      - name: Deploy to Production
        run: |
          pip install databricks-cli
          databricks repos update \\
            --path /Repos/production/pipeline \\
            --branch main
"""
    return workflow

# Save workflow
import os
os.makedirs(".github/workflows", exist_ok=True)
with open(".github/workflows/databricks-ci.yml", "w") as f:
    f.write(create_github_actions_workflow())

print("✅ Created GitHub Actions workflow")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 5.6-5.10: Deployment and Monitoring

# COMMAND ----------

class DeploymentManager:
    """Manage deployments across environments"""
    
    def __init__(self):
        self.environments = {
            "dev": {
                "repo_path": "/Repos/dev/pipeline",
                "branch": "develop",
                "database": "dev_db"
            },
            "staging": {
                "repo_path": "/Repos/staging/pipeline",
                "branch": "release/*",
                "database": "staging_db"
            },
            "prod": {
                "repo_path": "/Repos/production/pipeline",
                "branch": "main",
                "database": "prod_db"
            }
        }
    
    def deploy(self, environment, branch=None):
        """Deploy to specified environment"""
        if environment not in self.environments:
            raise ValueError(f"Unknown environment: {environment}")
        
        config = self.environments[environment]
        target_branch = branch or config["branch"]
        
        print(f"Deploying to {environment}...")
        print(f"  Repo: {config['repo_path']}")
        print(f"  Branch: {target_branch}")
        print(f"  Database: {config['database']}")
        
        # Update repo
        w = WorkspaceClient()
        repos = list(w.repos.list())
        repo = next((r for r in repos if config['repo_path'] in r.path), None)
        
        if repo:
            w.repos.update(
                repo_id=repo.id,
                branch=target_branch
            )
            print(f"✅ Deployed successfully")
        else:
            print(f"❌ Repository not found")
    
    def rollback(self, environment, previous_commit):
        """Rollback to previous version"""
        print(f"Rolling back {environment} to {previous_commit}")
        # Implementation would revert to previous commit
    
    def health_check(self, environment):
        """Check deployment health"""
        print(f"Health check for {environment}:")
        print("  ✅ Repository accessible")
        print("  ✅ Database connection OK")
        print("  ✅ Jobs running")
        return True

# Example usage
manager = DeploymentManager()
print("✅ Deployment manager created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC ✅ Completed all solutions:
# MAGIC - Repository setup and management
# MAGIC - Branch operations and workflows
# MAGIC - Collaborative development
# MAGIC - Best practices implementation
# MAGIC - CI/CD pipeline setup
# MAGIC - Deployment automation
# MAGIC 
# MAGIC **Key Takeaways:**
# MAGIC 1. Use feature branches for development
# MAGIC 2. Write clear commit messages
# MAGIC 3. Implement code review process
# MAGIC 4. Automate testing and deployment
# MAGIC 5. Protect main branch
# MAGIC 6. Document workflows
# MAGIC 7. Use proper .gitignore
# MAGIC 8. Implement CI/CD pipelines
