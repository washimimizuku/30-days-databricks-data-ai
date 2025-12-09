# Databricks notebook source
# MAGIC %md
# MAGIC # Day 24: Databricks Repos & Version Control - Exercises
# MAGIC 
# MAGIC ## Overview
# MAGIC Complete these exercises to master Databricks Repos and Git integration.
# MAGIC 
# MAGIC **Topics Covered**:
# MAGIC - Repository setup and cloning
# MAGIC - Branch management
# MAGIC - Committing and pushing changes
# MAGIC - Collaborative workflows
# MAGIC - CI/CD integration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the setup from `setup.md` first

# COMMAND ----------

# Verify you're in a Git repository
import os
current_path = os.getcwd()
print(f"Current path: {current_path}")

# Check if .git directory exists
is_git_repo = os.path.exists(os.path.join(current_path, ".git"))
print(f"Is Git repository: {is_git_repo}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Repository Basics (10 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.1: Clone a Repository
# MAGIC Clone your Git repository into Databricks Repos

# COMMAND ----------

# TODO: Document the steps you took to clone the repository
# 1. Navigate to Repos
# 2. Click Add Repo
# 3. Enter URL
# 4. Create

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.2: Explore Repository Structure
# MAGIC List all files and folders in your repository

# COMMAND ----------

# TODO: List all files in the repository
files = dbutils.fs.ls(f"file:{current_path}")
for file in files:
    print(f"{file.name} - {'DIR' if file.isDir() else 'FILE'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.3: Check Current Branch
# MAGIC Identify which branch you're currently on

# COMMAND ----------

# TODO: Check current branch
# Hint: Look at the branch dropdown in Repos UI

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.4: View Git Status
# MAGIC Check for uncommitted changes

# COMMAND ----------

# TODO: View Git status in the UI
# Navigate to Git tab in Repos

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.5: Create a New File
# MAGIC Create a new notebook in the repository

# COMMAND ----------

# TODO: Create a new notebook called "test_notebook"
# Save it in the notebooks/ directory

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.6: Modify Existing File
# MAGIC Edit the README.md file

# COMMAND ----------

# TODO: Add a new section to README.md
# Add: ## Contributors section

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.7: View Changed Files
# MAGIC See what files have been modified

# COMMAND ----------

# TODO: Check Git tab to see changed files

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.8: Discard Changes
# MAGIC Revert changes to a file

# COMMAND ----------

# TODO: Discard changes to test_notebook
# Right-click file → Discard changes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.9: Pull Latest Changes
# MAGIC Update your local repository with remote changes

# COMMAND ----------

# TODO: Pull latest changes from remote
# Click Pull button in Git tab

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.10: View Repository URL
# MAGIC Find the remote repository URL

# COMMAND ----------

# TODO: Find and document the repository URL
# Check repo settings or Git tab

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Branch Management (15 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.1: Create a Feature Branch
# MAGIC Create a new branch for feature development

# COMMAND ----------

# TODO: Create branch "feature/add-validation"
# Use branch dropdown → Create branch

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.2: Switch to Feature Branch
# MAGIC Change to your newly created branch

# COMMAND ----------

# TODO: Switch to feature/add-validation branch

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.3: Make Changes on Feature Branch
# MAGIC Add a new validation function

# COMMAND ----------

# TODO: Create src/utils/validation.py with a validation function

def validate_data(df):
    """Validate DataFrame has required columns"""
    required_columns = ["id", "name", "value"]
    missing = set(required_columns) - set(df.columns)
    return len(missing) == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.4: Commit Changes
# MAGIC Commit your changes with a meaningful message

# COMMAND ----------

# TODO: Commit with message "Add data validation function"
# Use Git tab → Enter message → Commit

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.5: Push Feature Branch
# MAGIC Push your feature branch to remote

# COMMAND ----------

# TODO: Push feature branch to remote
# Click Push button

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.6: Create Development Branch
# MAGIC Create a develop branch for integration

# COMMAND ----------

# TODO: Create "develop" branch from main

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.7: Switch Between Branches
# MAGIC Practice switching between main and develop

# COMMAND ----------

# TODO: Switch to main, then to develop, then back to feature branch

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.8: List All Branches
# MAGIC View all available branches

# COMMAND ----------

# TODO: List all branches in branch dropdown

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.9: Create Hotfix Branch
# MAGIC Create a branch for urgent fixes

# COMMAND ----------

# TODO: Create "hotfix/fix-null-handling" from main

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.10: Make Hotfix
# MAGIC Fix a critical issue

# COMMAND ----------

# TODO: Add null check to existing function
# Commit with message "Fix null handling in validation"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.11: View Branch History
# MAGIC See commit history for current branch

# COMMAND ----------

# TODO: View commit history in Git tab

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.12: Compare Branches
# MAGIC See differences between branches

# COMMAND ----------

# TODO: Compare feature branch with main
# Use Git provider's web interface

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.13: Delete Local Branch
# MAGIC Remove a branch you no longer need

# COMMAND ----------

# TODO: Delete a test branch (if created)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.14: Fetch Remote Branches
# MAGIC Update list of remote branches

# COMMAND ----------

# TODO: Fetch latest branches from remote
# Pull updates

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.15: Create Release Branch
# MAGIC Prepare a release

# COMMAND ----------

# TODO: Create "release/v1.0" from develop

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Collaborative Workflows (10 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.1: Create Pull Request
# MAGIC Create a PR for your feature branch

# COMMAND ----------

# TODO: On GitHub/GitLab, create PR from feature branch to develop
# Document the steps

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.2: Add PR Description
# MAGIC Write a clear PR description

# COMMAND ----------

# TODO: Add description explaining:
# - What changed
# - Why it changed
# - How to test

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.3: Request Code Review
# MAGIC Assign reviewers to your PR

# COMMAND ----------

# TODO: Add reviewers to your PR

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.4: Address Review Comments
# MAGIC Make changes based on feedback

# COMMAND ----------

# TODO: Make requested changes
# Commit and push updates

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.5: Resolve Merge Conflicts
# MAGIC Handle conflicts when merging

# COMMAND ----------

# TODO: Create a conflict scenario
# 1. Edit same file on two branches
# 2. Try to merge
# 3. Resolve conflicts

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.6: Merge Pull Request
# MAGIC Complete the PR merge

# COMMAND ----------

# TODO: Merge PR after approval

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.7: Update Local Branch After Merge
# MAGIC Sync your local branch with remote

# COMMAND ----------

# TODO: Switch to develop and pull latest changes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.8: Tag a Release
# MAGIC Create a version tag

# COMMAND ----------

# TODO: Create tag "v1.0.0" on main branch
# Use Git provider's web interface

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.9: Cherry-Pick a Commit
# MAGIC Apply specific commit to another branch

# COMMAND ----------

# TODO: Cherry-pick a commit from feature to hotfix
# Use Git provider's web interface

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.10: Revert a Commit
# MAGIC Undo a problematic commit

# COMMAND ----------

# TODO: Revert a commit if needed
# Create revert commit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Best Practices (10 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.1: Create .gitignore
# MAGIC Add appropriate ignore patterns

# COMMAND ----------

# TODO: Create/update .gitignore with:
# - Python cache files
# - Databricks checkpoints
# - Log files
# - Environment files

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.2: Organize Repository Structure
# MAGIC Create proper folder structure

# COMMAND ----------

# TODO: Ensure you have:
# - notebooks/
# - src/
# - tests/
# - jobs/
# - README.md

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.3: Add Documentation
# MAGIC Document your code and repository

# COMMAND ----------

# TODO: Add docstrings to functions
# Update README with usage instructions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.4: Create requirements.txt
# MAGIC List Python dependencies

# COMMAND ----------

# TODO: Create requirements.txt with:
# - pyspark
# - pytest
# - databricks-cli

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.5: Add Unit Tests
# MAGIC Create tests for your functions

# COMMAND ----------

# TODO: Add tests in tests/ directory

def test_validate_data():
    """Test validation function"""
    # Add test implementation
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.6: Use Meaningful Commit Messages
# MAGIC Practice good commit message format

# COMMAND ----------

# TODO: Make a commit with format:
# "Add feature: description of what was added"
# 
# Body: More details if needed

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.7: Keep Commits Atomic
# MAGIC Make small, focused commits

# COMMAND ----------

# TODO: Make separate commits for:
# 1. Add function
# 2. Add tests
# 3. Update documentation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.8: Use Branch Naming Conventions
# MAGIC Follow consistent naming

# COMMAND ----------

# TODO: Create branches following pattern:
# - feature/description
# - bugfix/description
# - hotfix/description
# - release/version

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.9: Protect Main Branch
# MAGIC Configure branch protection

# COMMAND ----------

# TODO: On Git provider, enable:
# - Require pull request reviews
# - Require status checks
# - Prevent force pushes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.10: Set Up Code Owners
# MAGIC Define code ownership

# COMMAND ----------

# TODO: Create CODEOWNERS file
# Assign owners for different paths

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: CI/CD Integration (10 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.1: Create GitHub Actions Workflow
# MAGIC Set up automated testing

# COMMAND ----------

# TODO: Create .github/workflows/test.yml
# Add workflow to run tests on PR

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.2: Add Linting Check
# MAGIC Enforce code quality

# COMMAND ----------

# TODO: Add linting step to CI workflow
# Use flake8 or pylint

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.3: Configure Deployment
# MAGIC Automate deployment to Databricks

# COMMAND ----------

# TODO: Add deployment step for main branch
# Use databricks-cli

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.4: Set Up Environment Secrets
# MAGIC Store credentials securely

# COMMAND ----------

# TODO: Add secrets to GitHub/GitLab:
# - DATABRICKS_HOST
# - DATABRICKS_TOKEN

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.5: Create Staging Environment
# MAGIC Set up separate environment for testing

# COMMAND ----------

# TODO: Configure staging deployment
# Deploy develop branch to staging

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.6: Add Status Badges
# MAGIC Show build status in README

# COMMAND ----------

# TODO: Add CI/CD status badge to README.md

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.7: Configure Notifications
# MAGIC Get alerts for build failures

# COMMAND ----------

# TODO: Set up email/Slack notifications for CI failures

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.8: Implement Blue-Green Deployment
# MAGIC Set up zero-downtime deployment

# COMMAND ----------

# TODO: Configure two production environments
# Switch between them for deployments

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.9: Add Rollback Capability
# MAGIC Enable quick rollback on issues

# COMMAND ----------

# TODO: Create rollback workflow
# Revert to previous version if needed

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.10: Monitor Deployment Success
# MAGIC Track deployment metrics

# COMMAND ----------

# TODO: Add monitoring for:
# - Deployment success rate
# - Deployment duration
# - Post-deployment errors

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Challenges (5 exercises)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 1: Implement Git Flow
# MAGIC Set up complete Git Flow workflow

# COMMAND ----------

# TODO: Create and document:
# - main (production)
# - develop (integration)
# - feature/* branches
# - release/* branches
# - hotfix/* branches

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 2: Create Pre-commit Hooks
# MAGIC Automate checks before commits

# COMMAND ----------

# TODO: Set up pre-commit hooks for:
# - Code formatting
# - Linting
# - Test execution

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 3: Implement Semantic Versioning
# MAGIC Automate version management

# COMMAND ----------

# TODO: Set up automatic version bumping
# Based on commit messages

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 4: Create Multi-Environment Pipeline
# MAGIC Build complete deployment pipeline

# COMMAND ----------

# TODO: Create pipeline:
# dev → staging → production
# With approval gates

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 5: Document Git Workflow
# MAGIC Create team guidelines

# COMMAND ----------

# TODO: Create CONTRIBUTING.md with:
# - Branching strategy
# - Commit message format
# - PR process
# - Code review guidelines

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC You've completed 55 exercises covering:
# MAGIC - ✅ Repository basics (10 exercises)
# MAGIC - ✅ Branch management (15 exercises)
# MAGIC - ✅ Collaborative workflows (10 exercises)
# MAGIC - ✅ Best practices (10 exercises)
# MAGIC - ✅ CI/CD integration (10 exercises)
# MAGIC - ✅ Bonus challenges (5 exercises)
# MAGIC 
# MAGIC Next steps:
# MAGIC 1. Review your solutions
# MAGIC 2. Practice with real projects
# MAGIC 3. Take the quiz
# MAGIC 4. Move on to Day 25: Unity Catalog Basics
