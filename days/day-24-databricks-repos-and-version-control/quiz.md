# Day 24 Quiz: Databricks Repos & Version Control

Test your knowledge of Databricks Repos and Git integration.

## Question 1
**What authentication method does Databricks Repos use to connect to Git providers?**

A) SSH keys  
B) Personal Access Token (PAT)  
C) OAuth  
D) Username and password  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Personal Access Token (PAT)**

Explanation: Databricks Repos uses Personal Access Tokens for authentication with Git providers. PATs provide secure, token-based authentication without exposing passwords.
</details>

---

## Question 2
**Where are Databricks Repos stored in the workspace?**

A) /Workspace/Repos/  
B) /Repos/username/repo-name  
C) /Git/Repos/  
D) /Users/username/repos/  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) /Repos/username/repo-name**

Explanation: Repos are stored under /Repos/ with the path structure /Repos/username/repo-name. This keeps repositories organized by user or team.
</details>

---

## Question 3
**Which Git providers are supported by Databricks Repos?**

A) Only GitHub  
B) GitHub and GitLab only  
C) GitHub, GitLab, Bitbucket, Azure DevOps, and AWS CodeCommit  
D) Any Git provider  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) GitHub, GitLab, Bitbucket, Azure DevOps, and AWS CodeCommit**

Explanation: Databricks Repos supports the major Git providers: GitHub (including Enterprise), GitLab (including self-hosted), Bitbucket (Cloud and Server), Azure DevOps, and AWS CodeCommit.
</details>

---

## Question 4
**How do you create a new branch in Databricks Repos?**

A) Use Git command line only  
B) Click branch dropdown and type new branch name  
C) Edit .git/config file  
D) Branches can only be created on Git provider website  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Click branch dropdown and type new branch name**

Explanation: In Databricks Repos UI, you can create a new branch by clicking the branch dropdown, typing the new branch name, and clicking "Create branch".
</details>

---

## Question 5
**What happens when you commit changes in Databricks Repos?**

A) Changes are only saved locally  
B) Changes are committed and pushed to remote  
C) Changes are staged but not committed  
D) Changes are lost  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Changes are committed and pushed to remote**

Explanation: When you commit in Databricks Repos UI, it performs both commit and push operations, immediately syncing your changes to the remote repository.
</details>

---

## Question 6
**Which file should you create to ignore Databricks-specific files in Git?**

A) .databricksignore  
B) .gitignore  
C) .ignore  
D) databricks.ignore  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) .gitignore**

Explanation: Use the standard .gitignore file to specify which files Git should ignore. Include patterns for Python cache files, checkpoints, and logs.
</details>

---

## Question 7
**What is the recommended branching strategy for team collaboration?**

A) Everyone works on main branch  
B) Git Flow with main, develop, and feature branches  
C) One branch per person  
D) No branching needed  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Git Flow with main, develop, and feature branches**

Explanation: Git Flow is recommended for teams, using main for production, develop for integration, and feature/* branches for new development. This provides structure and enables code review.
</details>

---

## Question 8
**How do you resolve merge conflicts in Databricks Repos?**

A) Conflicts are automatically resolved  
B) Pull changes, manually edit conflicted files, then commit  
C) Delete the repository and start over  
D) Conflicts cannot be resolved in Databricks  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Pull changes, manually edit conflicted files, then commit**

Explanation: When conflicts occur, you must pull the latest changes, manually resolve conflicts in the affected files, and then commit the merge resolution.
</details>

---

## Question 9
**What is the purpose of a pull request (PR)?**

A) To pull changes from remote  
B) To request code review before merging  
C) To create a new branch  
D) To delete old branches  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To request code review before merging**

Explanation: Pull requests enable code review by allowing team members to review, comment on, and approve changes before they're merged into the target branch.
</details>

---

## Question 10
**Which files should NOT be committed to Git?**

A) Notebooks and source code  
B) Configuration files  
C) Large data files and credentials  
D) Documentation  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Large data files and credentials**

Explanation: Never commit large data files (use cloud storage instead) or credentials (use secrets management). These should be in .gitignore.
</details>

---

## Question 11
**How do you switch between branches in Databricks Repos?**

A) Delete and re-clone repository  
B) Use branch dropdown to select branch  
C) Edit files manually  
D) Restart Databricks workspace  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Use branch dropdown to select branch**

Explanation: Simply click the branch dropdown in Repos UI and select the branch you want to switch to. Databricks will update the workspace to reflect that branch's content.
</details>

---

## Question 12
**What is the recommended structure for a Databricks repository?**

A) All files in root directory  
B) Separate folders for notebooks/, src/, tests/, and docs/  
C) One folder per notebook  
D) No specific structure needed  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Separate folders for notebooks/, src/, tests/, and docs/**

Explanation: Organize repositories with clear structure: notebooks/ for Databricks notebooks, src/ for Python modules, tests/ for unit tests, and docs/ for documentation.
</details>

---

## Question 13
**How can you automate deployment with Databricks Repos?**

A) Manual deployment only  
B) Use CI/CD pipelines with GitHub Actions or GitLab CI  
C) Email code to admin  
D) Copy files manually  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Use CI/CD pipelines with GitHub Actions or GitLab CI**

Explanation: Integrate Databricks Repos with CI/CD tools like GitHub Actions, GitLab CI, or Azure Pipelines to automate testing and deployment when code is merged.
</details>

---

## Question 14
**What should a good commit message include?**

A) Just "update"  
B) Type, scope, and clear description of changes  
C) Your name only  
D) Random text  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Type, scope, and clear description of changes**

Explanation: Good commit messages follow a format like "feat(pipeline): Add data validation" - including type (feat/fix/docs), scope, and clear description of what changed and why.
</details>

---

## Question 15
**Which branch should be protected with required reviews?**

A) Feature branches  
B) Personal branches  
C) Main/production branch  
D) All branches  

<details>
<summary>Click to reveal answer</summary>

**Answer: C) Main/production branch**

Explanation: The main/production branch should be protected with required pull request reviews, status checks, and restrictions on force pushes to ensure code quality and prevent accidental changes.
</details>

---

## Scoring

- **13-15 correct (87-100%)**: Excellent! You've mastered Databricks Repos
- **10-12 correct (67-86%)**: Good! Review the topics you missed
- **7-9 correct (47-66%)**: Fair - More practice needed
- **Below 7 (< 47%)**: Review the material and try again

---

**Next**: Day 25 - Unity Catalog Basics
