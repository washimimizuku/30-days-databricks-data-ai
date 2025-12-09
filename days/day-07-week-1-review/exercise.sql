-- Day 7: Week 1 Review - Mini-Project
-- =========================================================
-- This mini-project integrates concepts from Days 1-6
-- =========================================================

USE week1_review;

-- =========================================================
-- PART 1: DATABASE AND TABLE MANAGEMENT (Day 5)
-- =========================================================

-- Exercise 1: Verify Table Types
-- TODO: Check if companies table is external and projects table is managed
-- Hint: Use DESCRIBE EXTENDED

-- YOUR CODE HERE


-- Exercise 2: Check Default Location
-- TODO: Find the default storage location for the projects table

-- YOUR CODE HERE


-- Exercise 3: Create a New Managed Table
-- TODO: Create a managed table called 'departments' with columns:
-- department_id (INT), department_name (STRING), budget (DECIMAL)

-- YOUR CODE HERE


-- Exercise 4: Create a New External Table
-- TODO: Create an external table called 'vendors' at '/FileStore/week1_review/vendors'

-- YOUR CODE HERE


-- =========================================================
-- PART 2: DELTA LAKE FEATURES (Day 3)
-- =========================================================

-- Exercise 5: View Table History
-- TODO: Show the history of the projects table

-- YOUR CODE HERE


-- Exercise 6: Update Data
-- TODO: Update the status of project_id 101 to 'Completed'

-- YOUR CODE HERE


-- Exercise 7: Time Travel - Before Update
-- TODO: Query the projects table as it was before the update (VERSION AS OF 0)

-- YOUR CODE HERE


-- Exercise 8: Time Travel - After Update
-- TODO: Query the current version of projects table

-- YOUR CODE HERE


-- Exercise 9: Optimize Table
-- TODO: Optimize the projects table

-- YOUR CODE HERE


-- Exercise 10: Z-Order Optimization
-- TODO: Optimize the projects table with Z-ORDER BY company_id

-- YOUR CODE HERE


-- =========================================================
-- PART 3: BASIC SQL QUERIES (Day 6)
-- =========================================================

-- Exercise 11: Simple SELECT
-- TODO: List all companies in the Technology industry

-- YOUR CODE HERE


-- Exercise 12: Aggregation
-- TODO: Count the number of projects by status

-- YOUR CODE HERE


-- Exercise 13: Filtering and Sorting
-- TODO: Find engineers with more than 7 years of experience, ordered by hourly_rate DESC

-- YOUR CODE HERE


-- Exercise 14: DISTINCT Values
-- TODO: Find all unique specializations

-- YOUR CODE HERE


-- Exercise 15: Date Filtering
-- TODO: Find all projects that started in 2024

-- YOUR CODE HERE


-- =========================================================
-- PART 4: JOINS (Day 6)
-- =========================================================

-- Exercise 16: INNER JOIN
-- TODO: Join projects and companies to show project names with company names

-- YOUR CODE HERE


-- Exercise 17: LEFT JOIN
-- TODO: Show all engineers and their total allocated hours (include engineers with 0 hours)

-- YOUR CODE HERE


-- Exercise 18: Multiple JOINs
-- TODO: Join projects, companies, and project_assignments to show:
-- project_name, company_name, and count of assigned engineers

-- YOUR CODE HERE


-- Exercise 19: SELF JOIN
-- TODO: Find pairs of engineers with the same specialization

-- YOUR CODE HERE


-- Exercise 20: Complex JOIN
-- TODO: Show project_name, company_name, engineer names, and hours_allocated
-- for all active projects

-- YOUR CODE HERE


-- =========================================================
-- PART 5: AGGREGATIONS AND GROUP BY (Day 6)
-- =========================================================

-- Exercise 21: Basic Aggregation
-- TODO: Calculate total budget across all projects

-- YOUR CODE HERE


-- Exercise 22: GROUP BY
-- TODO: Calculate total budget by company

-- YOUR CODE HERE


-- Exercise 23: GROUP BY with Multiple Aggregates
-- TODO: For each company, show: number of projects, total budget, average budget

-- YOUR CODE HERE


-- Exercise 24: HAVING Clause
-- TODO: Find companies with total project budget > 1,000,000

-- YOUR CODE HERE


-- Exercise 25: Complex Aggregation
-- TODO: For each specialization, show: count of engineers, average hourly rate,
-- total hours allocated

-- YOUR CODE HERE


-- =========================================================
-- PART 6: SUBQUERIES (Day 6)
-- =========================================================

-- Exercise 26: Scalar Subquery
-- TODO: Find projects with budget greater than the average project budget

-- YOUR CODE HERE


-- Exercise 27: IN Subquery
-- TODO: Find engineers assigned to projects at TechCorp

-- YOUR CODE HERE


-- Exercise 28: NOT IN Subquery
-- TODO: Find engineers who are not assigned to any project

-- YOUR CODE HERE


-- Exercise 29: Correlated Subquery
-- TODO: Find projects with budget greater than the average budget for their company

-- YOUR CODE HERE


-- Exercise 30: EXISTS
-- TODO: Find companies that have at least one completed project

-- YOUR CODE HERE


-- =========================================================
-- PART 7: COMMON TABLE EXPRESSIONS (Day 6)
-- =========================================================

-- Exercise 31: Basic CTE
-- TODO: Create a CTE for high-budget projects (>500000) and count them by company

-- YOUR CODE HERE


-- Exercise 32: Multiple CTEs
-- TODO: Create two CTEs:
-- 1. company_stats: total projects and budget per company
-- 2. top_companies: companies with >2 projects
-- Then join with companies table to show company details

-- YOUR CODE HERE


-- Exercise 33: CTE with Aggregation
-- TODO: Create a CTE for engineer utilization (total hours / 160 * 100 as percentage)
-- Then find engineers with >100% utilization

-- YOUR CODE HERE


-- Exercise 34: Nested CTE
-- TODO: Create CTEs to find:
-- 1. Project costs (hours * hourly_rate)
-- 2. Projects over budget (cost > budget)

-- YOUR CODE HERE


-- =========================================================
-- PART 8: VIEWS (Day 5)
-- =========================================================

-- Exercise 35: Create Standard View
-- TODO: Create a view 'project_summary' showing project details with company info

-- YOUR CODE HERE


-- Exercise 36: Create Temporary View
-- TODO: Create a temp view 'overbudget_projects' for projects where
-- actual cost > budget (calculate from assignments and hourly rates)

-- YOUR CODE HERE


-- Exercise 37: Query Views
-- TODO: Query both views created above

-- YOUR CODE HERE


-- Exercise 38: Drop View
-- TODO: Drop the project_summary view

-- YOUR CODE HERE


-- =========================================================
-- PART 9: ADVANCED QUERIES
-- =========================================================

-- Exercise 39: CASE Expression
-- TODO: Categorize projects as 'Small' (<300k), 'Medium' (300k-700k), 'Large' (>700k)

-- YOUR CODE HERE


-- Exercise 40: Window Function (Preview for Week 2)
-- TODO: Rank projects by budget within each company

-- YOUR CODE HERE


-- Exercise 41: Date Calculations
-- TODO: Calculate project duration in days and months

-- YOUR CODE HERE


-- Exercise 42: String Functions
-- TODO: Create full names for engineers and email domains

-- YOUR CODE HERE


-- Exercise 43: Set Operations - UNION
-- TODO: Combine engineers from 'Data Engineering' and projects with budget > 600000
-- (create a unified list of high-value resources)

-- YOUR CODE HERE


-- Exercise 44: Set Operations - INTERSECT
-- TODO: Find engineers who are both experienced (>7 years) AND highly paid (>150/hr)

-- YOUR CODE HERE


-- Exercise 45: Set Operations - EXCEPT
-- TODO: Find companies that have projects but no completed milestones

-- YOUR CODE HERE


-- =========================================================
-- PART 10: OPTIMIZATION AND BEST PRACTICES (Day 3)
-- =========================================================

-- Exercise 46: Check Table Statistics
-- TODO: Use DESCRIBE DETAIL to check file count and size for projects table

-- YOUR CODE HERE


-- Exercise 47: Partition Information
-- TODO: Show all partitions in project_assignments table

-- YOUR CODE HERE


-- Exercise 48: Vacuum Simulation
-- TODO: Check what files would be removed by VACUUM (use DRY RUN)
-- Note: VACUUM DRY RUN shows files that would be deleted

-- YOUR CODE HERE


-- Exercise 49: Table Properties
-- TODO: Set table properties for auto-optimize on projects table

-- YOUR CODE HERE


-- Exercise 50: Performance Query
-- TODO: Write an optimized query to find:
-- - Top 5 companies by total project budget
-- - Their project count
-- - Average project duration
-- - Number of unique engineers assigned
-- Use CTEs for readability

-- YOUR CODE HERE


-- =========================================================
-- BONUS: COMPREHENSIVE ANALYSIS
-- =========================================================

-- Bonus 1: Resource Utilization Report
-- TODO: Create a comprehensive report showing:
-- - Engineer name, specialization, hourly rate
-- - Total hours allocated
-- - Total revenue generated (hours * rate)
-- - Number of projects
-- - Utilization percentage
-- - Rank within specialization by revenue

-- YOUR CODE HERE


-- Bonus 2: Project Health Dashboard
-- TODO: Create a dashboard query showing:
-- - Project name, company, status
-- - Budget vs actual cost
-- - Percentage complete (completed milestones / total milestones)
-- - Days remaining until end_date
-- - Risk level (Red/Yellow/Green based on budget and timeline)

-- YOUR CODE HERE


-- Bonus 3: Company Performance Analysis
-- TODO: Analyze each company:
-- - Total investment (sum of budgets)
-- - Number of active vs completed projects
-- - Average project duration
-- - Total engineer hours allocated
-- - ROI indicator (completed projects / total projects)

-- YOUR CODE HERE


-- Bonus 4: Engineer Skill Matrix
-- TODO: Create a matrix showing:
-- - Specialization
-- - Number of engineers
-- - Average experience
-- - Average hourly rate
-- - Total hours allocated
-- - Demand indicator (hours / engineer count)

-- YOUR CODE HERE


-- Bonus 5: Time Series Analysis
-- TODO: Analyze project starts by month:
-- - Month/Year
-- - Number of projects started
-- - Total budget allocated
-- - Running total of active projects
-- - Trend indicator (increasing/decreasing)

-- YOUR CODE HERE


-- =========================================================
-- REFLECTION QUESTIONS
-- =========================================================
-- Answer these in a markdown cell:
--
-- 1. What's the difference between managed and external tables?
-- 2. When should you use OPTIMIZE vs VACUUM?
-- 3. What are the benefits of using CTEs over subqueries?
-- 4. How does partitioning improve query performance?
-- 5. What's the difference between INNER JOIN and LEFT JOIN?
-- 6. When should you use WHERE vs HAVING?
-- 7. What is Time Travel and how do you use it?
-- 8. What are the three types of views and their scopes?
-- 9. How do you optimize a large Delta table?
-- 10. What's the default retention period for VACUUM?
-- 11. What happens when you DROP a managed table?
-- 12. What happens when you DROP an external table?
-- 13. What's the difference between UNION and UNION ALL?
-- 14. How do you find the storage location of a table?
-- 15. What's the purpose of Z-ORDER BY?

-- =========================================================
-- WEEK 1 KNOWLEDGE CHECK
-- =========================================================
-- Rate your confidence (1-5) on each topic:
--
-- [ ] Databricks Architecture (Control vs Data Plane)
-- [ ] Cluster Types and Configuration
-- [ ] Delta Lake ACID Transactions
-- [ ] Time Travel Queries
-- [ ] OPTIMIZE and VACUUM
-- [ ] DBFS File Operations
-- [ ] Managed vs External Tables
-- [ ] View Types and Scopes
-- [ ] SQL JOINs (all types)
-- [ ] Aggregations and GROUP BY
-- [ ] Subqueries and CTEs
-- [ ] Set Operations
-- [ ] Partitioning Strategies
-- [ ] Table Metadata Commands
-- [ ] Query Optimization

-- =========================================================
-- CLEANUP (Optional)
-- =========================================================

-- Uncomment to clean up
-- DROP VIEW IF EXISTS project_summary;
-- DROP VIEW IF EXISTS overbudget_projects;
-- DROP DATABASE IF EXISTS week1_review CASCADE;

-- =========================================================
-- CONGRATULATIONS!
-- =========================================================
-- You've completed Week 1 of the Databricks Data Engineer bootcamp!
-- 
-- Key Achievements:
-- ✅ Mastered Databricks architecture
-- ✅ Configured and managed clusters
-- ✅ Worked with Delta Lake features
-- ✅ Performed DBFS operations
-- ✅ Created managed and external tables
-- ✅ Wrote complex SQL queries
-- ✅ Used JOINs, CTEs, and subqueries
-- ✅ Applied optimization techniques
--
-- Next Steps:
-- 1. Complete the 50-question quiz
-- 2. Review any weak areas
-- 3. Get ready for Week 2: Advanced SQL and Python!
--
-- 🎉 Great job! See you in Week 2! 🎉

