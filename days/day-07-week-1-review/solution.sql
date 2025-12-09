-- Day 7: Week 1 Review - Mini-Project Solutions
-- =========================================================

USE week1_review;

-- =========================================================
-- PART 1: DATABASE AND TABLE MANAGEMENT (Day 5)
-- =========================================================

-- Exercise 1: Verify Table Types
DESCRIBE EXTENDED companies;
-- Look for "Type: EXTERNAL" and location path

DESCRIBE EXTENDED projects;
-- Look for "Type: MANAGED" and warehouse location

-- Exercise 2: Check Default Location
DESCRIBE DETAIL projects;
-- Shows location in /user/hive/warehouse/week1_review.db/projects

-- Exercise 3: Create a New Managed Table
CREATE TABLE IF NOT EXISTS departments (
    department_id INT,
    department_name STRING,
    budget DECIMAL(12,2)
) USING DELTA;

-- Exercise 4: Create a New External Table
CREATE TABLE IF NOT EXISTS vendors (
    vendor_id INT,
    vendor_name STRING,
    contact_email STRING
) USING DELTA
LOCATION '/FileStore/week1_review/vendors';

-- =========================================================
-- PART 2: DELTA LAKE FEATURES (Day 3)
-- =========================================================

-- Exercise 5: View Table History
DESCRIBE HISTORY projects;

-- Exercise 6: Update Data
UPDATE projects 
SET status = 'Completed' 
WHERE project_id = 101;

-- Exercise 7: Time Travel - Before Update
SELECT * FROM projects VERSION AS OF 0
WHERE project_id = 101;

-- Exercise 8: Time Travel - After Update
SELECT * FROM projects
WHERE project_id = 101;

-- Exercise 9: Optimize Table
OPTIMIZE projects;

-- Exercise 10: Z-Order Optimization
OPTIMIZE projects ZORDER BY (company_id);

-- =========================================================
-- PART 3: BASIC SQL QUERIES (Day 6)
-- =========================================================

-- Exercise 11: Simple SELECT
SELECT * FROM companies
WHERE industry = 'Technology';

-- Exercise 12: Aggregation
SELECT 
    status,
    COUNT(*) AS project_count
FROM projects
GROUP BY status;

-- Exercise 13: Filtering and Sorting
SELECT * FROM engineers
WHERE years_experience > 7
ORDER BY hourly_rate DESC;

-- Exercise 14: DISTINCT Values
SELECT DISTINCT specialization
FROM engineers
ORDER BY specialization;

-- Exercise 15: Date Filtering
SELECT * FROM projects
WHERE YEAR(start_date) = 2024;

-- =========================================================
-- PART 4: JOINS (Day 6)
-- =========================================================

-- Exercise 16: INNER JOIN
SELECT 
    p.project_name,
    c.company_name,
    p.budget,
    p.status
FROM projects p
INNER JOIN companies c ON p.company_id = c.company_id;

-- Exercise 17: LEFT JOIN
SELECT 
    e.engineer_id,
    e.first_name || ' ' || e.last_name AS engineer_name,
    COALESCE(SUM(pa.hours_allocated), 0) AS total_hours
FROM engineers e
LEFT JOIN project_assignments pa ON e.engineer_id = pa.engineer_id
GROUP BY e.engineer_id, e.first_name, e.last_name;

-- Exercise 18: Multiple JOINs
SELECT 
    p.project_name,
    c.company_name,
    COUNT(DISTINCT pa.engineer_id) AS engineer_count
FROM projects p
INNER JOIN companies c ON p.company_id = c.company_id
LEFT JOIN project_assignments pa ON p.project_id = pa.project_id
GROUP BY p.project_name, c.company_name;

-- Exercise 19: SELF JOIN
SELECT 
    e1.first_name || ' ' || e1.last_name AS engineer1,
    e2.first_name || ' ' || e2.last_name AS engineer2,
    e1.specialization
FROM engineers e1
INNER JOIN engineers e2 ON e1.specialization = e2.specialization
WHERE e1.engineer_id < e2.engineer_id
ORDER BY e1.specialization;

-- Exercise 20: Complex JOIN
SELECT 
    p.project_name,
    c.company_name,
    e.first_name || ' ' || e.last_name AS engineer_name,
    pa.hours_allocated
FROM projects p
INNER JOIN companies c ON p.company_id = c.company_id
INNER JOIN project_assignments pa ON p.project_id = pa.project_id
INNER JOIN engineers e ON pa.engineer_id = e.engineer_id
WHERE p.status = 'In Progress'
ORDER BY p.project_name, e.last_name;

-- =========================================================
-- PART 5: AGGREGATIONS AND GROUP BY (Day 6)
-- =========================================================

-- Exercise 21: Basic Aggregation
SELECT SUM(budget) AS total_budget
FROM projects;

-- Exercise 22: GROUP BY
SELECT 
    c.company_name,
    SUM(p.budget) AS total_budget
FROM projects p
INNER JOIN companies c ON p.company_id = c.company_id
GROUP BY c.company_name
ORDER BY total_budget DESC;

-- Exercise 23: GROUP BY with Multiple Aggregates
SELECT 
    c.company_name,
    COUNT(p.project_id) AS project_count,
    SUM(p.budget) AS total_budget,
    AVG(p.budget) AS avg_budget
FROM projects p
INNER JOIN companies c ON p.company_id = c.company_id
GROUP BY c.company_name
ORDER BY total_budget DESC;

-- Exercise 24: HAVING Clause
SELECT 
    c.company_name,
    SUM(p.budget) AS total_budget
FROM projects p
INNER JOIN companies c ON p.company_id = c.company_id
GROUP BY c.company_name
HAVING SUM(p.budget) > 1000000
ORDER BY total_budget DESC;

-- Exercise 25: Complex Aggregation
SELECT 
    e.specialization,
    COUNT(e.engineer_id) AS engineer_count,
    AVG(e.hourly_rate) AS avg_hourly_rate,
    COALESCE(SUM(pa.hours_allocated), 0) AS total_hours
FROM engineers e
LEFT JOIN project_assignments pa ON e.engineer_id = pa.engineer_id
GROUP BY e.specialization
ORDER BY total_hours DESC;

-- =========================================================
-- PART 6: SUBQUERIES (Day 6)
-- =========================================================

-- Exercise 26: Scalar Subquery
SELECT 
    project_name,
    budget
FROM projects
WHERE budget > (SELECT AVG(budget) FROM projects);

-- Exercise 27: IN Subquery
SELECT 
    e.first_name || ' ' || e.last_name AS engineer_name,
    e.specialization
FROM engineers e
WHERE e.engineer_id IN (
    SELECT pa.engineer_id
    FROM project_assignments pa
    INNER JOIN projects p ON pa.project_id = p.project_id
    INNER JOIN companies c ON p.company_id = c.company_id
    WHERE c.company_name = 'TechCorp'
);

-- Exercise 28: NOT IN Subquery
SELECT 
    engineer_id,
    first_name || ' ' || last_name AS engineer_name,
    specialization
FROM engineers
WHERE engineer_id NOT IN (
    SELECT DISTINCT engineer_id FROM project_assignments
);

-- Exercise 29: Correlated Subquery
SELECT 
    p1.project_name,
    p1.budget,
    c.company_name
FROM projects p1
INNER JOIN companies c ON p1.company_id = c.company_id
WHERE p1.budget > (
    SELECT AVG(p2.budget)
    FROM projects p2
    WHERE p2.company_id = p1.company_id
);

-- Exercise 30: EXISTS
SELECT 
    c.company_name,
    c.industry
FROM companies c
WHERE EXISTS (
    SELECT 1
    FROM projects p
    WHERE p.company_id = c.company_id
    AND p.status = 'Completed'
);

-- =========================================================
-- PART 7: COMMON TABLE EXPRESSIONS (Day 6)
-- =========================================================

-- Exercise 31: Basic CTE
WITH high_budget_projects AS (
    SELECT * FROM projects WHERE budget > 500000
)
SELECT 
    c.company_name,
    COUNT(hbp.project_id) AS high_budget_count
FROM high_budget_projects hbp
INNER JOIN companies c ON hbp.company_id = c.company_id
GROUP BY c.company_name;

-- Exercise 32: Multiple CTEs
WITH 
    company_stats AS (
        SELECT 
            company_id,
            COUNT(project_id) AS project_count,
            SUM(budget) AS total_budget
        FROM projects
        GROUP BY company_id
    ),
    top_companies AS (
        SELECT company_id
        FROM company_stats
        WHERE project_count > 2
    )
SELECT 
    c.company_name,
    c.industry,
    cs.project_count,
    cs.total_budget
FROM companies c
INNER JOIN top_companies tc ON c.company_id = tc.company_id
INNER JOIN company_stats cs ON c.company_id = cs.company_id
ORDER BY cs.total_budget DESC;

-- Exercise 33: CTE with Aggregation
WITH engineer_utilization AS (
    SELECT 
        e.engineer_id,
        e.first_name || ' ' || e.last_name AS engineer_name,
        SUM(pa.hours_allocated) AS total_hours,
        (SUM(pa.hours_allocated) / 160.0 * 100) AS utilization_pct
    FROM engineers e
    LEFT JOIN project_assignments pa ON e.engineer_id = pa.engineer_id
    GROUP BY e.engineer_id, e.first_name, e.last_name
)
SELECT *
FROM engineer_utilization
WHERE utilization_pct > 100
ORDER BY utilization_pct DESC;

-- Exercise 34: Nested CTE
WITH 
    project_costs AS (
        SELECT 
            p.project_id,
            p.project_name,
            p.budget,
            SUM(pa.hours_allocated * e.hourly_rate) AS actual_cost
        FROM projects p
        LEFT JOIN project_assignments pa ON p.project_id = pa.project_id
        LEFT JOIN engineers e ON pa.engineer_id = e.engineer_id
        GROUP BY p.project_id, p.project_name, p.budget
    ),
    overbudget_projects AS (
        SELECT *
        FROM project_costs
        WHERE actual_cost > budget
    )
SELECT 
    project_name,
    budget,
    actual_cost,
    (actual_cost - budget) AS over_amount,
    ROUND((actual_cost - budget) / budget * 100, 2) AS over_percentage
FROM overbudget_projects
ORDER BY over_percentage DESC;

-- =========================================================
-- PART 8: VIEWS (Day 5)
-- =========================================================

-- Exercise 35: Create Standard View
CREATE OR REPLACE VIEW project_summary AS
SELECT 
    p.project_id,
    p.project_name,
    c.company_name,
    c.industry,
    p.budget,
    p.start_date,
    p.end_date,
    p.status,
    DATEDIFF(p.end_date, p.start_date) AS duration_days
FROM projects p
INNER JOIN companies c ON p.company_id = c.company_id;

-- Exercise 36: Create Temporary View
CREATE OR REPLACE TEMP VIEW overbudget_projects AS
WITH project_costs AS (
    SELECT 
        p.project_id,
        p.project_name,
        p.budget,
        SUM(pa.hours_allocated * e.hourly_rate) AS actual_cost
    FROM projects p
    LEFT JOIN project_assignments pa ON p.project_id = pa.project_id
    LEFT JOIN engineers e ON pa.engineer_id = e.engineer_id
    GROUP BY p.project_id, p.project_name, p.budget
)
SELECT *
FROM project_costs
WHERE actual_cost > budget;

-- Exercise 37: Query Views
SELECT * FROM project_summary WHERE status = 'In Progress';
SELECT * FROM overbudget_projects;

-- Exercise 38: Drop View
DROP VIEW IF EXISTS project_summary;

-- =========================================================
-- PART 9: ADVANCED QUERIES
-- =========================================================

-- Exercise 39: CASE Expression
SELECT 
    project_name,
    budget,
    CASE 
        WHEN budget < 300000 THEN 'Small'
        WHEN budget BETWEEN 300000 AND 700000 THEN 'Medium'
        ELSE 'Large'
    END AS project_size
FROM projects
ORDER BY budget;

-- Exercise 40: Window Function
SELECT 
    project_name,
    company_id,
    budget,
    RANK() OVER (PARTITION BY company_id ORDER BY budget DESC) AS budget_rank
FROM projects
ORDER BY company_id, budget_rank;

-- Exercise 41: Date Calculations
SELECT 
    project_name,
    start_date,
    end_date,
    DATEDIFF(end_date, start_date) AS duration_days,
    ROUND(DATEDIFF(end_date, start_date) / 30.0, 1) AS duration_months
FROM projects;

-- Exercise 42: String Functions
SELECT 
    first_name || ' ' || last_name AS full_name,
    email,
    SUBSTRING_INDEX(email, '@', -1) AS email_domain
FROM engineers;

-- Exercise 43: Set Operations - UNION
SELECT engineer_id AS resource_id, first_name || ' ' || last_name AS resource_name, 'Engineer' AS type
FROM engineers
WHERE specialization = 'Data Engineering'
UNION
SELECT project_id, project_name, 'High-Value Project'
FROM projects
WHERE budget > 600000;

-- Exercise 44: Set Operations - INTERSECT
SELECT engineer_id, first_name, last_name
FROM engineers
WHERE years_experience > 7
INTERSECT
SELECT engineer_id, first_name, last_name
FROM engineers
WHERE hourly_rate > 150;

-- Exercise 45: Set Operations - EXCEPT
SELECT DISTINCT c.company_id, c.company_name
FROM companies c
INNER JOIN projects p ON c.company_id = p.company_id
EXCEPT
SELECT DISTINCT c.company_id, c.company_name
FROM companies c
INNER JOIN projects p ON c.company_id = p.company_id
INNER JOIN project_milestones pm ON p.project_id = pm.project_id
WHERE pm.status = 'Completed';

-- =========================================================
-- PART 10: OPTIMIZATION AND BEST PRACTICES (Day 3)
-- =========================================================

-- Exercise 46: Check Table Statistics
DESCRIBE DETAIL projects;

-- Exercise 47: Partition Information
SHOW PARTITIONS project_assignments;

-- Exercise 48: Vacuum Simulation
-- VACUUM projects DRY RUN;
-- Note: Commented out as it requires appropriate permissions

-- Exercise 49: Table Properties
ALTER TABLE projects SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Exercise 50: Performance Query
WITH 
    company_metrics AS (
        SELECT 
            c.company_id,
            c.company_name,
            COUNT(p.project_id) AS project_count,
            SUM(p.budget) AS total_budget,
            AVG(DATEDIFF(p.end_date, p.start_date)) AS avg_duration_days,
            COUNT(DISTINCT pa.engineer_id) AS unique_engineers
        FROM companies c
        INNER JOIN projects p ON c.company_id = p.company_id
        LEFT JOIN project_assignments pa ON p.project_id = pa.project_id
        GROUP BY c.company_id, c.company_name
    )
SELECT *
FROM company_metrics
ORDER BY total_budget DESC
LIMIT 5;

-- =========================================================
-- BONUS SOLUTIONS
-- =========================================================

-- Bonus 1: Resource Utilization Report
WITH engineer_metrics AS (
    SELECT 
        e.engineer_id,
        e.first_name || ' ' || e.last_name AS engineer_name,
        e.specialization,
        e.hourly_rate,
        COALESCE(SUM(pa.hours_allocated), 0) AS total_hours,
        COALESCE(SUM(pa.hours_allocated * e.hourly_rate), 0) AS total_revenue,
        COUNT(DISTINCT pa.project_id) AS project_count,
        ROUND(COALESCE(SUM(pa.hours_allocated), 0) / 160.0 * 100, 2) AS utilization_pct
    FROM engineers e
    LEFT JOIN project_assignments pa ON e.engineer_id = pa.engineer_id
    GROUP BY e.engineer_id, e.first_name, e.last_name, e.specialization, e.hourly_rate
)
SELECT 
    *,
    RANK() OVER (PARTITION BY specialization ORDER BY total_revenue DESC) AS revenue_rank
FROM engineer_metrics
ORDER BY specialization, revenue_rank;

-- Bonus 2: Project Health Dashboard
WITH 
    project_costs AS (
        SELECT 
            p.project_id,
            SUM(pa.hours_allocated * e.hourly_rate) AS actual_cost
        FROM projects p
        LEFT JOIN project_assignments pa ON p.project_id = pa.project_id
        LEFT JOIN engineers e ON pa.engineer_id = e.engineer_id
        GROUP BY p.project_id
    ),
    milestone_progress AS (
        SELECT 
            project_id,
            COUNT(*) AS total_milestones,
            SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) AS completed_milestones
        FROM project_milestones
        GROUP BY project_id
    )
SELECT 
    p.project_name,
    c.company_name,
    p.status,
    p.budget,
    COALESCE(pc.actual_cost, 0) AS actual_cost,
    ROUND((COALESCE(pc.actual_cost, 0) / p.budget * 100), 2) AS budget_used_pct,
    COALESCE(mp.completed_milestones, 0) AS completed_milestones,
    COALESCE(mp.total_milestones, 0) AS total_milestones,
    ROUND((COALESCE(mp.completed_milestones, 0) * 100.0 / NULLIF(mp.total_milestones, 0)), 2) AS completion_pct,
    DATEDIFF(p.end_date, CURRENT_DATE()) AS days_remaining,
    CASE 
        WHEN COALESCE(pc.actual_cost, 0) > p.budget * 1.1 THEN 'Red'
        WHEN COALESCE(pc.actual_cost, 0) > p.budget * 0.9 THEN 'Yellow'
        ELSE 'Green'
    END AS risk_level
FROM projects p
INNER JOIN companies c ON p.company_id = c.company_id
LEFT JOIN project_costs pc ON p.project_id = pc.project_id
LEFT JOIN milestone_progress mp ON p.project_id = mp.project_id
ORDER BY risk_level DESC, days_remaining;

-- Bonus 3: Company Performance Analysis
WITH company_performance AS (
    SELECT 
        c.company_id,
        c.company_name,
        c.industry,
        SUM(p.budget) AS total_investment,
        COUNT(CASE WHEN p.status = 'In Progress' THEN 1 END) AS active_projects,
        COUNT(CASE WHEN p.status = 'Completed' THEN 1 END) AS completed_projects,
        AVG(DATEDIFF(p.end_date, p.start_date)) AS avg_project_duration,
        SUM(pa.hours_allocated) AS total_engineer_hours,
        ROUND(COUNT(CASE WHEN p.status = 'Completed' THEN 1 END) * 100.0 / COUNT(p.project_id), 2) AS completion_rate
    FROM companies c
    LEFT JOIN projects p ON c.company_id = p.company_id
    LEFT JOIN project_assignments pa ON p.project_id = pa.project_id
    GROUP BY c.company_id, c.company_name, c.industry
)
SELECT *
FROM company_performance
ORDER BY total_investment DESC;

-- Bonus 4: Engineer Skill Matrix
SELECT 
    e.specialization,
    COUNT(e.engineer_id) AS engineer_count,
    ROUND(AVG(e.years_experience), 1) AS avg_experience,
    ROUND(AVG(e.hourly_rate), 2) AS avg_hourly_rate,
    COALESCE(SUM(pa.hours_allocated), 0) AS total_hours_allocated,
    ROUND(COALESCE(SUM(pa.hours_allocated), 0) / COUNT(e.engineer_id), 2) AS hours_per_engineer,
    CASE 
        WHEN COALESCE(SUM(pa.hours_allocated), 0) / COUNT(e.engineer_id) > 150 THEN 'High Demand'
        WHEN COALESCE(SUM(pa.hours_allocated), 0) / COUNT(e.engineer_id) > 100 THEN 'Medium Demand'
        ELSE 'Low Demand'
    END AS demand_indicator
FROM engineers e
LEFT JOIN project_assignments pa ON e.engineer_id = pa.engineer_id
GROUP BY e.specialization
ORDER BY total_hours_allocated DESC;

-- Bonus 5: Time Series Analysis
WITH monthly_projects AS (
    SELECT 
        DATE_FORMAT(start_date, 'yyyy-MM') AS month_year,
        COUNT(*) AS projects_started,
        SUM(budget) AS budget_allocated
    FROM projects
    GROUP BY DATE_FORMAT(start_date, 'yyyy-MM')
)
SELECT 
    month_year,
    projects_started,
    budget_allocated,
    SUM(projects_started) OVER (ORDER BY month_year) AS running_total_projects,
    CASE 
        WHEN projects_started > LAG(projects_started) OVER (ORDER BY month_year) THEN 'Increasing'
        WHEN projects_started < LAG(projects_started) OVER (ORDER BY month_year) THEN 'Decreasing'
        ELSE 'Stable'
    END AS trend
FROM monthly_projects
ORDER BY month_year;

-- =========================================================
-- WEEK 1 SUMMARY
-- =========================================================
%md
## Week 1 Completion Summary

### Key Achievements
✅ Mastered Databricks architecture (control vs data plane)
✅ Configured and optimized clusters
✅ Leveraged Delta Lake features (ACID, Time Travel, OPTIMIZE)
✅ Performed DBFS file operations
✅ Created and managed tables (managed vs external)
✅ Wrote complex SQL queries with JOINs and CTEs
✅ Applied optimization techniques

### Skills Demonstrated
- Database and table management
- Delta Lake time travel and optimization
- Complex multi-table JOINs
- Aggregations and window functions
- CTEs for query organization
- View creation and management
- Performance optimization

### Next Steps
- Complete the 50-question review quiz
- Review any challenging concepts
- Prepare for Week 2: Advanced SQL and Python DataFrames

**Congratulations on completing Week 1! 🎉**

