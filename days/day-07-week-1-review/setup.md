# Day 7 Setup: Week 1 Review & Mini-Project

## Prerequisites

- Completed Days 1-6
- Active Databricks cluster
- Workspace access
- All previous day databases still available (optional but helpful)

## Setup Steps

### Step 1: Create Review Workspace

1. Navigate to your Workspace
2. Go to your `30-days-databricks` folder
3. Create a new notebook: `day-07-week1-review`
4. Set default language to **SQL**

### Step 2: Create Review Database

```sql
-- Create database for Week 1 review project
CREATE DATABASE IF NOT EXISTS week1_review;
USE week1_review;
```

### Step 3: Create Comprehensive Dataset

We'll create a realistic data engineering scenario with multiple related tables.

**Companies Table** (External):
```sql
-- Create external table for companies
CREATE TABLE IF NOT EXISTS companies (
    company_id INT,
    company_name STRING,
    industry STRING,
    founded_year INT,
    headquarters STRING,
    employee_count INT
) USING DELTA
LOCATION '/FileStore/week1_review/companies';

INSERT INTO companies VALUES
    (1, 'TechCorp', 'Technology', 2010, 'San Francisco', 500),
    (2, 'DataSystems', 'Technology', 2015, 'New York', 250),
    (3, 'CloudServices', 'Technology', 2018, 'Seattle', 150),
    (4, 'FinanceHub', 'Finance', 2005, 'Chicago', 800),
    (5, 'RetailGiant', 'Retail', 2000, 'Los Angeles', 2000),
    (6, 'HealthTech', 'Healthcare', 2012, 'Boston', 400),
    (7, 'EduPlatform', 'Education', 2016, 'Austin', 180),
    (8, 'MediaStream', 'Media', 2019, 'Miami', 120),
    (9, 'AutoDrive', 'Automotive', 2017, 'Detroit', 600),
    (10, 'EnergyPlus', 'Energy', 2008, 'Houston', 450);
```

**Projects Table** (Managed):
```sql
-- Create managed table for projects
CREATE TABLE IF NOT EXISTS projects (
    project_id INT,
    project_name STRING,
    company_id INT,
    start_date DATE,
    end_date DATE,
    budget DECIMAL(12,2),
    status STRING
) USING DELTA;

INSERT INTO projects VALUES
    (101, 'Data Warehouse Migration', 1, '2024-01-01', '2024-06-30', 500000.00, 'In Progress'),
    (102, 'ML Pipeline Development', 1, '2024-02-01', '2024-08-31', 750000.00, 'In Progress'),
    (103, 'Customer Analytics Platform', 2, '2024-01-15', '2024-07-15', 400000.00, 'In Progress'),
    (104, 'Real-time Dashboard', 2, '2023-10-01', '2024-03-31', 300000.00, 'Completed'),
    (105, 'Cloud Infrastructure Setup', 3, '2024-03-01', '2024-09-30', 600000.00, 'In Progress'),
    (106, 'Data Lake Implementation', 4, '2023-11-01', '2024-05-31', 850000.00, 'In Progress'),
    (107, 'ETL Pipeline Modernization', 4, '2024-01-01', '2024-04-30', 450000.00, 'In Progress'),
    (108, 'Inventory Optimization', 5, '2024-02-15', '2024-08-15', 550000.00, 'In Progress'),
    (109, 'Patient Data Platform', 6, '2023-12-01', '2024-06-30', 700000.00, 'In Progress'),
    (110, 'Learning Analytics', 7, '2024-01-10', '2024-07-10', 350000.00, 'In Progress'),
    (111, 'Content Recommendation Engine', 8, '2024-02-01', '2024-05-31', 280000.00, 'In Progress'),
    (112, 'Autonomous Vehicle Data Pipeline', 9, '2023-09-01', '2024-12-31', 1200000.00, 'In Progress'),
    (113, 'Smart Grid Analytics', 10, '2024-01-01', '2024-10-31', 900000.00, 'In Progress'),
    (114, 'Mobile App Backend', 1, '2023-08-01', '2023-12-31', 400000.00, 'Completed'),
    (115, 'API Gateway', 3, '2023-07-01', '2023-11-30', 250000.00, 'Completed');
```

**Engineers Table** (Managed):
```sql
-- Create managed table for engineers
CREATE TABLE IF NOT EXISTS engineers (
    engineer_id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    hire_date DATE,
    specialization STRING,
    years_experience INT,
    hourly_rate DECIMAL(8,2)
) USING DELTA;

INSERT INTO engineers VALUES
    (1001, 'Alice', 'Johnson', 'alice.j@email.com', '2020-01-15', 'Data Engineering', 8, 150.00),
    (1002, 'Bob', 'Smith', 'bob.s@email.com', '2019-03-20', 'Data Engineering', 10, 175.00),
    (1003, 'Carol', 'Williams', 'carol.w@email.com', '2021-06-10', 'ML Engineering', 5, 140.00),
    (1004, 'David', 'Brown', 'david.b@email.com', '2018-09-05', 'Data Engineering', 12, 200.00),
    (1005, 'Emma', 'Davis', 'emma.d@email.com', '2022-01-12', 'Analytics Engineering', 3, 120.00),
    (1006, 'Frank', 'Miller', 'frank.m@email.com', '2020-07-22', 'Data Engineering', 7, 145.00),
    (1007, 'Grace', 'Wilson', 'grace.w@email.com', '2021-11-18', 'ML Engineering', 4, 135.00),
    (1008, 'Henry', 'Moore', 'henry.m@email.com', '2019-05-30', 'Data Engineering', 9, 165.00),
    (1009, 'Iris', 'Taylor', 'iris.t@email.com', '2022-03-15', 'Analytics Engineering', 2, 110.00),
    (1010, 'Jack', 'Anderson', 'jack.a@email.com', '2020-10-08', 'Data Engineering', 6, 140.00),
    (1011, 'Kate', 'Thomas', 'kate.t@email.com', '2021-02-25', 'ML Engineering', 5, 138.00),
    (1012, 'Leo', 'Jackson', 'leo.j@email.com', '2019-08-14', 'Data Engineering', 11, 180.00),
    (1013, 'Mia', 'White', 'mia.w@email.com', '2022-05-20', 'Analytics Engineering', 3, 115.00),
    (1014, 'Noah', 'Harris', 'noah.h@email.com', '2020-12-03', 'Data Engineering', 7, 148.00),
    (1015, 'Olivia', 'Martin', 'olivia.m@email.com', '2021-09-17', 'ML Engineering', 4, 132.00);
```

**Project Assignments Table** (Managed, Partitioned):
```sql
-- Create partitioned table for project assignments
CREATE TABLE IF NOT EXISTS project_assignments (
    assignment_id INT,
    project_id INT,
    engineer_id INT,
    hours_allocated INT,
    assignment_date DATE
) USING DELTA
PARTITIONED BY (assignment_date);

INSERT INTO project_assignments VALUES
    (1, 101, 1001, 160, '2024-01-01'),
    (2, 101, 1002, 160, '2024-01-01'),
    (3, 102, 1003, 160, '2024-02-01'),
    (4, 102, 1004, 120, '2024-02-01'),
    (5, 103, 1005, 160, '2024-01-15'),
    (6, 103, 1006, 160, '2024-01-15'),
    (7, 104, 1007, 160, '2023-10-01'),
    (8, 105, 1008, 160, '2024-03-01'),
    (9, 105, 1009, 80, '2024-03-01'),
    (10, 106, 1010, 160, '2023-11-01'),
    (11, 106, 1011, 160, '2023-11-01'),
    (12, 107, 1012, 160, '2024-01-01'),
    (13, 108, 1013, 160, '2024-02-15'),
    (14, 109, 1014, 160, '2023-12-01'),
    (15, 110, 1015, 160, '2024-01-10'),
    (16, 111, 1001, 80, '2024-02-01'),
    (17, 112, 1002, 160, '2023-09-01'),
    (18, 113, 1004, 160, '2024-01-01'),
    (19, 114, 1006, 160, '2023-08-01'),
    (20, 115, 1008, 160, '2023-07-01');
```

**Project Milestones Table** (Managed):
```sql
-- Create table for project milestones
CREATE TABLE IF NOT EXISTS project_milestones (
    milestone_id INT,
    project_id INT,
    milestone_name STRING,
    due_date DATE,
    completed_date DATE,
    status STRING
) USING DELTA;

INSERT INTO project_milestones VALUES
    (1, 101, 'Requirements Gathering', '2024-01-31', '2024-01-28', 'Completed'),
    (2, 101, 'Architecture Design', '2024-02-29', '2024-03-05', 'Completed'),
    (3, 101, 'Implementation Phase 1', '2024-04-30', NULL, 'In Progress'),
    (4, 102, 'Data Collection', '2024-03-01', '2024-02-28', 'Completed'),
    (5, 102, 'Model Development', '2024-06-30', NULL, 'In Progress'),
    (6, 103, 'Platform Setup', '2024-02-15', '2024-02-10', 'Completed'),
    (7, 103, 'Dashboard Development', '2024-05-15', NULL, 'In Progress'),
    (8, 104, 'Final Deployment', '2024-03-31', '2024-03-28', 'Completed'),
    (9, 105, 'Infrastructure Planning', '2024-04-01', NULL, 'In Progress'),
    (10, 106, 'Data Lake Setup', '2024-03-31', NULL, 'In Progress');
```

### Step 4: Verify Setup

```sql
-- Check all tables exist
SHOW TABLES IN week1_review;

-- Verify row counts
SELECT 'companies' AS table_name, COUNT(*) AS row_count FROM companies
UNION ALL
SELECT 'projects', COUNT(*) FROM projects
UNION ALL
SELECT 'engineers', COUNT(*) FROM engineers
UNION ALL
SELECT 'project_assignments', COUNT(*) FROM project_assignments
UNION ALL
SELECT 'project_milestones', COUNT(*) FROM project_milestones;

-- Expected output:
-- companies: 10
-- projects: 15
-- engineers: 15
-- project_assignments: 20
-- project_milestones: 10
```

### Step 5: Verify Table Types

```sql
-- Check which tables are managed vs external
DESCRIBE EXTENDED companies;
-- Should show external location

DESCRIBE EXTENDED projects;
-- Should show managed location in warehouse

-- Verify partitioning
SHOW PARTITIONS project_assignments;
```

### Step 6: Create Views for Review

```sql
-- Standard view: Active projects
CREATE OR REPLACE VIEW active_projects AS
SELECT 
    p.project_id,
    p.project_name,
    c.company_name,
    p.budget,
    p.start_date,
    p.end_date
FROM projects p
INNER JOIN companies c ON p.company_id = c.company_id
WHERE p.status = 'In Progress';

-- Temporary view: Engineer workload
CREATE OR REPLACE TEMP VIEW engineer_workload AS
SELECT 
    e.engineer_id,
    e.first_name || ' ' || e.last_name AS engineer_name,
    e.specialization,
    SUM(pa.hours_allocated) AS total_hours
FROM engineers e
LEFT JOIN project_assignments pa ON e.engineer_id = pa.engineer_id
GROUP BY e.engineer_id, e.first_name, e.last_name, e.specialization;

-- Verify views
SELECT * FROM active_projects LIMIT 5;
SELECT * FROM engineer_workload LIMIT 5;
```

## Data Model

```
companies (External)
└── company_id (PK)

projects (Managed)
├── project_id (PK)
└── company_id (FK → companies.company_id)

engineers (Managed)
└── engineer_id (PK)

project_assignments (Managed, Partitioned by assignment_date)
├── assignment_id (PK)
├── project_id (FK → projects.project_id)
└── engineer_id (FK → engineers.engineer_id)

project_milestones (Managed)
├── milestone_id (PK)
└── project_id (FK → projects.project_id)
```

## Verification Checklist

- [ ] Database `week1_review` created
- [ ] All 5 tables created successfully
- [ ] Companies table is external
- [ ] Projects table is managed
- [ ] Project_assignments table is partitioned
- [ ] All data inserted correctly
- [ ] Views created successfully
- [ ] Can query all tables

## Troubleshooting

### Issue: "Database already exists"
**Solution**: 
```sql
DROP DATABASE IF EXISTS week1_review CASCADE;
CREATE DATABASE week1_review;
```

### Issue: "Cannot create external table"
**Solution**: Ensure the location path is accessible:
```sql
-- Check if path exists
%fs ls /FileStore/week1_review/

-- If not, create it
%fs mkdirs /FileStore/week1_review/
```

### Issue: "Partition not showing"
**Solution**: Insert data first, then check:
```sql
SHOW PARTITIONS project_assignments;
```

### Issue: "View not found"
**Solution**: Ensure you're in the correct database:
```sql
USE week1_review;
SHOW VIEWS;
```

## Quick Reference Commands

```sql
-- Switch to review database
USE week1_review;

-- List all tables
SHOW TABLES;

-- Check table type
DESCRIBE EXTENDED table_name;

-- View table history (Delta Lake)
DESCRIBE HISTORY projects;

-- Check partitions
SHOW PARTITIONS project_assignments;

-- View table details
DESCRIBE DETAIL projects;

-- List views
SHOW VIEWS;
```

## What's Next?

You're ready for the Week 1 review exercises and quiz!

**Activities**:
1. Complete the mini-project in `exercise.sql`
2. Take the 50-question review quiz in `quiz.md`
3. Review any topics where you struggled
4. Celebrate completing Week 1! 🎉

**Mini-Project Topics**:
- Creating and managing databases
- Working with managed and external tables
- Using Delta Lake features (Time Travel, OPTIMIZE)
- Complex SQL queries with multiple JOINs
- CTEs for complex analysis
- Aggregations and window functions
- Partitioning strategies

Good luck! 🚀

