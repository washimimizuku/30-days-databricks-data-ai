# Day 7: Week 1 Review & Mini-Project

## Overview

Congratulations on completing Week 1! Today is dedicated to reviewing and consolidating everything you've learned about the Databricks Lakehouse Platform. This review day includes:

- Comprehensive review of Days 1-6
- 50-question quiz covering all Week 1 topics
- Hands-on mini-project integrating multiple concepts
- Exam preparation tips

## Learning Objectives

By the end of today, you will:
- Consolidate knowledge from Days 1-6
- Identify areas needing additional review
- Apply multiple concepts in an integrated project
- Practice exam-style questions
- Build confidence for Week 2

## Week 1 Topics Recap

### Day 1: Databricks Architecture & Workspace
**Key Concepts**:
- Control Plane vs Data Plane
- Workspace organization
- Notebooks and magic commands
- Community Edition limitations

**Critical for Exam**:
- Understand the separation of control and data planes
- Know what runs where (UI in control plane, clusters in data plane)
- Recognize magic commands (%md, %sql, %python, %scala, %r, %sh, %fs)

### Day 2: Clusters & Compute
**Key Concepts**:
- Cluster types (All-Purpose vs Job)
- Cluster modes (Standard, High Concurrency, Single Node)
- Autoscaling and auto-termination
- DBUs and cost optimization
- Cluster policies

**Critical for Exam**:
- Know when to use All-Purpose vs Job clusters
- Understand cluster mode differences
- Know cost optimization strategies
- Recognize cluster configuration options

### Day 3: Delta Lake Fundamentals
**Key Concepts**:
- ACID transactions
- Time Travel (VERSION AS OF, TIMESTAMP AS OF)
- Transaction log (_delta_log)
- DESCRIBE HISTORY
- OPTIMIZE and VACUUM
- Z-ORDER BY

**Critical for Exam**:
- Understand ACID properties
- Know Time Travel syntax
- Understand VACUUM retention period (default 7 days)
- Know when to use OPTIMIZE and Z-ORDER

### Day 4: Databricks File System (DBFS)
**Key Concepts**:
- DBFS structure and paths
- dbutils.fs commands
- File operations (ls, cp, mv, rm, mkdirs)
- Reading and writing files
- FileStore for web-accessible files

**Critical for Exam**:
- Know DBFS path formats (/dbfs/, dbfs:/)
- Understand dbutils.fs commands
- Know FileStore usage
- Recognize file operation patterns

### Day 5: Databases, Tables, and Views
**Key Concepts**:
- Managed vs External tables
- DROP behavior differences
- View types (Standard, Temp, Global Temp)
- Table metadata commands
- Partitioning
- Default table location (/user/hive/warehouse/)

**Critical for Exam**:
- **MOST IMPORTANT**: Managed vs External table differences
- Know DROP behavior for each table type
- Understand view scopes
- Know default storage locations
- Understand partitioning benefits

### Day 6: Spark SQL Basics
**Key Concepts**:
- SELECT, WHERE, ORDER BY, LIMIT
- JOIN types (INNER, LEFT, RIGHT, FULL, SELF)
- GROUP BY and HAVING
- Subqueries (scalar, column, table, correlated)
- CTEs (Common Table Expressions)
- Set operations (UNION, INTERSECT, EXCEPT)
- SQL execution order

**Critical for Exam**:
- Know all JOIN types and when to use each
- Understand WHERE vs HAVING
- Know UNION vs UNION ALL
- Understand SQL execution order
- Master CTEs for complex queries

## Common Exam Question Patterns

### Pattern 1: Table Type Selection
**Question Type**: "You need to create a table that... What type should you use?"

**Key Decision Factors**:
- Data shared with other systems → External
- Data only for Databricks → Managed
- Want data to persist after DROP → External
- Temporary/development data → Managed

### Pattern 2: Time Travel
**Question Type**: "How do you query data as it was 2 days ago?"

**Answer**: 
```sql
SELECT * FROM table_name VERSION AS OF 5
SELECT * FROM table_name TIMESTAMP AS OF '2024-01-01'
```

### Pattern 3: Cluster Selection
**Question Type**: "What cluster type should you use for..."

**Decision Matrix**:
- Interactive development → All-Purpose
- Scheduled jobs → Job cluster
- Multiple users → High Concurrency
- Single user, cost-sensitive → Single Node

### Pattern 4: JOIN Selection
**Question Type**: "You need all records from table A and matching records from table B..."

**Answer**: LEFT JOIN (all from left + matches from right)

### Pattern 5: Optimization
**Question Type**: "How do you optimize a large table with frequent queries on date column?"

**Answer**: Partition by date, use OPTIMIZE, consider Z-ORDER

## Week 1 Integration: Key Connections

### Connection 1: Architecture → Clusters → Tables
1. **Architecture**: Data plane runs in your cloud
2. **Clusters**: Compute resources in data plane
3. **Tables**: Data stored in your cloud storage

### Connection 2: DBFS → Tables → Delta Lake
1. **DBFS**: File system for storing data
2. **Tables**: Metadata pointing to files in DBFS
3. **Delta Lake**: Format providing ACID transactions

### Connection 3: Databases → Tables → SQL
1. **Databases**: Logical grouping of tables
2. **Tables**: Store data (managed or external)
3. **SQL**: Query language to access data

## Hands-On Mini-Project

See `exercise.sql` for a comprehensive mini-project that integrates:
- Creating databases and tables
- Loading data from DBFS
- Using Delta Lake features
- Complex SQL queries with JOINs and CTEs
- Optimization techniques

## Key Takeaways from Week 1

### Architecture & Platform
1. **Control plane** manages UI/notebooks, **data plane** processes data
2. **Clusters** are compute resources that execute code
3. **Workspaces** organize notebooks and resources

### Data Storage
4. **Delta Lake** provides ACID transactions on data lakes
5. **Managed tables**: Databricks manages data + metadata
6. **External tables**: User manages data, Databricks manages metadata
7. **DBFS** is the distributed file system

### Data Access
8. **SQL** is the primary query language (60% of exam)
9. **JOINs** combine data from multiple tables
10. **CTEs** improve query readability

### Optimization
11. **OPTIMIZE** compacts small files
12. **Z-ORDER** co-locates related data
13. **VACUUM** removes old files (default 7 days)
14. **Partitioning** improves query performance

### Best Practices
15. Use **Delta Lake** for all production tables
16. Use **Job clusters** for scheduled workloads
17. Enable **autoscaling** for variable workloads
18. Use **CTEs** for complex queries
19. **Partition** large tables on frequently filtered columns
20. Use **external tables** for shared data

## Exam Preparation Tips

### High-Priority Topics (Study These First)
1. ✅ **Managed vs External tables** (appears in almost every exam)
2. ✅ **Delta Lake Time Travel** (common scenario questions)
3. ✅ **JOIN types** (multiple questions per exam)
4. ✅ **Cluster types and modes** (configuration questions)
5. ✅ **OPTIMIZE and VACUUM** (optimization questions)

### Medium-Priority Topics
6. View types and scopes
7. DBFS commands and paths
8. SQL execution order
9. WHERE vs HAVING
10. Set operations (UNION, INTERSECT, EXCEPT)

### Lower-Priority Topics (But Still Important)
11. Magic commands
12. Workspace organization
13. String and date functions
14. CASE expressions
15. Subquery types

## Common Mistakes to Avoid

### Mistake 1: Confusing Table Types
❌ **Wrong**: "DROP external table deletes data"
✅ **Right**: "DROP external table deletes only metadata, data remains"

### Mistake 2: Wrong JOIN Type
❌ **Wrong**: Using INNER JOIN when you need all records from one table
✅ **Right**: Use LEFT/RIGHT JOIN to keep all records from one side

### Mistake 3: VACUUM Too Soon
❌ **Wrong**: "VACUUM immediately after OPTIMIZE"
✅ **Right**: "VACUUM respects retention period (default 7 days) for Time Travel"

### Mistake 4: WHERE vs HAVING
❌ **Wrong**: "Use HAVING to filter individual rows"
✅ **Right**: "Use WHERE for rows, HAVING for aggregated groups"

### Mistake 5: Cluster Selection
❌ **Wrong**: "Use All-Purpose cluster for scheduled jobs"
✅ **Right**: "Use Job cluster for scheduled jobs (cheaper, auto-terminates)"

## Study Strategy for Week 2

### Daily Review (15 minutes)
- Review one previous day's README
- Redo 5 quiz questions
- Practice one complex query

### Weekly Review (1 hour)
- Complete practice quizzes
- Build mini-projects
- Review exam tips

### Focus Areas for Week 2
- Advanced SQL (Window Functions, Higher-Order Functions)
- Python DataFrames API
- Data ingestion patterns
- ELT best practices

## Additional Resources

### Databricks Documentation
- [Delta Lake Guide](https://docs.databricks.com/delta/index.html)
- [SQL Reference](https://docs.databricks.com/sql/language-manual/index.html)
- [Cluster Configuration](https://docs.databricks.com/clusters/configure.html)
- [DBFS Guide](https://docs.databricks.com/dbfs/index.html)

### Practice Resources
- Review all Day 1-6 quizzes
- Redo exercises from Days 1-6
- Complete the mini-project in exercise.sql
- Take the 50-question review quiz

## Self-Assessment Checklist

### Architecture & Workspace (Day 1)
- [ ] Can explain control plane vs data plane
- [ ] Know all magic commands
- [ ] Understand workspace organization
- [ ] Can navigate Databricks UI

### Clusters & Compute (Day 2)
- [ ] Know cluster types and when to use each
- [ ] Understand cluster modes
- [ ] Can configure autoscaling
- [ ] Know cost optimization strategies

### Delta Lake (Day 3)
- [ ] Understand ACID properties
- [ ] Can use Time Travel
- [ ] Know OPTIMIZE and VACUUM
- [ ] Understand transaction log

### DBFS (Day 4)
- [ ] Know DBFS path formats
- [ ] Can use dbutils.fs commands
- [ ] Understand file operations
- [ ] Know FileStore usage

### Tables & Views (Day 5)
- [ ] Know managed vs external differences
- [ ] Understand DROP behavior
- [ ] Know view types and scopes
- [ ] Can create partitioned tables

### SQL Basics (Day 6)
- [ ] Master all JOIN types
- [ ] Understand WHERE vs HAVING
- [ ] Can write CTEs
- [ ] Know set operations

## Confidence Check

Rate your confidence (1-5) on each topic:
- 5: Can teach others
- 4: Very confident
- 3: Somewhat confident
- 2: Need more practice
- 1: Need to review

**Topics to Rate**:
1. Databricks architecture ___
2. Cluster configuration ___
3. Delta Lake features ___
4. DBFS operations ___
5. Table types ___
6. SQL JOINs ___
7. Aggregations ___
8. CTEs ___

**If you rated anything 1-2**: Review that day's materials before Week 2

## Next Steps

### Today
1. Complete the 50-question review quiz
2. Work through the mini-project
3. Review any weak areas
4. Celebrate completing Week 1! 🎉

### Tomorrow (Day 8)
- Start Week 2: ELT with Spark SQL & Python
- Learn Advanced SQL: Window Functions
- Build on Week 1 foundations

### This Week
- Complete Days 8-14
- Master advanced SQL and Python DataFrames
- Build ELT pipelines
- Prepare for Week 2 review

---

**Congratulations on completing Week 1!** You've built a solid foundation in the Databricks Lakehouse Platform. Week 2 will build on these concepts with more advanced SQL and Python techniques.

Tomorrow: [Day 8 - Advanced SQL: Window Functions](../day-08-advanced-sql-window-functions/README.md)

