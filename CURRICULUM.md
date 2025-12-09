# Databricks Data Engineer Associate - 30 Day Curriculum

## Quick Reference

| Week | Focus Area | Days | Hours | Key Topics |
|------|-----------|------|-------|------------|
| 1 | Databricks Lakehouse Platform | 1-7 | 12h | Architecture, Clusters, Delta Lake, DBFS |
| 2 | ELT with Spark SQL & Python | 8-15 | 16h | SQL, DataFrames, Ingestion, Transformation |
| 3 | Incremental Data Processing | 16-22 | 13.5h | Streaming, MERGE, CDC, Multi-hop |
| 4 | Production Pipelines & Exam Prep | 23-30 | 13.5h | Jobs, Unity Catalog, Practice Exams |

**Total**: 55 hours over 30 days (1.5-2 hours/day)

---

## Detailed Daily Breakdown

### Week 1: Databricks Lakehouse Platform (12 hours)

| Day | Topic | Time | Hands-On Focus |
|-----|-------|------|----------------|
| 1 | Databricks Architecture & Workspace | 1.5h | Navigate UI, create notebooks |
| 2 | Clusters & Compute | 2h | Create clusters, configure autoscaling |
| 3 | Delta Lake Fundamentals | 2h | Create Delta tables, time travel |
| 4 | Databricks File System (DBFS) | 1.5h | Upload files, use dbutils |
| 5 | Databases, Tables, and Views | 2h | Create managed/external tables |
| 6 | Spark SQL Basics | 2h | Write 20+ SQL queries |
| 7 | Review & Mini-Project | 1.5h | Build basic lakehouse |

**Exam Weight**: 15% (Databricks Lakehouse Platform)

---

### Week 2: ELT with Spark SQL & Python (16 hours)

| Day | Topic | Time | Hands-On Focus |
|-----|-------|------|----------------|
| 8 | Advanced SQL - Window Functions | 2h | Ranking, running totals |
| 9 | Advanced SQL - Higher-Order Functions | 2h | TRANSFORM, FILTER, EXPLODE |
| 10 | Python DataFrames Basics | 2h | Read/write data, transformations |
| 11 | Python DataFrames Advanced | 2h | Joins, window functions, UDFs |
| 12 | Data Ingestion Patterns | 2h | COPY INTO, Auto Loader |
| 13 | Data Transformation Patterns | 2h | Cleaning, type conversions |
| 14 | ELT Best Practices | 2h | Medallion architecture |
| 15 | Review & ELT Project | 2h | Build complete ELT pipeline |

**Exam Weight**: 30% (ELT with Spark SQL and Python)

---

### Week 3: Incremental Data Processing (13.5 hours)

| Day | Topic | Time | Hands-On Focus |
|-----|-------|------|----------------|
| 16 | Structured Streaming Basics | 2h | Create streaming queries |
| 17 | Streaming Transformations | 1.5h | Watermarking, windowing |
| 18 | Delta Lake Merge (UPSERT) | 2h | Implement MERGE logic |
| 19 | Change Data Capture (CDC) | 1.5h | Process CDC events, SCD Type 2 |
| 20 | Multi-Hop Architecture | 2h | Bronze → Silver → Gold |
| 21 | Optimization Techniques | 2h | Partitioning, Z-ordering, OPTIMIZE |
| 22 | Review & Incremental Project | 1.5h | Build incremental pipeline |

**Exam Weight**: 25% (Incremental Data Processing)

---

### Week 4: Production Pipelines & Exam Prep (13.5 hours)

| Day | Topic | Time | Activity |
|-----|-------|------|----------|
| 23 | Databricks Jobs | 2h | Create multi-task jobs |
| 24 | Databricks Repos & Version Control | 1.5h | Git integration |
| 25 | Unity Catalog Basics | 2h | Catalogs, schemas, permissions |
| 26 | Data Quality & Testing | 1.5h | Quality checks, testing |
| 27 | Production Best Practices | 1.5h | Error handling, monitoring |
| 28 | Practice Exam 1 | 1.5h | Full-length practice exam |
| 29 | Practice Exam 2 & Review | 1.5h | Second exam + review |
| 30 | Final Review & Exam | 1.5h | Take certification exam |

**Exam Weight**: 20% (Production Pipelines) + 10% (Unity Catalog)

---

## Exam Topic Coverage

### Domain 1: Databricks Lakehouse Platform (15%)
- **Days 1-5**: Architecture, clusters, Delta Lake, DBFS, tables
- **Practice**: 10+ hands-on exercises
- **Project**: Basic lakehouse setup

### Domain 2: ELT with Spark SQL and Python (30%)
- **Days 6-14**: SQL (basic + advanced), Python DataFrames, ingestion, transformation
- **Practice**: 50+ SQL queries, 20+ Python scripts
- **Project**: Complete ELT pipeline

### Domain 3: Incremental Data Processing (25%)
- **Days 16-21**: Streaming, MERGE, CDC, multi-hop, optimization
- **Practice**: 3 streaming pipelines
- **Project**: Incremental processing pipeline

### Domain 4: Production Pipelines (20%)
- **Days 23-27**: Jobs, repos, Unity Catalog, quality, best practices
- **Practice**: Multi-task job creation
- **Deliverable**: Production-ready pipeline

### Domain 5: Data Governance (10%)
- **Day 25**: Unity Catalog basics
- **Integrated**: Security and governance throughout

---

## Key Concepts to Master

### Delta Lake
- ACID transactions
- Time travel (VERSION AS OF, TIMESTAMP AS OF)
- Schema evolution
- OPTIMIZE and VACUUM
- Change Data Feed

### Structured Streaming
- readStream and writeStream
- Output modes (append, complete, update)
- Checkpointing
- Watermarking
- Windowing operations

### Data Ingestion
- COPY INTO (batch)
- Auto Loader (streaming)
- Schema inference and evolution
- File format options

### Transformations
- SQL: Window functions, higher-order functions
- Python: DataFrame API, UDFs
- Data cleaning and validation

### Optimization
- Partitioning strategies
- Z-ordering
- File compaction (OPTIMIZE)
- Data skipping
- VACUUM for cleanup

### Production
- Databricks Jobs
- Task orchestration
- Git integration (Repos)
- Unity Catalog permissions
- Error handling and monitoring

---

## SQL vs. Python Coverage

### When to Use SQL (60% of exam)
- Data querying and analysis
- Table creation and management
- MERGE operations
- Window functions
- Set operations

### When to Use Python (40% of exam)
- Complex transformations
- UDFs
- Streaming operations
- DataFrame API
- Integration with libraries

**Both are tested** - Be proficient in both!

---

## Common Exam Question Patterns

### Pattern 1: Choose the Best Approach
"What is the most efficient way to..."
- Know trade-offs between approaches
- Understand performance implications

### Pattern 2: Identify the Error
"What is wrong with this code?"
- Syntax errors
- Logic errors
- Performance issues

### Pattern 3: Complete the Code
"Fill in the blank to..."
- Know exact syntax
- Understand parameters

### Pattern 4: Conceptual Understanding
"Which statement is true about..."
- Understand concepts deeply
- Know limitations and constraints

---

## Study Schedule Options

### Option 1: Consistent Daily (Recommended)
- 1.5-2 hours every day for 30 days
- Best for retention
- Steady progress

### Option 2: Weekday Intensive
- 2.5 hours Mon-Fri (50 hours)
- 2.5 hours on 2 weekends (5 hours)
- Faster completion (4 weeks)

### Option 3: Weekend Warrior
- 5 hours every Saturday & Sunday
- 6 weekends = 60 hours
- Slower but flexible

---

## Progress Tracking

### Week 1 Checklist
- [ ] Created Databricks account
- [ ] Configured clusters
- [ ] Created Delta tables
- [ ] Understand Delta Lake basics
- [ ] Can write basic SQL queries

### Week 2 Checklist
- [ ] Mastered window functions
- [ ] Proficient with DataFrame API
- [ ] Built ELT pipeline
- [ ] Understand medallion architecture
- [ ] Can use COPY INTO and Auto Loader

### Week 3 Checklist
- [ ] Created streaming queries
- [ ] Implemented MERGE operations
- [ ] Built multi-hop pipeline
- [ ] Applied optimization techniques
- [ ] Understand CDC patterns

### Week 4 Checklist
- [ ] Created Databricks Jobs
- [ ] Used Git integration
- [ ] Understand Unity Catalog
- [ ] Scored 80%+ on practice exams
- [ ] Ready for exam

---

## Exam Day Preparation

### Before Exam
- [ ] Good night's sleep (8 hours)
- [ ] Light meal
- [ ] Quiet environment
- [ ] Stable internet connection
- [ ] ID ready
- [ ] Arrive 15 minutes early

### During Exam
- [ ] Read questions carefully
- [ ] Flag difficult questions
- [ ] Manage time (2 min/question)
- [ ] Don't overthink
- [ ] Review flagged questions

### Exam Format
- 45 multiple choice questions
- 90 minutes (1 hour 30 minutes)
- 70% passing score (32/45 correct)
- No negative marking
- Can flag and review questions

---

## Key Facts to Memorize

### Delta Lake
- Default retention: 30 days for VACUUM
- Time travel: Based on transaction log
- OPTIMIZE: Compacts small files
- Z-order: Max 4 columns

### Structured Streaming
- Output modes: append, complete, update
- Checkpoint: Required for fault tolerance
- Watermark: For handling late data
- Trigger: once, continuous, processingTime

### File Formats
- Delta: ACID, time travel, schema evolution
- Parquet: Columnar, compressed
- CSV: Row-based, human-readable
- JSON: Semi-structured, flexible

### Cluster Types
- All-purpose: Interactive workloads
- Job: Automated workloads
- SQL Warehouse: SQL analytics

### Unity Catalog Hierarchy
- Metastore → Catalog → Schema → Table

---

## Post-Exam Next Steps

### If You Pass ✅
1. Update LinkedIn with certification
2. Add to resume
3. Build Project D2 (Delta Live Tables)
4. Start studying for Professional certification
5. Write blog post about exam experience

### If You Don't Pass ❌
1. Review exam report (topic breakdown)
2. Focus on weak areas
3. Retake after 14 days
4. Additional study: 10-20 hours on weak topics
5. Take more practice exams

---

## Success Rate Prediction

**If you complete all 30 days**: 80-85% pass rate  
**If you skip hands-on**: 40-50% pass rate  
**If you only do practice exams**: 30-40% pass rate

**Key Success Factors**:
1. Hands-on practice (most important)
2. Both SQL and Python proficiency
3. Understanding Delta Lake deeply
4. Practice exams (identify weak areas)
5. Consistent daily study

---

## Comparison: Associate vs. Professional

| Aspect | Associate | Professional |
|--------|-----------|--------------|
| **Difficulty** | Moderate | Hard |
| **Study Time** | 45-60 hours | 60-70 hours |
| **Focus** | Fundamentals | Advanced + Production |
| **Hands-On** | Basic | Extensive |
| **Prerequisites** | None | Associate required |

**After Associate**: You'll be 50% ready for Professional

Good luck! 🚀
