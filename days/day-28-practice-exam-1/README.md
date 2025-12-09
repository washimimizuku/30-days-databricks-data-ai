# Day 28: Practice Exam 1

**Time**: 90 minutes (1.5 hours)  
**Format**: 45 multiple choice questions  
**Passing Score**: 32/45 (70%)  
**Exam Weight Distribution**: Matches real certification exam

## Overview

Welcome to your first full-length practice exam! This exam simulates the actual Databricks Certified Data Engineer Associate certification exam in format, difficulty, and content distribution.

## Learning Objectives

By the end of today, you will:
- Experience the actual exam format and time pressure
- Identify your strengths and weaknesses across all exam domains
- Practice time management (2 minutes per question)
- Build confidence for the real certification exam
- Understand the types of questions you'll encounter

## Exam Format

### Question Distribution by Domain

The practice exam follows the official exam blueprint:

| Domain | Weight | Questions | Topics |
|--------|--------|-----------|--------|
| **Databricks Lakehouse Platform** | 15% | 7 questions | Architecture, clusters, Delta Lake, DBFS, tables |
| **ELT with Spark SQL and Python** | 30% | 13 questions | SQL, DataFrames, ingestion, transformation |
| **Incremental Data Processing** | 25% | 11 questions | Streaming, MERGE, CDC, multi-hop |
| **Production Pipelines** | 20% | 9 questions | Jobs, repos, monitoring, best practices |
| **Data Governance (Unity Catalog)** | 10% | 5 questions | Catalogs, schemas, permissions |

### Question Types

1. **Choose the Best Approach** (40%)
   - "What is the most efficient way to..."
   - "Which method should you use to..."
   - Tests understanding of trade-offs

2. **Identify the Error** (25%)
   - "What is wrong with this code?"
   - "Why will this query fail?"
   - Tests syntax and logic knowledge

3. **Complete the Code** (20%)
   - "Fill in the blank to..."
   - "Which clause is missing?"
   - Tests exact syntax knowledge

4. **Conceptual Understanding** (15%)
   - "Which statement is true about..."
   - "What happens when..."
   - Tests deep understanding

## How to Take This Exam

### Before You Start

1. **Set aside 90 minutes** of uninterrupted time
2. **Prepare your environment**:
   - Quiet space
   - No distractions
   - Timer ready
   - Notebook for flagging questions
3. **Review exam rules**:
   - No looking at notes or documentation
   - No testing code in Databricks
   - Simulate real exam conditions

### During the Exam

1. **Time Management**:
   - 90 minutes ÷ 45 questions = 2 minutes per question
   - Don't spend more than 3 minutes on any question
   - Flag difficult questions and move on
   - Leave 10 minutes at the end for review

2. **Question Strategy**:
   - Read each question carefully (twice if needed)
   - Eliminate obviously wrong answers first
   - Look for keywords (MOST efficient, BEST practice, etc.)
   - Watch for negative words (NOT, EXCEPT, CANNOT)

3. **Flagging System**:
   - Mark questions you're unsure about
   - Come back to them after completing easier questions
   - Don't change answers unless you're certain

### After the Exam

1. **Score yourself** using the answer key in `quiz.md`
2. **Analyze your results**:
   - Which domains were strongest?
   - Which domains need more study?
   - What types of questions were hardest?
3. **Review incorrect answers** thoroughly
4. **Create a study plan** for weak areas

## Exam Content Coverage

### Domain 1: Databricks Lakehouse Platform (7 questions)

**Topics Tested**:
- Databricks architecture and workspace navigation
- Cluster types and configuration
- Delta Lake ACID properties and time travel
- DBFS file operations
- Managed vs external tables
- Database and schema management

**Key Concepts**:
- Control plane vs data plane
- All-purpose vs job clusters
- Delta Lake transaction log
- DBFS mount points
- Table locations and metadata

### Domain 2: ELT with Spark SQL and Python (13 questions)

**Topics Tested**:
- SQL window functions (RANK, ROW_NUMBER, LAG, LEAD)
- Higher-order functions (TRANSFORM, FILTER, AGGREGATE)
- DataFrame API (select, filter, join, groupBy)
- Data ingestion (COPY INTO, Auto Loader)
- Data transformation patterns
- Medallion architecture (Bronze, Silver, Gold)

**Key Concepts**:
- Window function syntax and use cases
- Array and struct manipulation
- DataFrame vs SQL API equivalence
- Schema inference and evolution
- Data quality patterns

### Domain 3: Incremental Data Processing (11 questions)

**Topics Tested**:
- Structured Streaming basics (readStream, writeStream)
- Output modes (append, complete, update)
- Checkpointing and fault tolerance
- MERGE operations (INSERT, UPDATE, DELETE)
- Change Data Capture (CDC) patterns
- Multi-hop architecture
- Optimization techniques (OPTIMIZE, Z-ORDER, VACUUM)

**Key Concepts**:
- Streaming vs batch processing
- Watermarking for late data
- MERGE WHEN MATCHED/NOT MATCHED
- SCD Type 1 vs Type 2
- File compaction strategies

### Domain 4: Production Pipelines (9 questions)

**Topics Tested**:
- Databricks Jobs and task orchestration
- Git integration with Databricks Repos
- Error handling and retry logic
- Logging and monitoring
- Idempotency patterns
- Deployment strategies

**Key Concepts**:
- Multi-task job dependencies
- Git workflows (branch, commit, merge)
- Try-except patterns
- Job run monitoring
- Production best practices

### Domain 5: Data Governance (5 questions)

**Topics Tested**:
- Unity Catalog three-level namespace
- Catalog and schema management
- Table permissions (SELECT, MODIFY, CREATE)
- Managed vs external tables in Unity Catalog
- System tables and audit logs

**Key Concepts**:
- Metastore → Catalog → Schema → Table hierarchy
- GRANT/REVOKE syntax
- Data lineage
- Access control lists (ACLs)

## Scoring Guide

### Score Interpretation

| Score Range | Percentage | Assessment | Action |
|-------------|-----------|------------|--------|
| **40-45** | 89-100% | Excellent! Ready for exam | Take exam with confidence |
| **36-39** | 80-87% | Very good! Minor gaps | Review weak areas, then take exam |
| **32-35** | 71-78% | Passing score | More practice recommended |
| **28-31** | 62-69% | Close to passing | Focus on weak domains |
| **Below 28** | < 62% | More study needed | Review all materials, take Day 29 exam |

### Minimum Passing Score

- **Real Exam**: 32/45 (70%)
- **Recommended Practice Score**: 36/45 (80%) for confidence

## Common Mistakes to Avoid

### Syntax Errors
- Forgetting `.format("delta")` in DataFrame writes
- Using `writeStream` instead of `write` for batch
- Incorrect MERGE syntax (missing WHEN clauses)
- Wrong window function syntax

### Conceptual Errors
- Confusing append vs complete output modes
- Not understanding when to use OPTIMIZE vs VACUUM
- Mixing up managed vs external table behavior
- Incorrect Unity Catalog permission hierarchy

### Time Management Errors
- Spending too long on difficult questions
- Not flagging questions for review
- Rushing through easy questions and making mistakes
- Not leaving time for final review

## Study Tips Based on Practice Exam Results

### If You Scored Low on Domain 1 (Lakehouse Platform)
- Review Days 1-5
- Focus on Delta Lake properties
- Practice cluster configuration
- Understand table types

### If You Scored Low on Domain 2 (ELT)
- Review Days 6-15
- Practice window functions extensively
- Master DataFrame API
- Understand ingestion patterns

### If You Scored Low on Domain 3 (Incremental Processing)
- Review Days 16-22
- Practice streaming queries
- Master MERGE syntax
- Understand CDC patterns

### If You Scored Low on Domain 4 (Production)
- Review Days 23-24, 27
- Practice job creation
- Understand error handling
- Learn monitoring techniques

### If You Scored Low on Domain 5 (Governance)
- Review Day 25
- Master Unity Catalog hierarchy
- Practice GRANT/REVOKE
- Understand permissions

## Exam Day Preparation Checklist

### One Week Before
- [ ] Complete this practice exam
- [ ] Score 80%+ on practice exam
- [ ] Review all weak areas
- [ ] Complete Day 29 practice exam

### One Day Before
- [ ] Light review of key concepts
- [ ] Get 8 hours of sleep
- [ ] Prepare exam environment
- [ ] Have ID ready

### Exam Day
- [ ] Arrive 15 minutes early
- [ ] Stable internet connection
- [ ] Quiet environment
- [ ] Positive mindset

## Key Facts to Memorize

### Delta Lake
- Default VACUUM retention: **30 days**
- Time travel: Based on **transaction log**
- OPTIMIZE: Compacts **small files**
- Z-ORDER: Max **4 columns** recommended

### Structured Streaming
- Output modes: **append, complete, update**
- Checkpoint: **Required** for fault tolerance
- Watermark: For handling **late data**
- Trigger types: **once, continuous, processingTime**

### File Formats
- **Delta**: ACID, time travel, schema evolution
- **Parquet**: Columnar, compressed
- **CSV**: Row-based, human-readable
- **JSON**: Semi-structured, flexible

### Unity Catalog
- Hierarchy: **Metastore → Catalog → Schema → Table**
- Permissions: **SELECT, MODIFY, CREATE, USAGE**
- Table types: **Managed vs External**

### Jobs
- Cluster types: **New, existing, job**
- Task types: **Notebook, JAR, Python, SQL**
- Dependencies: **Linear, fan-out, fan-in**

## Taking the Exam

The actual exam questions are in `quiz.md`. 

**Instructions**:
1. Set a 90-minute timer
2. Open `quiz.md`
3. Answer all 45 questions
4. Don't look at answers until complete
5. Score yourself using the answer key
6. Review incorrect answers thoroughly

## After This Practice Exam

### Next Steps
1. **Score Analysis**: Calculate your score by domain
2. **Identify Weak Areas**: Which domains scored lowest?
3. **Targeted Review**: Focus on weak domains
4. **Day 29**: Take second practice exam
5. **Final Review**: Day 30 comprehensive review

### Study Plan Template

**Weak Domain**: _______________  
**Score**: ___/___  
**Topics to Review**:
1. _______________
2. _______________
3. _______________

**Action Items**:
- [ ] Re-read relevant day READMEs
- [ ] Redo exercises from those days
- [ ] Review quiz questions
- [ ] Practice in Databricks workspace

## Confidence Building

Remember:
- This is a **practice exam** - mistakes are learning opportunities
- The real exam has the **same format** - you're getting valuable practice
- **70% is passing** - you don't need to be perfect
- **Most candidates pass** with proper preparation
- You've completed **27 days** of intensive study - you're prepared!

## Additional Resources

- [Databricks Certification Guide](https://www.databricks.com/learn/certification)
- [Exam Registration](https://www.databricks.com/learn/certification/data-engineer-associate)
- Days 1-27 README files for review
- Practice exercises from all previous days

## Next Steps

Tomorrow: [Day 29 - Practice Exam 2 & Review](../day-29-practice-exam-2/README.md)

---

**Good luck! Take your time, read carefully, and trust your preparation.** 🚀
