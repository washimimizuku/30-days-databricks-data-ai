# Day 28: Practice Exam 1 - Answer Key & Explanations

## How to Use This Answer Key

1. **Complete the exam first** - Don't look at answers until you've attempted all questions
2. **Score yourself** - Count how many you got correct
3. **Review explanations** - Understand why each answer is correct
4. **Identify patterns** - Note which types of questions you missed
5. **Create study plan** - Focus on weak areas

## Scoring Your Exam

### Calculate Your Score

```
Total Correct: ___/45
Percentage: ___% 

Domain Breakdown:
- Domain 1 (Questions 1-7): ___/7
- Domain 2 (Questions 8-20): ___/13
- Domain 3 (Questions 21-31): ___/11
- Domain 4 (Questions 32-40): ___/9
- Domain 5 (Questions 41-45): ___/5
```

### Score Interpretation

| Score | Percentage | Assessment | Recommendation |
|-------|-----------|------------|----------------|
| 40-45 | 89-100% | Excellent! | Schedule your exam |
| 36-39 | 80-87% | Very good! | Light review, then exam |
| 32-35 | 71-78% | Passing | More practice recommended |
| 28-31 | 62-69% | Close | Focus on weak domains |
| < 28 | < 62% | More study needed | Review all materials |

## Answer Key Quick Reference

**Domain 1 (Lakehouse Platform)**: 1-B, 2-C, 3-A, 4-D, 5-B, 6-C, 7-A  
**Domain 2 (ELT)**: 8-B, 9-D, 10-C, 11-A, 12-B, 13-D, 14-C, 15-A, 16-B, 17-D, 18-C, 19-A, 20-B  
**Domain 3 (Incremental)**: 21-C, 22-A, 23-D, 24-B, 25-C, 26-A, 27-D, 28-B, 29-C, 30-A, 31-D  
**Domain 4 (Production)**: 32-B, 33-C, 34-A, 35-D, 36-B, 37-C, 38-A, 39-D, 40-B  
**Domain 5 (Governance)**: 41-C, 42-A, 43-D, 44-B, 45-C

## Detailed Explanations

All detailed explanations are provided in the quiz.md file under each question's answer section.

## Common Mistakes Analysis

### Mistake Pattern 1: Misreading Questions

**Common Issues**:
- Missing keywords like "MOST efficient" or "BEST practice"
- Overlooking negative words (NOT, EXCEPT, CANNOT)
- Not reading all options before choosing

**How to Avoid**:
- Read each question twice
- Underline or highlight keywords
- Eliminate wrong answers systematically

### Mistake Pattern 2: Syntax Confusion

**Common Issues**:
- Mixing up SQL and Python syntax
- Forgetting required clauses (WHEN MATCHED in MERGE)
- Wrong method names (writeStream vs write)

**How to Avoid**:
- Practice writing code by hand
- Review syntax cheat sheets
- Do more hands-on exercises

### Mistake Pattern 3: Conceptual Misunderstanding

**Common Issues**:
- Not understanding when to use each output mode
- Confusing managed vs external tables
- Wrong understanding of permissions

**How to Avoid**:
- Re-read concept sections in README files
- Create comparison tables
- Teach concepts to someone else

### Mistake Pattern 4: Time Pressure Errors

**Common Issues**:
- Rushing through easy questions
- Spending too long on hard questions
- Not flagging questions for review

**How to Avoid**:
- Practice time management
- Use flagging system
- Take practice exams under time pressure

## Domain-Specific Review Guide

### If You Scored Low on Domain 1 (< 5/7)

**Review These Days**:
- Day 1: Databricks Architecture
- Day 2: Clusters & Compute
- Day 3: Delta Lake Fundamentals
- Day 4: DBFS
- Day 5: Databases, Tables, and Views

**Key Concepts to Master**:
- Control plane vs data plane
- All-purpose vs job clusters
- Delta Lake ACID properties
- Time travel syntax
- Managed vs external tables
- DBFS paths and operations

**Practice Activities**:
- Create different cluster types
- Practice time travel queries
- Create managed and external tables
- Use dbutils commands

### If You Scored Low on Domain 2 (< 9/13)

**Review These Days**:
- Day 6: Spark SQL Basics
- Day 8: Window Functions
- Day 9: Higher-Order Functions
- Day 10-11: Python DataFrames
- Day 12: Data Ingestion
- Day 13: Data Transformation
- Day 14: ELT Best Practices

**Key Concepts to Master**:
- Window function syntax (PARTITION BY, ORDER BY)
- Higher-order functions (TRANSFORM, FILTER, AGGREGATE)
- DataFrame API methods
- COPY INTO vs Auto Loader
- Schema inference and evolution
- Medallion architecture

**Practice Activities**:
- Write 20+ window function queries
- Practice array manipulation
- Convert SQL to DataFrame API
- Implement Bronze-Silver-Gold pipeline

### If You Scored Low on Domain 3 (< 8/11)

**Review These Days**:
- Day 16: Structured Streaming Basics
- Day 17: Streaming Transformations
- Day 18: Delta Lake MERGE
- Day 19: Change Data Capture
- Day 20: Multi-Hop Architecture
- Day 21: Optimization Techniques

**Key Concepts to Master**:
- readStream and writeStream syntax
- Output modes (append, complete, update)
- Checkpointing
- MERGE syntax (WHEN MATCHED/NOT MATCHED)
- CDC patterns (SCD Type 1 and 2)
- OPTIMIZE, Z-ORDER, VACUUM

**Practice Activities**:
- Create streaming queries
- Implement MERGE operations
- Build CDC pipeline
- Practice optimization commands

### If You Scored Low on Domain 4 (< 6/9)

**Review These Days**:
- Day 23: Databricks Jobs
- Day 24: Databricks Repos
- Day 27: Production Best Practices

**Key Concepts to Master**:
- Job task types and dependencies
- Git workflows (branch, commit, merge)
- Error handling and retry logic
- Logging and monitoring
- Idempotency patterns
- Deployment strategies

**Practice Activities**:
- Create multi-task jobs
- Practice Git commands
- Implement error handling
- Add logging to pipelines

### If You Scored Low on Domain 5 (< 3/5)

**Review These Days**:
- Day 25: Unity Catalog Basics

**Key Concepts to Master**:
- Three-level namespace (Catalog.Schema.Table)
- GRANT/REVOKE syntax
- Permission types (SELECT, MODIFY, CREATE, USAGE)
- Managed vs external tables in Unity Catalog
- System tables and audit logs

**Practice Activities**:
- Create catalogs and schemas
- Practice GRANT/REVOKE commands
- Query system tables
- Understand permission inheritance

## Study Plan Template

Based on your results, create a personalized study plan:

### Weak Domain 1: _______________

**Current Score**: ___/___  
**Target Score**: ___/___  
**Days to Review**: _______________

**Action Items**:
- [ ] Re-read README files for relevant days
- [ ] Redo all exercises
- [ ] Review quiz questions
- [ ] Practice in Databricks workspace
- [ ] Create summary notes

**Time Needed**: ___ hours

### Weak Domain 2: _______________

**Current Score**: ___/___  
**Target Score**: ___/___  
**Days to Review**: _______________

**Action Items**:
- [ ] Re-read README files for relevant days
- [ ] Redo all exercises
- [ ] Review quiz questions
- [ ] Practice in Databricks workspace
- [ ] Create summary notes

**Time Needed**: ___ hours

## Next Steps

### Immediate (Today)
1. ✅ Calculate your score by domain
2. ✅ Identify your weakest domain
3. ✅ Read explanations for all incorrect answers
4. ✅ Note common themes in mistakes

### Short Term (This Week)
1. ✅ Review weak domains thoroughly
2. ✅ Redo exercises from those days
3. ✅ Take Day 29 practice exam
4. ✅ Compare scores between exams

### Before Real Exam
1. ✅ Score 80%+ on both practice exams
2. ✅ Review all flagged topics
3. ✅ Get good sleep
4. ✅ Stay confident

## Confidence Boosters

Remember:
- **You've studied 27 days** - that's significant preparation
- **Practice exams are harder** - real exam may feel easier
- **70% passes** - you don't need perfection
- **Mistakes are learning** - each wrong answer teaches you
- **You can retake** - if needed, you can try again

## Additional Practice Resources

### If You Need More Practice

1. **Retake This Exam** (in 2-3 days after review)
2. **Take Day 29 Exam** (tomorrow or after review)
3. **Review All Previous Quizzes** (Days 1-27)
4. **Create Your Own Questions** (best way to learn)
5. **Practice in Databricks** (hands-on is crucial)

### Official Resources

- [Databricks Certification Guide](https://www.databricks.com/learn/certification)
- [Databricks Documentation](https://docs.databricks.com)
- [Databricks Academy](https://www.databricks.com/learn/training)

## Final Thoughts

This practice exam is designed to:
- ✅ Simulate real exam conditions
- ✅ Identify your weak areas
- ✅ Build confidence
- ✅ Improve time management
- ✅ Prepare you for success

Use your results to create a targeted study plan. Focus on weak areas, but don't neglect strong areas completely.

**You've got this!** 🚀

---

**Next**: Review weak areas, then take Day 29 Practice Exam 2
