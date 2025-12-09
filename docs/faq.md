# Frequently Asked Questions (FAQ)

## General Questions

### Q: Do I need prior experience with Databricks?
**A**: No, but basic SQL and Python knowledge is recommended. This bootcamp starts from the fundamentals.

### Q: How much time should I dedicate daily?
**A**: Plan for 1.5-2 hours per day. Consistency is more important than long study sessions.

### Q: Can I complete this faster than 30 days?
**A**: Yes! You can accelerate by studying 3-4 hours daily. However, don't skip the hands-on exercises.

### Q: What if I fall behind?
**A**: No problem! This is self-paced. Just pick up where you left off. The 30-day structure is a guideline.

### Q: Is this bootcamp enough to pass the exam?
**A**: If you complete all exercises and score 80%+ on practice exams, you have a strong chance (80-85% pass rate).

---

## Account and Setup

### Q: Should I use Community Edition or Trial?
**A**: 
- **Community Edition**: Free, limited to single-node clusters, perfect for learning
- **Trial**: 14 days, full features, better for production-like practice
- **Recommendation**: Start with Community Edition, upgrade to trial if needed

### Q: How much does the exam cost?
**A**: $200 USD. The certification is valid for 2 years.

### Q: Do I need a cloud account (AWS/Azure/GCP)?
**A**: No, Databricks Community Edition doesn't require a cloud account. Trial accounts may require cloud setup.

### Q: Can I use my company's Databricks account?
**A**: Yes, if you have access. Just be mindful of costs and company policies.

---

## Technical Questions

### Q: SQL or Python - which should I focus on?
**A**: Both! The exam tests both languages. Aim for 60% SQL, 40% Python proficiency.

### Q: Do I need to know Scala?
**A**: No, the Data Engineer Associate exam focuses on SQL and Python. Scala is optional.

### Q: What's the difference between Delta Lake and Parquet?
**A**: 
- **Parquet**: Columnar file format, no ACID guarantees
- **Delta Lake**: Built on Parquet, adds ACID transactions, time travel, schema evolution
- **Use Delta** for lakehouse architectures

### Q: When should I use COPY INTO vs Auto Loader?
**A**:
- **COPY INTO**: Batch loading, idempotent, good for scheduled loads
- **Auto Loader**: Streaming ingestion, automatic schema evolution, better for continuous data
- **Exam tip**: Know the trade-offs!

### Q: What's the difference between append, complete, and update output modes?
**A**:
- **Append**: Only new rows (most common)
- **Complete**: Entire result table (for aggregations)
- **Update**: Only changed rows (for aggregations with watermark)

---

## Exam Questions

### Q: How difficult is the exam?
**A**: Moderate. With proper preparation (this bootcamp), most people pass. The exam tests practical knowledge, not just theory.

### Q: What's the exam format?
**A**: 45 multiple-choice questions, 90 minutes, 70% passing score (32/45 correct).

### Q: Can I use notes during the exam?
**A**: No, it's a closed-book exam. However, you can use scratch paper.

### Q: What happens if I fail?
**A**: You can retake after 14 days. Review the exam report to identify weak areas.

### Q: How long is the certification valid?
**A**: 2 years from the date you pass.

### Q: Do I get a certificate?
**A**: Yes, a digital certificate and badge you can share on LinkedIn.

---

## Study Strategy

### Q: Should I memorize syntax?
**A**: Yes, for common operations (MERGE, time travel, streaming). The exam tests exact syntax.

### Q: How many practice exams should I take?
**A**: At least 2-3. Take them seriously - simulate exam conditions.

### Q: What if I score poorly on practice exams?
**A**: Review weak areas, do more hands-on practice, and retake. Don't take the real exam until you score 80%+.

### Q: Should I read the documentation?
**A**: Yes! The official Databricks docs are excellent. Use them to clarify concepts.

### Q: How important are hands-on exercises?
**A**: Critical! Hands-on practice is the #1 success factor. Don't skip exercises.

---

## Cluster and Cost Questions

### Q: How much will this cost?
**A**:
- **Community Edition**: Free
- **Trial**: Free for 14 days
- **Paid**: Varies by cloud provider and usage (estimate $50-200 for bootcamp if using paid account)

### Q: How do I minimize costs?
**A**:
- Use Community Edition
- Set auto-termination (30 minutes)
- Stop clusters when not in use
- Use smallest instance types
- Use spot/preemptible instances

### Q: My cluster won't start. What should I do?
**A**:
1. Check cloud credits/trial status
2. Try a smaller instance type
3. Check region capacity
4. Review error logs
5. Wait and retry

### Q: Should I use all-purpose or job clusters?
**A**:
- **Learning**: All-purpose (interactive)
- **Production**: Job clusters (cost-effective)
- **Exam**: Know the difference!

---

## Content Questions

### Q: Why is Delta Lake so important?
**A**: It's the foundation of the Databricks Lakehouse. Expect 30-40% of exam questions on Delta Lake.

### Q: Do I need to know Unity Catalog deeply?
**A**: Basic understanding is enough (10% of exam). Know the hierarchy and permissions.

### Q: What's the medallion architecture?
**A**: Bronze (raw) → Silver (cleaned) → Gold (aggregated). It's a best practice for data pipelines.

### Q: Should I learn Delta Live Tables?
**A**: Not required for Associate exam. It's covered in the Professional certification.

---

## After the Bootcamp

### Q: What's next after passing?
**A**:
1. Update LinkedIn and resume
2. Build portfolio projects
3. Consider Professional certification
4. Apply skills to real projects

### Q: Should I pursue Professional certification?
**A**: If you want to deepen your expertise, yes! Wait 2-3 months after Associate to prepare.

### Q: How do I maintain my skills?
**A**: Build projects, contribute to open source, stay updated with Databricks releases.

### Q: Can I get a job with just this certification?
**A**: The certification helps, but combine it with:
- Portfolio projects
- Practical experience
- Other relevant skills (cloud, SQL, Python)

---

## Troubleshooting

### Q: My notebook won't run. What's wrong?
**A**:
1. Check cluster is running
2. Verify cluster is attached to notebook
3. Check for syntax errors
4. Review error messages
5. Restart cluster if needed

### Q: I get "Table not found" errors
**A**:
1. Verify table exists: `SHOW TABLES`
2. Check database: `USE database_name`
3. Use fully qualified name: `database.table`
4. Ensure table was created successfully

### Q: Streaming query fails with checkpoint error
**A**:
1. Ensure checkpoint location is specified
2. Check checkpoint location is writable
3. Delete checkpoint to restart (loses state)
4. Verify schema compatibility

### Q: MERGE statement fails
**A**:
1. Check join condition is correct
2. Verify column names match
3. Ensure target is Delta table
4. Check for syntax errors (WHEN clauses)

---

## Community and Support

### Q: Where can I get help?
**A**:
- [Databricks Community Forums](https://community.databricks.com/)
- [Stack Overflow](https://stackoverflow.com/questions/tagged/databricks)
- This bootcamp's GitHub issues
- Study groups and Discord communities

### Q: Can I contribute to this bootcamp?
**A**: Yes! Submit pull requests with improvements, corrections, or additional content.

### Q: How do I report errors in the bootcamp?
**A**: Open an issue on GitHub with details about the error and which day/file it's in.

---

## Tips for Success

### Q: What's the #1 tip for passing?
**A**: **Hands-on practice!** Don't just read - actually run the code and build projects.

### Q: How do I stay motivated for 30 days?
**A**:
- Track progress (use progress tracker)
- Join study groups
- Set daily goals
- Celebrate small wins
- Remember your "why"

### Q: What if I don't understand a concept?
**A**:
1. Re-read the material
2. Check official documentation
3. Try hands-on exercises
4. Ask in community forums
5. Watch video tutorials
6. Move on and come back later

### Q: Should I take notes?
**A**: Yes! Writing helps retention. Keep a learning journal.

---

## Exam Day

### Q: What should I do the day before the exam?
**A**:
- Light review (don't cram)
- Get good sleep (8 hours)
- Prepare exam environment
- Relax and stay confident

### Q: What do I need for the exam?
**A**:
- Government-issued ID
- Quiet, well-lit room
- Stable internet connection
- Webcam and microphone
- Clear desk

### Q: Can I take breaks during the exam?
**A**: No, the 90 minutes is continuous. Use the restroom before starting.

### Q: What if I have technical issues during the exam?
**A**: Contact the proctor immediately through the chat. They can help or reschedule if needed.

---

## Still Have Questions?

- Check the [Databricks Community Forums](https://community.databricks.com/)
- Review the [Official Exam Guide](https://www.databricks.com/learn/certification/data-engineer-associate)
- Open an issue on this bootcamp's GitHub repository

**Good luck with your certification journey!** 🚀
