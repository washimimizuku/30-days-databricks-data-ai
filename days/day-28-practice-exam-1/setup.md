# Day 28 Setup: Practice Exam 1

## Overview

This practice exam requires **NO Databricks setup**. It's a written exam that tests your knowledge across all domains covered in Days 1-27.

## Prerequisites

### Knowledge Prerequisites
- ✅ Completed Days 1-27 (or equivalent study)
- ✅ Reviewed all quiz questions from previous days
- ✅ Practiced hands-on exercises
- ✅ Comfortable with both SQL and Python

### Environment Prerequisites
- ✅ Quiet space for 90 minutes
- ✅ Timer or clock
- ✅ Notebook for flagging questions (optional)
- ✅ No access to notes, documentation, or Databricks workspace

## Exam Setup

### Step 1: Prepare Your Environment

**Physical Setup**:
```
1. Find a quiet space with no interruptions
2. Close all browser tabs except the quiz
3. Turn off notifications on your devices
4. Have water and snacks nearby
5. Ensure comfortable seating
```

**Mental Setup**:
```
1. Take 5 deep breaths
2. Review the exam format (45 questions, 90 minutes)
3. Remember: 70% is passing (32/45 correct)
4. Trust your preparation
5. Stay calm and focused
```

### Step 2: Set Up Your Timer

**Time Allocation**:
- **Total Time**: 90 minutes
- **Per Question**: 2 minutes average
- **First Pass**: 60-70 minutes (answer all questions)
- **Review Pass**: 20-30 minutes (review flagged questions)

**Timer Options**:
1. Phone timer (put phone on silent)
2. Online timer (e.g., timer.com)
3. Physical clock or watch
4. Pomodoro timer app

### Step 3: Prepare Your Answer Sheet (Optional)

Create a simple tracking sheet:

```
Question | Answer | Confidence | Flag for Review
---------|--------|------------|----------------
1        | B      | High       | 
2        | A      | Medium     | ✓
3        | D      | High       |
...
```

Or simply use the quiz.md file directly and mark your answers.

### Step 4: Review Exam Rules

**Allowed**:
- ✅ Reading questions carefully
- ✅ Flagging questions for review
- ✅ Changing answers before submitting
- ✅ Taking breaks (but timer keeps running)

**NOT Allowed**:
- ❌ Looking at notes or documentation
- ❌ Testing code in Databricks
- ❌ Searching online
- ❌ Using AI assistants
- ❌ Asking others for help

### Step 5: Understand the Question Format

**Question Structure**:
```markdown
## Question X
**[Question text with scenario]**

A) [Option A]
B) [Option B]
C) [Option C]
D) [Option D]

<details>
<summary>Click to reveal answer</summary>

**Answer: [Correct answer]**

Explanation: [Why this is correct and others are wrong]
</details>
```

**How to Answer**:
1. Read the question twice
2. Identify keywords (MOST, BEST, NOT, EXCEPT)
3. Eliminate obviously wrong answers
4. Choose the best remaining option
5. Flag if unsure
6. Move to next question

## Exam Strategy

### Time Management Strategy

**First 60 Minutes** (Questions 1-45):
- Spend 1-2 minutes per question
- Answer all questions you know immediately
- Flag difficult questions (don't spend > 3 minutes)
- Keep moving forward

**Last 30 Minutes** (Review):
- Review flagged questions
- Double-check answers you're unsure about
- Verify you answered all questions
- Don't second-guess yourself too much

### Question Approach Strategy

**For "Choose the Best" Questions**:
1. Identify what makes an approach "best" (efficiency, scalability, best practice)
2. Eliminate approaches that are incorrect or inefficient
3. Choose the most optimal solution

**For "Identify the Error" Questions**:
1. Read the code carefully
2. Check syntax first (missing keywords, wrong order)
3. Check logic second (will it produce correct results?)
4. Look for common mistakes (wrong output mode, missing checkpoint, etc.)

**For "Complete the Code" Questions**:
1. Understand what the code is trying to do
2. Identify what's missing
3. Know exact syntax (no guessing)
4. Verify the completion makes sense

**For "Conceptual" Questions**:
1. Recall definitions and properties
2. Eliminate obviously false statements
3. Watch for tricky wording
4. Choose the most accurate statement

### Confidence Level Strategy

**High Confidence** (70% of questions):
- Answer immediately
- Don't second-guess
- Move on quickly

**Medium Confidence** (20% of questions):
- Narrow down to 2 options
- Make educated guess
- Flag for review
- Come back if time permits

**Low Confidence** (10% of questions):
- Eliminate 1-2 wrong answers
- Make best guess
- Flag for review
- Definitely come back

## Domain-Specific Tips

### Domain 1: Databricks Lakehouse Platform (7 questions)

**Focus Areas**:
- Cluster types and when to use each
- Delta Lake ACID properties
- Managed vs external tables
- DBFS paths and operations

**Common Traps**:
- Confusing all-purpose vs job clusters
- Not knowing default VACUUM retention (30 days)
- Mixing up table locations

### Domain 2: ELT with Spark SQL and Python (13 questions)

**Focus Areas**:
- Window function syntax
- Higher-order functions (TRANSFORM, FILTER)
- DataFrame API methods
- COPY INTO vs Auto Loader

**Common Traps**:
- Window function PARTITION BY vs ORDER BY
- Forgetting .format("delta")
- Confusing schema inference options

### Domain 3: Incremental Data Processing (11 questions)

**Focus Areas**:
- Streaming output modes
- MERGE syntax
- CDC patterns
- OPTIMIZE and Z-ORDER

**Common Traps**:
- Wrong output mode for use case
- Missing WHEN clauses in MERGE
- Not understanding when to OPTIMIZE vs VACUUM

### Domain 4: Production Pipelines (9 questions)

**Focus Areas**:
- Job task dependencies
- Git workflows
- Error handling patterns
- Idempotency

**Common Traps**:
- Not understanding task dependencies
- Forgetting retry logic
- Missing checkpoint locations

### Domain 5: Data Governance (5 questions)

**Focus Areas**:
- Unity Catalog hierarchy
- GRANT/REVOKE syntax
- Permission types
- Managed vs external in Unity Catalog

**Common Traps**:
- Wrong permission level
- Incorrect hierarchy order
- Not understanding permission inheritance

## Verification Checklist

Before starting the exam:

### Environment
- [ ] Quiet space secured for 90 minutes
- [ ] Timer set and ready
- [ ] All distractions removed
- [ ] Comfortable seating
- [ ] Water/snacks available

### Mental Preparation
- [ ] Reviewed exam format
- [ ] Understand scoring (70% to pass)
- [ ] Confident in preparation
- [ ] Calm and focused
- [ ] Ready to start

### Materials
- [ ] quiz.md file open
- [ ] Answer tracking method ready (optional)
- [ ] No notes or documentation accessible
- [ ] No Databricks workspace open

### Understanding
- [ ] Know the question format
- [ ] Understand time allocation
- [ ] Clear on exam rules
- [ ] Have a strategy for difficult questions

## Troubleshooting

### Issue: Feeling Overwhelmed

**Solution**:
1. Take 3 deep breaths
2. Remember: You've studied 27 days for this
3. 70% is passing - you don't need perfection
4. Focus on one question at a time
5. Flag and move on if stuck

### Issue: Running Out of Time

**Solution**:
1. Don't panic
2. Quickly answer remaining questions (educated guesses)
3. Prioritize flagged questions you're most confident about
4. Submit even if you don't finish review

### Issue: Second-Guessing Answers

**Solution**:
1. Trust your first instinct (usually correct)
2. Only change if you're certain you misread
3. Don't overthink
4. Move on

### Issue: Don't Know an Answer

**Solution**:
1. Eliminate obviously wrong options
2. Make educated guess from remaining
3. Flag for review
4. Come back if time permits
5. Never leave blank

### Issue: Losing Focus

**Solution**:
1. Take a 30-second break (close eyes, breathe)
2. Stand up and stretch (timer keeps running)
3. Drink water
4. Refocus on current question only
5. Don't think about score while testing

## Post-Exam Process

### Immediate Actions (After Completing All Questions)

1. **Score Your Exam**:
   - Go through answer key in quiz.md
   - Count correct answers
   - Calculate percentage

2. **Record Your Score**:
   ```
   Total Score: ___/45 (___%)
   Domain 1 (Lakehouse): ___/7
   Domain 2 (ELT): ___/13
   Domain 3 (Incremental): ___/11
   Domain 4 (Production): ___/9
   Domain 5 (Governance): ___/5
   ```

3. **Identify Weak Areas**:
   - Which domain had lowest score?
   - What types of questions were hardest?
   - Were there common themes in mistakes?

### Analysis Actions (Within 24 Hours)

1. **Review Incorrect Answers**:
   - Read explanation for each wrong answer
   - Understand why you got it wrong
   - Note the correct concept

2. **Create Study Plan**:
   - List weak domains
   - Identify specific topics to review
   - Plan which days to re-study

3. **Decide Next Steps**:
   - **Score 80%+**: Take Day 29 exam, then schedule real exam
   - **Score 70-79%**: Review weak areas, take Day 29 exam
   - **Score < 70%**: Intensive review of weak domains before Day 29

## What's Next?

### If You Scored Well (80%+)
1. ✅ Celebrate! You're ready!
2. ✅ Light review of any missed questions
3. ✅ Take Day 29 practice exam tomorrow
4. ✅ Schedule your real exam

### If You Scored Passing (70-79%)
1. ✅ Good job! You're close!
2. ✅ Review weak domains thoroughly
3. ✅ Redo exercises from those days
4. ✅ Take Day 29 practice exam
5. ✅ More review before real exam

### If You Scored Below Passing (< 70%)
1. ✅ Don't worry - this is practice!
2. ✅ Identify your weakest domain
3. ✅ Re-study those days completely
4. ✅ Redo all exercises
5. ✅ Retake this practice exam
6. ✅ Then take Day 29 exam

## Key Reminders

1. **This is practice** - mistakes are learning opportunities
2. **70% passes** - you don't need perfection
3. **Trust your preparation** - you've studied 27 days
4. **Read carefully** - many mistakes come from misreading
5. **Manage time** - don't get stuck on one question
6. **Stay calm** - anxiety hurts performance

---

**You're ready! Open quiz.md and begin your practice exam.** 🚀

**Good luck!** 🍀
