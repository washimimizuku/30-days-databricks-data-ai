#!/usr/bin/env python3
"""
Generate template files for bootcamp days
"""

import os
from pathlib import Path

# Day configurations
DAYS = {
    3: {"title": "Delta Lake Fundamentals", "language": "sql"},
    4: {"title": "Databricks File System (DBFS)", "language": "python"},
    5: {"title": "Databases, Tables, and Views", "language": "sql"},
    6: {"title": "Spark SQL Basics", "language": "sql"},
    7: {"title": "Week 1 Review", "language": "sql"},
    8: {"title": "Advanced SQL - Window Functions", "language": "sql"},
    9: {"title": "Advanced SQL - Higher-Order Functions", "language": "sql"},
    10: {"title": "Python DataFrames Basics", "language": "python"},
    11: {"title": "Python DataFrames Advanced", "language": "python"},
    12: {"title": "Data Ingestion Patterns", "language": "python"},
    13: {"title": "Data Transformation Patterns", "language": "python"},
    14: {"title": "ELT Best Practices", "language": "python"},
    15: {"title": "Week 2 Review", "language": "sql"},
    16: {"title": "Structured Streaming Basics", "language": "python"},
    17: {"title": "Streaming Transformations", "language": "python"},
    18: {"title": "Delta Lake Merge (UPSERT)", "language": "sql"},
    19: {"title": "Change Data Capture (CDC)", "language": "python"},
    20: {"title": "Multi-Hop Architecture", "language": "python"},
    21: {"title": "Optimization Techniques", "language": "sql"},
    22: {"title": "Week 3 Review", "language": "sql"},
    23: {"title": "Databricks Jobs", "language": "python"},
    24: {"title": "Databricks Repos & Version Control", "language": "python"},
    25: {"title": "Unity Catalog Basics", "language": "sql"},
    26: {"title": "Data Quality & Testing", "language": "python"},
    27: {"title": "Production Best Practices", "language": "python"},
    28: {"title": "Practice Exam 1", "language": "md"},
    29: {"title": "Practice Exam 2 & Review", "language": "md"},
    30: {"title": "Final Review & Exam", "language": "md"},
}

def create_readme(day_num, title, next_day):
    """Create README.md template"""
    return f"""# Day {day_num}: {title}

## Learning Objectives

By the end of today, you will:
- [Objective 1]
- [Objective 2]
- [Objective 3]

## Topics Covered

### 1. [Topic 1]

[Content here]

### 2. [Topic 2]

[Content here]

## Hands-On Exercises

See `exercise.{'sql' if 'sql' in DAYS[day_num]['language'] else 'py'}` for practical exercises.

## Key Takeaways

1. [Key point 1]
2. [Key point 2]
3. [Key point 3]

## Exam Tips

- [Tip 1]
- [Tip 2]
- [Tip 3]

## Additional Resources

- [Databricks Documentation](https://docs.databricks.com)

## Next Steps

{f"Tomorrow: [Day {next_day} - {DAYS[next_day]['title']}](../day-{next_day:02d}-{DAYS[next_day]['title'].lower().replace(' ', '-').replace('(', '').replace(')', '')}/README.md)" if next_day <= 30 else "Congratulations! You've completed the bootcamp!"}
"""

def create_setup(day_num, title):
    """Create setup.md template"""
    return f"""# Day {day_num} Setup: {title}

## Prerequisites

- Completed previous days
- Active Databricks cluster
- Workspace access

## Setup Steps

### Step 1: [Setup Task 1]

[Instructions]

### Step 2: [Setup Task 2]

[Instructions]

## Verification Checklist

- [ ] [Verification item 1]
- [ ] [Verification item 2]

## Troubleshooting

### [Common Issue 1]
[Solution]

### [Common Issue 2]
[Solution]

## What's Next?

You're ready for Day {day_num} exercises!
"""

def create_exercise_sql(day_num, title):
    """Create exercise.sql template"""
    return f"""-- Day {day_num}: {title} - Exercises
-- =========================================================

-- Exercise 1: [Exercise Title]
-- TODO: [Instructions]

-- YOUR CODE HERE


-- Exercise 2: [Exercise Title]
-- TODO: [Instructions]

-- YOUR CODE HERE


-- Exercise 3: [Exercise Title]
-- TODO: [Instructions]

-- YOUR CODE HERE


-- =========================================================
-- REFLECTION QUESTIONS
-- =========================================================
-- Answer these in a markdown cell:
--
-- 1. [Question 1]
-- 2. [Question 2]
-- 3. [Question 3]

-- =========================================================
-- BONUS CHALLENGES
-- =========================================================

-- Bonus 1: [Challenge Title]
-- TODO: [Instructions]

-- YOUR CODE HERE
"""

def create_exercise_python(day_num, title):
    """Create exercise.py template"""
    return f"""# Day {day_num}: {title} - Exercises
# =========================================================

# Exercise 1: [Exercise Title]
# TODO: [Instructions]

# YOUR CODE HERE


# Exercise 2: [Exercise Title]
# TODO: [Instructions]

# YOUR CODE HERE


# Exercise 3: [Exercise Title]
# TODO: [Instructions]

# YOUR CODE HERE


# =========================================================
# REFLECTION QUESTIONS
# =========================================================
# Answer these questions:
#
# 1. [Question 1]
# 2. [Question 2]
# 3. [Question 3]

# =========================================================
# BONUS CHALLENGES
# =========================================================

# Bonus 1: [Challenge Title]
# TODO: [Instructions]

# YOUR CODE HERE
"""

def create_solution(day_num, title, language):
    """Create solution file template"""
    if language == "sql":
        return f"""-- Day {day_num}: {title} - Solutions
-- =========================================================

-- Exercise 1 Solution
-- [Solution code]

-- Exercise 2 Solution
-- [Solution code]

-- Exercise 3 Solution
-- [Solution code]
"""
    else:
        return f"""# Day {day_num}: {title} - Solutions
# =========================================================

# Exercise 1 Solution
# [Solution code]

# Exercise 2 Solution
# [Solution code]

# Exercise 3 Solution
# [Solution code]
"""

def create_quiz(day_num, title):
    """Create quiz.md template with correct number of questions"""
    # Determine question count based on day type
    if day_num in [7, 15, 22]:  # Review days
        num_questions = 50
        quiz_type = "Review"
    elif day_num in [28, 29, 30]:  # Practice exams
        num_questions = 45
        quiz_type = "Practice Exam"
    else:  # Normal days
        num_questions = 15
        quiz_type = "Quiz"
    
    # Create header
    quiz_content = f"""# Day {day_num} {quiz_type}: {title}

Test your knowledge of {title.lower()}.

"""
    
    # Add note for practice exams
    if day_num in [28, 29, 30]:
        quiz_content += """**Exam Format**: 45 questions, 90 minutes (2 min/question)
**Passing Score**: 32/45 (70%)

**Instructions**: 
- Time yourself (90 minutes)
- Don't look at answers until complete
- Mark difficult questions for review
- Simulate real exam conditions

---

"""
    
    # Generate question templates
    for i in range(1, num_questions + 1):
        quiz_content += f"""## Question {i}
**[Question text]**

A) [Option A]  
B) [Option B]  
C) [Option C]  
D) [Option D]  

<details>
<summary>Click to reveal answer</summary>

**Answer: [Correct answer]**

Explanation: [Explanation text]
</details>

---

"""
    
    # Add scoring guide
    if day_num in [28, 29, 30]:  # Practice exams
        quiz_content += f"""## Scoring Guide

- **40-45 correct (89-100%)**: Excellent! You're ready for the exam.
- **36-39 correct (80-87%)**: Very good! Review weak areas.
- **32-35 correct (71-78%)**: Passing score. More practice recommended.
- **28-31 correct (62-69%)**: Close! Focus on weak topics.
- **Below 28 (< 62%)**: More study needed. Review all materials.

**Minimum to Pass Real Exam**: 32/45 (70%)

---

## Score Analysis

**Your Score**: ___/45 (___%)

**Weak Areas** (topics you missed):
1. 
2. 
3. 

**Action Items**:
- [ ] Review weak area topics
- [ ] Redo related exercises
- [ ] Take another practice exam

"""
    elif day_num in [7, 15, 22]:  # Review days
        quiz_content += f"""## Scoring Guide

- **45-50 correct (90-100%)**: Excellent! You've mastered this week.
- **40-44 correct (80-89%)**: Very good! Minor review needed.
- **35-39 correct (70-79%)**: Good! Review weak areas.
- **30-34 correct (60-69%)**: Fair. Significant review needed.
- **Below 30 (< 60%)**: Review all week's materials thoroughly.

---

## Week Review Summary

**Your Score**: ___/50 (___%)

**Topics Mastered**:
1. 
2. 
3. 

**Topics Needing Review**:
1. 
2. 
3. 

"""
    else:  # Normal days
        quiz_content += f"""## Scoring Guide

- **13-15 correct (87-100%)**: Excellent! You've mastered today's content.
- **11-12 correct (73-80%)**: Good job! Minor review recommended.
- **9-10 correct (60-67%)**: Fair. Review the material again.
- **Below 9 (< 60%)**: Re-read Day {day_num} content thoroughly.

---

"""
    
    # Add key concepts
    quiz_content += """## Key Concepts to Remember

1. [Key concept 1]
2. [Key concept 2]
3. [Key concept 3]

---

## Next Steps

"""
    
    if day_num < 30:
        quiz_content += f"- Review any questions you got wrong\n"
        quiz_content += f"- Re-read relevant sections in Day {day_num} README\n"
        if day_num not in [7, 15, 22, 28, 29]:
            quiz_content += f"- Move on to Day {day_num + 1}\n"
    else:
        quiz_content += "- If you scored 80%+, you're ready for the certification exam!\n"
        quiz_content += "- If below 80%, review weak areas and retake practice exams\n"
    
    return quiz_content

def main():
    """Generate all template files"""
    base_dir = Path("days")
    
    for day_num, config in DAYS.items():
        if day_num <= 2:  # Skip days 1-2 (already created)
            continue
            
        title = config["title"]
        language = config["language"]
        
        # Create day folder
        day_folder = base_dir / f"day-{day_num:02d}-{title.lower().replace(' ', '-').replace('(', '').replace(')', '').replace('&', '')}"
        day_folder.mkdir(exist_ok=True)
        
        # Create files
        next_day = day_num + 1
        
        # README.md
        readme_path = day_folder / "README.md"
        if not readme_path.exists():
            readme_path.write_text(create_readme(day_num, title, next_day))
            print(f"Created {readme_path}")
        
        # setup.md
        setup_path = day_folder / "setup.md"
        if not setup_path.exists():
            setup_path.write_text(create_setup(day_num, title))
            print(f"Created {setup_path}")
        
        # exercise file
        if language == "sql":
            exercise_path = day_folder / "exercise.sql"
            if not exercise_path.exists():
                exercise_path.write_text(create_exercise_sql(day_num, title))
                print(f"Created {exercise_path}")
        elif language == "python":
            exercise_path = day_folder / "exercise.py"
            if not exercise_path.exists():
                exercise_path.write_text(create_exercise_python(day_num, title))
                print(f"Created {exercise_path}")
        
        # solution file
        if language == "sql":
            solution_path = day_folder / "solution.sql"
        elif language == "python":
            solution_path = day_folder / "solution.py"
        else:
            solution_path = day_folder / "solution.md"
            
        if not solution_path.exists():
            solution_path.write_text(create_solution(day_num, title, language))
            print(f"Created {solution_path}")
        
        # quiz.md
        quiz_path = day_folder / "quiz.md"
        if not quiz_path.exists():
            quiz_path.write_text(create_quiz(day_num, title))
            print(f"Created {quiz_path}")

if __name__ == "__main__":
    main()
    print("\nTemplate generation complete!")
