#!/usr/bin/env python3
"""
Regenerate quiz files with correct question counts
"""

import os
from pathlib import Path

# Import the quiz creation function
import sys
sys.path.insert(0, str(Path(__file__).parent))

# Day configurations from CURRICULUM.md
DAYS = {
    1: {"title": "Databricks Architecture & Workspace", "questions": 15},
    2: {"title": "Clusters & Compute", "questions": 15},
    3: {"title": "Delta Lake Fundamentals", "questions": 15},
    4: {"title": "Databricks File System (DBFS)", "questions": 15},
    5: {"title": "Databases, Tables, and Views", "questions": 15},
    6: {"title": "Spark SQL Basics", "questions": 15},
    7: {"title": "Week 1 Review", "questions": 50},  # Review day
    8: {"title": "Advanced SQL - Window Functions", "questions": 15},
    9: {"title": "Advanced SQL - Higher-Order Functions", "questions": 15},
    10: {"title": "Python DataFrames Basics", "questions": 15},
    11: {"title": "Python DataFrames Advanced", "questions": 15},
    12: {"title": "Data Ingestion Patterns", "questions": 15},
    13: {"title": "Data Transformation Patterns", "questions": 15},
    14: {"title": "ELT Best Practices", "questions": 15},
    15: {"title": "Week 2 Review", "questions": 50},  # Review day
    16: {"title": "Structured Streaming Basics", "questions": 15},
    17: {"title": "Streaming Transformations", "questions": 15},
    18: {"title": "Delta Lake Merge (UPSERT)", "questions": 15},
    19: {"title": "Change Data Capture (CDC)", "questions": 15},
    20: {"title": "Multi-Hop Architecture", "questions": 15},
    21: {"title": "Optimization Techniques", "questions": 15},
    22: {"title": "Week 3 Review", "questions": 50},  # Review day
    23: {"title": "Databricks Jobs", "questions": 15},
    24: {"title": "Databricks Repos & Version Control", "questions": 15},
    25: {"title": "Unity Catalog Basics", "questions": 15},
    26: {"title": "Data Quality & Testing", "questions": 15},
    27: {"title": "Production Best Practices", "questions": 15},
    28: {"title": "Practice Exam 1", "questions": 45},  # Practice exam
    29: {"title": "Practice Exam 2 & Review", "questions": 45},  # Practice exam
    30: {"title": "Final Review & Exam", "questions": 45},  # Practice exam
}

def create_quiz(day_num, title, num_questions):
    """Create quiz.md with correct number of questions"""
    # Determine quiz type
    if day_num in [7, 15, 22]:
        quiz_type = "Review"
    elif day_num in [28, 29, 30]:
        quiz_type = "Practice Exam"
    else:
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

**Your Score**: ___/{num_questions} (___%)

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
    """Regenerate all quiz files"""
    base_dir = Path("days")
    
    print("Regenerating quiz files with correct question counts...\n")
    
    for day_num, config in DAYS.items():
        title = config["title"]
        num_questions = config["questions"]
        
        # Find day folder (handle different naming conventions)
        day_folders = list(base_dir.glob(f"day-{day_num:02d}-*"))
        
        if not day_folders:
            print(f"⚠️  Day {day_num} folder not found")
            continue
        
        day_folder = day_folders[0]
        quiz_path = day_folder / "quiz.md"
        
        # Skip Day 1 if it already has detailed content
        if day_num == 1 and quiz_path.exists():
            with open(quiz_path, 'r') as f:
                content = f.read()
                if "What is the primary function of the Databricks control plane?" in content:
                    print(f"✅ Day {day_num}: Keeping existing detailed quiz ({num_questions} questions)")
                    continue
        
        # Create quiz
        quiz_content = create_quiz(day_num, title, num_questions)
        quiz_path.write_text(quiz_content)
        
        print(f"✅ Day {day_num}: {title} - Generated {num_questions} questions")

    print("\n✅ Quiz regeneration complete!")
    print("\nQuestion counts:")
    print("- Normal days (1-6, 8-14, 16-21, 23-27): 15 questions")
    print("- Review days (7, 15, 22): 50 questions")
    print("- Practice exams (28-30): 45 questions")

if __name__ == "__main__":
    main()
