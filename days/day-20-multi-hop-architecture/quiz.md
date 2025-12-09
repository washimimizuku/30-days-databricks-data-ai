# Day 20: Multi-Hop Architecture - Quiz

## Instructions
- Answer all 15 questions
- Each question has one correct answer unless stated otherwise
- Check your answers at the end

---

## Questions

### Question 1
What is another name for Multi-Hop Architecture?

A) Lambda Architecture  
B) Medallion Architecture  
C) Kappa Architecture  
D) Star Schema  

### Question 2
What is the primary purpose of the Bronze layer?

A) Store aggregated business metrics  
B) Store cleaned and validated data  
C) Store raw data exactly as received from source  
D) Store denormalized data for BI tools  

### Question 3
Which layer should contain data quality validation and cleaning?

A) Bronze  
B) Silver  
C) Gold  
D) All layers equally  

### Question 4
What type of data should be stored in the Gold layer?

A) Raw data from source systems  
B) Cleaned individual records  
C) Business-level aggregates and metrics  
D) Temporary staging data  

### Question 5
In the Medallion Architecture, which layer typically has the MOST data volume?

A) Bronze  
B) Silver  
C) Gold  
D) All layers have equal volume  

### Question 6
What is a key characteristic of the Bronze layer?

A) Heavily aggregated  
B) Append-only and immutable  
C) Denormalized for performance  
D) Contains only validated data  

### Question 7
Which operation is most commonly used to update Gold layer tables incrementally?

A) INSERT  
B) UPDATE  
C) MERGE  
D) REPLACE  

### Question 8
What should be added to Bronze layer data for audit purposes?

A) Business calculations  
B) Aggregations  
C) Ingestion metadata (timestamp, source file)  
D) User preferences  

### Question 9
Which layer is optimized for business intelligence and analytics tools?

A) Bronze  
B) Silver  
C) Gold  
D) Bronze and Silver equally  

### Question 10
What is the recommended approach for handling duplicates in multi-hop architecture?

A) Remove in Bronze layer  
B) Remove in Silver layer  
C) Remove in Gold layer  
D) Keep all duplicates  

### Question 11
Which layer typically uses schema-on-read?

A) Bronze  
B) Silver  
C) Gold  
D) None  

### Question 12
What is the primary benefit of multi-hop architecture?

A) Reduces storage costs  
B) Clear data quality progression and easier troubleshooting  
C) Eliminates need for data validation  
D) Faster query performance only  

### Question 13
In incremental processing from Silver to Gold, what is the best practice?

A) Always reprocess all data  
B) Process only new or changed Silver records  
C) Delete and recreate Gold tables  
D) Process random samples  

### Question 14
Which layer should preserve the original source data format?

A) Bronze  
B) Silver  
C) Gold  
D) None - always transform immediately  

### Question 15
What is the typical user audience for the Gold layer?

A) Data engineers only  
B) System administrators  
C) Business analysts and BI tools  
D) External customers  

---

## Answer Key

<details>
<summary>Click to reveal answers</summary>

### Answer 1
**B) Medallion Architecture**

Explanation: Multi-hop architecture is also known as Medallion Architecture, named after the bronze, silver, and gold medal progression representing increasing data quality.

### Answer 2
**C) Store raw data exactly as received from source**

Explanation: The Bronze layer stores raw data as-is from source systems with minimal transformation, preserving the original data for audit and reprocessing purposes.

### Answer 3
**B) Silver**

Explanation: The Silver layer is where data quality validation, cleaning, deduplication, and standardization occur. Bronze keeps raw data, and Gold contains aggregates.

### Answer 4
**C) Business-level aggregates and metrics**

Explanation: The Gold layer contains business-level aggregates, metrics, and denormalized data optimized for analytics and BI tools.

### Answer 5
**A) Bronze**

Explanation: Bronze typically has the most data volume as it stores all raw data including duplicates and invalid records. Silver filters out bad data, and Gold aggregates further reduce volume.

### Answer 6
**B) Append-only and immutable**

Explanation: Bronze layer is typically append-only and immutable, preserving all raw data exactly as received for audit trails and potential reprocessing.

### Answer 7
**C) MERGE**

Explanation: MERGE is commonly used for incremental Gold layer updates as it can handle both inserting new aggregates and updating existing ones in a single operation.

### Answer 8
**C) Ingestion metadata (timestamp, source file)**

Explanation: Bronze layer should include audit metadata like ingestion_time and source_file to track data lineage and support troubleshooting.

### Answer 9
**C) Gold**

Explanation: The Gold layer is specifically optimized for business intelligence and analytics tools, containing denormalized, aggregated data ready for consumption.

### Answer 10
**B) Remove in Silver layer**

Explanation: Deduplication should occur in the Silver layer. Bronze preserves all data including duplicates, and Silver applies data quality rules including deduplication.

### Answer 11
**A) Bronze**

Explanation: Bronze layer typically uses schema-on-read, allowing flexible ingestion of raw data. Silver and Gold enforce schemas for data quality and consistency.

### Answer 12
**B) Clear data quality progression and easier troubleshooting**

Explanation: The primary benefit is clear data quality progression through layers, making it easier to troubleshoot issues, reprocess data, and understand data transformations.

### Answer 13
**B) Process only new or changed Silver records**

Explanation: Best practice for Silver to Gold is incremental processing - only processing new or changed records to improve efficiency and reduce processing time.

### Answer 14
**A) Bronze**

Explanation: Bronze layer should preserve the original source data format to maintain a complete audit trail and enable reprocessing if transformation logic changes.

### Answer 15
**C) Business analysts and BI tools**

Explanation: Gold layer is designed for business analysts and BI tools, containing business-ready aggregates and metrics in formats optimized for analytics.

</details>

---

## Scoring Guide

- **13-15 correct**: Excellent! You understand Medallion Architecture well
- **10-12 correct**: Good! Review the concepts you missed
- **7-9 correct**: Fair. Practice more with multi-hop pipelines
- **Below 7**: Review the README.md and complete more exercises

---

## Key Concepts to Remember

1. **Medallion Architecture** = Bronze → Silver → Gold
2. **Bronze** = Raw data (as-is from source)
3. **Silver** = Cleaned, validated, conformed data
4. **Gold** = Business-level aggregates and metrics
5. **Data quality** improves through each layer
6. **Bronze** is append-only and immutable
7. **Silver** applies data quality checks and deduplication
8. **Gold** is optimized for BI and analytics
9. **MERGE** is common for Gold layer updates
10. **Incremental processing** improves efficiency

---

**Next**: Day 21 - Optimization Techniques
