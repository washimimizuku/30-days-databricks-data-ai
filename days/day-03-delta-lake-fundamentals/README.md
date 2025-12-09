# Day 3: Delta Lake Fundamentals

## Learning Objectives

By the end of today, you will:
- Understand Delta Lake architecture and benefits
- Create and manage Delta tables
- Use time travel to query historical data
- Implement schema evolution
- Understand ACID transactions in Delta Lake
- Know the difference between Delta Lake and Parquet

## Topics Covered

### 1. What is Delta Lake?

**Definition**: Delta Lake is an open-source storage layer that brings ACID transactions to Apache Spark and big data workloads.

**Key Features**:
- **ACID Transactions**: Atomicity, Consistency, Isolation, Durability
- **Time Travel**: Query historical versions of data
- **Schema Evolution**: Add, modify columns without rewriting data
- **Unified Batch and Streaming**: Single source of truth
- **Audit History**: Complete audit trail of changes
- **Scalable Metadata**: Handles petabyte-scale tables

**Architecture**:
```
Delta Lake Table
├── Data Files (Parquet)
│   ├── part-00000.parquet
│   ├── part-00001.parquet
│   └── ...
└── Transaction Log (_delta_log/)
    ├── 00000000000000000000.json
    ├── 00000000000000000001.json
    └── ...
```

### 2. Delta Lake vs Parquet

| Feature | Parquet | Delta Lake |
|---------|---------|------------|
| **Format** | Columnar file format | Parquet + Transaction Log |
| **ACID** | No | Yes |
| **Time Travel** | No | Yes |
| **Schema Evolution** | Manual | Automatic |
| **Concurrent Writes** | No | Yes |
| **Audit Trail** | No | Yes |
| **Updates/Deletes** | Rewrite entire file | Efficient updates |
| **Use Case** | Read-heavy analytics | Lakehouse architecture |

**When to use**:
- **Parquet**: Simple read-only analytics, data archival
- **Delta Lake**: Production data lakes, streaming, frequent updates

### 3. Creating Delta Tables

**Method 1: SQL CREATE TABLE**
```sql
CREATE TABLE employees (
    id INT,
    name STRING,
    department STRING,
    salary DECIMAL(10,2),
    hire_date DATE
) USING DELTA;
```

**Method 2: CREATE TABLE AS SELECT (CTAS)**
```sql
CREATE TABLE employees_backup
USING DELTA
AS SELECT * FROM employees;
```

**Method 3: Python DataFrame**
```python
df.write.format("delta").saveAsTable("employees")
```

**Method 4: Save to Path**
```python
df.write.format("delta").save("/mnt/delta/employees")
```

### 4. Table Types

**Managed Tables**:
- Databricks manages both metadata and data
- Data stored in default location
- DROP TABLE deletes both metadata and data

```sql
CREATE TABLE managed_table (id INT, name STRING) USING DELTA;
```

**External Tables**:
- Databricks manages metadata only
- Data stored in specified location
- DROP TABLE deletes only metadata, data remains

```sql
CREATE TABLE external_table (id INT, name STRING)
USING DELTA
LOCATION '/mnt/delta/external_table';
```

### 5. Time Travel

**Query by Version**:
```sql
-- Query specific version
SELECT * FROM employees VERSION AS OF 5;

-- Query version before specific change
SELECT * FROM employees VERSION AS OF 0;
```

**Query by Timestamp**:
```sql
-- Query as of specific date
SELECT * FROM employees TIMESTAMP AS OF '2024-01-01';

-- Query as of specific datetime
SELECT * FROM employees TIMESTAMP AS OF '2024-01-01 12:00:00';
```

**View Table History**:
```sql
DESCRIBE HISTORY employees;
DESCRIBE HISTORY employees LIMIT 10;
```

**Restore to Previous Version**:
```sql
RESTORE TABLE employees TO VERSION AS OF 5;
RESTORE TABLE employees TO TIMESTAMP AS OF '2024-01-01';
```

### 6. ACID Transactions

**Atomicity**: All or nothing - transactions either complete fully or not at all

**Consistency**: Data remains in valid state before and after transaction

**Isolation**: Concurrent transactions don't interfere with each other

**Durability**: Committed transactions are permanent

**Example**:
```sql
-- Multiple operations in single transaction
INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 95000, '2024-01-01');
UPDATE employees SET salary = 100000 WHERE id = 1;
-- Both succeed or both fail
```

### 7. Schema Evolution

**Add Columns**:
```sql
ALTER TABLE employees ADD COLUMN email STRING;
```

**Automatic Schema Merge**:
```python
# Enable schema merging
df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("employees")
```

**Schema Enforcement**:
- Delta Lake validates schema on write
- Prevents incompatible data types
- Can be disabled with `mergeSchema` option

### 8. Transaction Log

**Purpose**: Records all changes to the table

**Structure**:
- JSON files in `_delta_log/` directory
- Each file represents a transaction
- Contains metadata about changes

**Benefits**:
- Enables time travel
- Provides audit trail
- Ensures ACID properties
- Enables concurrent reads/writes

### 9. Optimizations

**OPTIMIZE** (Compact Small Files):
```sql
OPTIMIZE employees;
```

**Z-ORDER** (Co-locate Related Data):
```sql
OPTIMIZE employees ZORDER BY (department, hire_date);
```

**VACUUM** (Remove Old Files):
```sql
VACUUM employees;
VACUUM employees RETAIN 168 HOURS;  -- 7 days
```

### 10. Best Practices

1. **Use Delta Lake for all production tables**
2. **Enable auto-optimize for write-heavy tables**
3. **Run OPTIMIZE regularly for read-heavy tables**
4. **Use Z-ORDER on frequently filtered columns**
5. **Set appropriate VACUUM retention period**
6. **Partition large tables by date or category**
7. **Use time travel for debugging and auditing**
8. **Enable Change Data Feed for CDC workloads**

## Hands-On Exercises

See `exercise.sql` for practical exercises covering:
- Creating Delta tables
- Time travel queries
- Schema evolution
- Table optimization
- Transaction log exploration

## Key Takeaways

1. **Delta Lake = Parquet + Transaction Log + ACID**
2. **Time travel enables querying historical data** (VERSION AS OF, TIMESTAMP AS OF)
3. **ACID transactions ensure data reliability**
4. **Schema evolution allows flexible data models**
5. **OPTIMIZE and VACUUM maintain performance**
6. **Transaction log provides complete audit trail**
7. **Use Delta Lake for all production lakehouse workloads**

## Exam Tips

- **Know time travel syntax** (VERSION AS OF vs TIMESTAMP AS OF)
- **Understand VACUUM retention** (default 7 days, can't vacuum files needed for time travel)
- **Know OPTIMIZE vs Z-ORDER** (compact files vs co-locate data)
- **Understand managed vs external tables** (DROP TABLE behavior)
- **Know schema evolution options** (mergeSchema, autoMerge)
- **Understand ACID properties** (what each letter means)
- **Know Delta Lake benefits** over Parquet (ACID, time travel, schema evolution)

## Common Exam Questions

1. How do you query a table as it existed 5 versions ago?
2. What is the default VACUUM retention period?
3. What happens to data when you DROP an external Delta table?
4. How do you enable automatic schema merging?
5. What is the purpose of the transaction log?
6. What is the difference between OPTIMIZE and Z-ORDER?

## Additional Resources

- [Delta Lake Documentation](https://docs.delta.io/)
- [Databricks Delta Lake Guide](https://docs.databricks.com/delta/index.html)
- [Delta Lake GitHub](https://github.com/delta-io/delta)

## Next Steps

Tomorrow: [Day 4 - Databricks File System (DBFS)](../day-04-databricks-file-system/README.md)
