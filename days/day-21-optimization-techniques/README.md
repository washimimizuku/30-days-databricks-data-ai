# Day 21: Optimization Techniques

## Learning Objectives

By the end of today, you will:
- Understand Delta Lake optimization strategies
- Implement table partitioning effectively
- Use Z-ordering for data skipping
- Apply OPTIMIZE command for file compaction
- Use VACUUM to clean up old files
- Understand data skipping and statistics
- Optimize query performance
- Apply caching strategies
- Implement best practices for production
- Measure and improve performance

## Topics Covered

### 1. Why Optimization Matters

**Common Performance Issues**:
- Too many small files (small file problem)
- Poor data layout
- Inefficient queries
- Lack of statistics
- No data skipping

**Benefits of Optimization**:
- Faster query performance
- Reduced storage costs
- Better resource utilization
- Improved data skipping
- Lower compute costs

### 2. Table Partitioning

Partitioning divides data into separate directories based on column values.

**When to Partition**:
- High cardinality columns (date, region, category)
- Frequently filtered columns
- Data naturally segments by column
- Query patterns align with partition columns

**When NOT to Partition**:
- Low cardinality (< 1000 unique values)
- Columns not used in WHERE clauses
- Too many partitions (> 10,000)
- Evenly distributed queries

#### Partitioning Syntax

```python
# Write with partitioning
(df.write
    .format("delta")
    .partitionBy("date", "region")
    .saveAsTable("sales")
)

# SQL
CREATE TABLE sales (
    sale_id INT,
    amount DOUBLE,
    date DATE,
    region STRING
)
USING DELTA
PARTITIONED BY (date, region)
```

#### Partition Pruning

```python
# Query with partition filter - only scans relevant partitions
spark.sql("""
    SELECT * FROM sales
    WHERE date = '2024-01-15'  -- Partition pruning!
      AND region = 'US'
""")

# Without partition filter - scans all partitions
spark.sql("""
    SELECT * FROM sales
    WHERE amount > 1000  -- No partition pruning
""")
```

#### Repartitioning Existing Tables

```python
# Read existing table
df = spark.table("sales")

# Repartition and write
(df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("date")
    .option("overwriteSchema", "true")
    .saveAsTable("sales")
)
```

### 3. Z-Ordering (Multi-Dimensional Clustering)

Z-ordering co-locates related data in the same files for better data skipping.

**What is Z-Ordering?**
- Organizes data using Z-order curve
- Co-locates similar values
- Improves data skipping
- Works with multiple columns

**When to Use Z-Ordering**:
- Frequently filtered columns
- High cardinality columns
- Columns used in JOIN conditions
- Columns in WHERE clauses

#### Z-Order Syntax

```sql
-- Z-order by single column
OPTIMIZE sales ZORDER BY (customer_id);

-- Z-order by multiple columns (max 4 recommended)
OPTIMIZE sales ZORDER BY (customer_id, product_id);

-- Z-order with partition
OPTIMIZE sales
WHERE date >= '2024-01-01'
ZORDER BY (customer_id);
```

#### Z-Ordering Best Practices

```python
# 1. Choose high-cardinality columns
OPTIMIZE sales ZORDER BY (customer_id)  # Good - high cardinality

# 2. Limit to 4 columns max
OPTIMIZE sales ZORDER BY (col1, col2, col3, col4)  # Max recommended

# 3. Order by query frequency
OPTIMIZE sales ZORDER BY (most_queried_col, second_most_queried)

# 4. Don't Z-order partition columns
# Partition by date, Z-order by customer_id (not date)
```

### 4. OPTIMIZE Command

OPTIMIZE compacts small files into larger files.

**What OPTIMIZE Does**:
- Combines small files
- Removes deleted rows
- Improves read performance
- Reduces metadata overhead

**When to Run OPTIMIZE**:
- After many small writes
- After DELETE/UPDATE operations
- Before important queries
- On schedule (daily/weekly)

#### OPTIMIZE Syntax

```sql
-- Basic OPTIMIZE
OPTIMIZE sales;

-- OPTIMIZE specific partition
OPTIMIZE sales
WHERE date = '2024-01-15';

-- OPTIMIZE with Z-ordering
OPTIMIZE sales ZORDER BY (customer_id);

-- OPTIMIZE with WHERE clause
OPTIMIZE sales
WHERE date >= '2024-01-01' AND date < '2024-02-01';
```

#### OPTIMIZE in Python

```python
from delta.tables import DeltaTable

# Basic OPTIMIZE
DeltaTable.forName(spark, "sales").optimize().executeCompaction()

# OPTIMIZE with Z-ordering
DeltaTable.forName(spark, "sales").optimize().executeZOrderBy("customer_id")

# OPTIMIZE specific partition
(DeltaTable.forName(spark, "sales")
    .optimize()
    .where("date = '2024-01-15'")
    .executeCompaction()
)
```

### 5. VACUUM Command

VACUUM removes old data files no longer referenced by the table.

**What VACUUM Does**:
- Deletes old file versions
- Frees up storage
- Removes files older than retention period
- Cannot be undone!

**Default Retention**: 7 days (168 hours)

#### VACUUM Syntax

```sql
-- VACUUM with default retention (7 days)
VACUUM sales;

-- VACUUM with custom retention
VACUUM sales RETAIN 168 HOURS;  -- 7 days

-- VACUUM specific partition
VACUUM sales WHERE date < '2024-01-01';

-- Dry run (see what would be deleted)
VACUUM sales DRY RUN;
```

#### VACUUM Safety

```python
# Check retention before VACUUM
spark.sql("DESCRIBE DETAIL sales").select("minReaderVersion", "minWriterVersion").show()

# Dry run first
spark.sql("VACUUM sales RETAIN 168 HOURS DRY RUN").show()

# Then actual VACUUM
spark.sql("VACUUM sales RETAIN 168 HOURS")
```

**VACUUM Warnings**:
- Cannot time travel beyond VACUUM retention
- Cannot restore VACUUMed files
- Affects concurrent readers
- Test with DRY RUN first

### 6. Data Skipping

Data skipping uses statistics to skip irrelevant files.

**How Data Skipping Works**:
1. Delta Lake collects statistics (min, max, null count)
2. Query optimizer checks statistics
3. Skips files that can't contain matching data
4. Only reads relevant files

#### Statistics Collection

```python
# Statistics are collected automatically
# View statistics
spark.sql("DESCRIBE DETAIL sales").show()

# Force statistics collection
spark.sql("ANALYZE TABLE sales COMPUTE STATISTICS")

# Column statistics
spark.sql("ANALYZE TABLE sales COMPUTE STATISTICS FOR COLUMNS customer_id, amount")
```

#### Optimizing for Data Skipping

```python
# 1. Use appropriate data types
# INT is better than STRING for numeric IDs

# 2. Partition by frequently filtered columns
.partitionBy("date")

# 3. Z-order by high-cardinality columns
OPTIMIZE sales ZORDER BY (customer_id)

# 4. Use specific filters
WHERE customer_id = 123  # Good - specific
WHERE customer_id IN (1,2,3)  # Good - specific
WHERE customer_id LIKE '%123%'  # Bad - can't skip
```

### 7. File Size Optimization

**Target File Size**: 128 MB - 1 GB per file

**Small File Problem**:
- Too many small files
- High metadata overhead
- Slow queries
- Inefficient I/O

**Large File Problem**:
- Can't parallelize well
- Memory issues
- Slow updates

#### Controlling File Size

```python
# Set target file size (bytes)
spark.conf.set("spark.databricks.delta.targetFileSize", "134217728")  # 128 MB

# Repartition before write
df.repartition(10).write.format("delta").save("/path")

# Coalesce for fewer files
df.coalesce(5).write.format("delta").save("/path")
```

### 8. Caching Strategies

#### Delta Cache

```python
# Enable Delta cache
spark.conf.set("spark.databricks.io.cache.enabled", "true")

# Cache table
spark.sql("CACHE SELECT * FROM sales WHERE date = '2024-01-15'")

# Uncache
spark.sql("UNCACHE TABLE sales")
```

#### DataFrame Caching

```python
# Cache DataFrame
df = spark.table("sales").filter(col("date") == "2024-01-15")
df.cache()
df.count()  # Trigger caching

# Persist with storage level
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)

# Unpersist
df.unpersist()
```

### 9. Query Optimization

#### Predicate Pushdown

```python
# Good - filter pushed to source
df = spark.table("sales").filter(col("date") == "2024-01-15")

# Bad - filter after loading all data
df = spark.table("sales")
df = df.filter(col("date") == "2024-01-15")
```

#### Column Pruning

```python
# Good - select only needed columns
df = spark.table("sales").select("customer_id", "amount")

# Bad - select all columns
df = spark.table("sales")
```

#### Broadcast Joins

```python
from pyspark.sql.functions import broadcast

# Broadcast small dimension table
large_df.join(broadcast(small_df), "key")
```

### 10. Complete Optimization Strategy

```python
class TableOptimizer:
    """Complete table optimization strategy"""
    
    def __init__(self, table_name):
        self.table_name = table_name
        self.table = DeltaTable.forName(spark, table_name)
        
    def analyze_table(self):
        """Analyze table statistics"""
        detail = spark.sql(f"DESCRIBE DETAIL {self.table_name}").first()
        
        print(f"Table: {self.table_name}")
        print(f"  Files: {detail['numFiles']}")
        print(f"  Size: {detail['sizeInBytes'] / 1024 / 1024:.2f} MB")
        print(f"  Partitions: {detail['partitionColumns']}")
        
        # Check for small files
        avg_file_size = detail['sizeInBytes'] / detail['numFiles'] if detail['numFiles'] > 0 else 0
        print(f"  Avg file size: {avg_file_size / 1024 / 1024:.2f} MB")
        
        if avg_file_size < 128 * 1024 * 1024:  # < 128 MB
            print("  ⚠ Small file problem detected!")
            
        return detail
    
    def optimize_table(self, zorder_cols=None):
        """Optimize table with optional Z-ordering"""
        print(f"\nOptimizing {self.table_name}...")
        
        if zorder_cols:
            self.table.optimize().executeZOrderBy(zorder_cols)
            print(f"  ✓ Optimized with Z-order: {zorder_cols}")
        else:
            self.table.optimize().executeCompaction()
            print("  ✓ Optimized (compaction only)")
    
    def vacuum_table(self, retention_hours=168, dry_run=True):
        """VACUUM table with safety checks"""
        print(f"\nVACUUMing {self.table_name}...")
        
        if dry_run:
            result = spark.sql(f"VACUUM {self.table_name} RETAIN {retention_hours} HOURS DRY RUN")
            print(f"  Dry run - would delete {result.count()} files")
        else:
            spark.sql(f"VACUUM {self.table_name} RETAIN {retention_hours} HOURS")
            print(f"  ✓ VACUUMed (retention: {retention_hours} hours)")
    
    def full_optimization(self, zorder_cols=None, vacuum=False):
        """Run complete optimization"""
        print("="*60)
        print("FULL TABLE OPTIMIZATION")
        print("="*60)
        
        # Analyze
        self.analyze_table()
        
        # Optimize
        self.optimize_table(zorder_cols)
        
        # Vacuum
        if vacuum:
            self.vacuum_table(dry_run=False)
        
        # Re-analyze
        print("\nAfter optimization:")
        self.analyze_table()
        
        print("="*60)

# Usage
optimizer = TableOptimizer("sales")
optimizer.full_optimization(zorder_cols=["customer_id", "product_id"], vacuum=True)
```

## Best Practices

1. **Partition wisely**: Use date/region, avoid over-partitioning
2. **Z-order frequently**: On high-cardinality filter columns
3. **OPTIMIZE regularly**: After writes, before important queries
4. **VACUUM carefully**: Test with DRY RUN, respect retention
5. **Monitor file sizes**: Target 128 MB - 1 GB per file
6. **Use data skipping**: Specific filters, appropriate data types
7. **Cache strategically**: Hot data, repeated queries
8. **Analyze statistics**: Keep statistics up to date
9. **Test optimizations**: Measure before and after
10. **Schedule maintenance**: Regular OPTIMIZE and VACUUM

## Key Takeaways

1. **Partitioning** divides data by column values
2. **Z-ordering** co-locates related data for skipping
3. **OPTIMIZE** compacts small files
4. **VACUUM** removes old file versions
5. **Data skipping** uses statistics to skip files
6. **Target file size** is 128 MB - 1 GB
7. **OPTIMIZE** should run regularly
8. **VACUUM** cannot be undone
9. **Z-order** max 4 columns recommended
10. **Measure** performance before and after

## Exam Tips

1. Know when to partition vs Z-order
2. Understand OPTIMIZE command and when to use it
3. Know VACUUM retention period (default 7 days)
4. Understand data skipping and statistics
5. Know target file size (128 MB - 1 GB)
6. Understand partition pruning
7. Know Z-ordering limitations (max 4 columns)
8. Understand VACUUM cannot be undone
9. Know how to check table statistics
10. Understand small file problem

## Additional Resources

- [Delta Lake Optimization](https://docs.delta.io/latest/optimizations-oss.html)
- [Z-Ordering](https://docs.databricks.com/delta/data-skipping.html)
- [VACUUM](https://docs.delta.io/latest/delta-utility.html#vacuum)

---

**Next**: Day 22 - Week 3 Review & Incremental Project
