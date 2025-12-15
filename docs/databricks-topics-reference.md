# Databricks Certified Data Engineer Associate - Topics & Documentation Reference

## Databricks Lakehouse Platform (15% - Days 1-5)

### Databricks Architecture & Workspace
- **Databricks Architecture**: https://docs.databricks.com/getting-started/overview.html
- **Workspace Administration**: https://docs.databricks.com/administration-guide/workspace/index.html
- **Notebooks**: https://docs.databricks.com/notebooks/index.html
- **Repos**: https://docs.databricks.com/repos/index.html

### Clusters & Compute
- **Clusters Overview**: https://docs.databricks.com/clusters/index.html
- **Cluster Configuration**: https://docs.databricks.com/clusters/configure.html
- **Cluster Policies**: https://docs.databricks.com/administration-guide/clusters/policies.html
- **Databricks Runtime**: https://docs.databricks.com/runtime/index.html
- **SQL Warehouses**: https://docs.databricks.com/sql/admin/sql-endpoints.html

### Delta Lake Fundamentals
- **Delta Lake Overview**: https://docs.databricks.com/delta/index.html
- **Delta Lake Quickstart**: https://docs.databricks.com/delta/quick-start.html
- **Time Travel**: https://docs.databricks.com/delta/history.html
- **Schema Evolution**: https://docs.databricks.com/delta/schema-evolution.html
- **ACID Transactions**: https://docs.databricks.com/delta/concurrency-control.html

### Databricks File System (DBFS)
- **DBFS Overview**: https://docs.databricks.com/dbfs/index.html
- **DBFS CLI**: https://docs.databricks.com/dev-tools/cli/dbfs-cli.html
- **File System Utilities**: https://docs.databricks.com/dev-tools/databricks-utils.html#file-system-utilities
- **External Locations**: https://docs.databricks.com/external-data/index.html

### Databases, Tables, and Views
- **Databases and Tables**: https://docs.databricks.com/data/tables.html
- **Managed vs External Tables**: https://docs.databricks.com/data/tables.html#managed-and-unmanaged-tables
- **Views**: https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-view.html
- **Table Utilities**: https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-utility-commands.html

## ELT with Spark SQL and Python (30% - Days 6-15)

### Spark SQL Fundamentals
- **Spark SQL Guide**: https://docs.databricks.com/spark/latest/spark-sql/index.html
- **SQL Language Manual**: https://docs.databricks.com/spark/latest/spark-sql/language-manual/index.html
- **SELECT Statement**: https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-qry-select.html
- **JOIN Operations**: https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-qry-select-join.html

### Advanced SQL Functions
- **Window Functions**: https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-functions-builtin.html#window-functions
- **Higher-Order Functions**: https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-functions-builtin.html#higher-order-functions
- **Array Functions**: https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-functions-builtin.html#array-functions
- **Built-in Functions**: https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-functions-builtin.html

### PySpark DataFrames
- **PySpark Overview**: https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-python.html
- **DataFrame API**: https://docs.databricks.com/spark/latest/dataframes-datasets/index.html
- **Reading Data**: https://docs.databricks.com/spark/latest/dataframes-datasets/read-csv.html
- **Writing Data**: https://docs.databricks.com/spark/latest/dataframes-datasets/write-to-delta.html
- **User-Defined Functions**: https://docs.databricks.com/spark/latest/spark-sql/udf-python.html

### Data Ingestion
- **COPY INTO**: https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-copy-into.html
- **Auto Loader**: https://docs.databricks.com/ingestion/auto-loader/index.html
- **File Formats**: https://docs.databricks.com/data/data-sources/index.html
- **Schema Inference**: https://docs.databricks.com/ingestion/auto-loader/schema.html

### Data Transformation
- **Data Cleaning**: https://docs.databricks.com/spark/latest/dataframes-datasets/data-cleaning.html
- **Data Types**: https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-datatypes.html
- **String Functions**: https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-functions-builtin.html#string-functions
- **Date/Time Functions**: https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-functions-builtin.html#datetime-functions

### ELT Architecture
- **Medallion Architecture**: https://docs.databricks.com/lakehouse/medallion.html
- **Data Engineering Patterns**: https://docs.databricks.com/data-engineering/index.html
- **Best Practices**: https://docs.databricks.com/best-practices/index.html

## Incremental Data Processing (25% - Days 16-22)

### Structured Streaming
- **Structured Streaming Overview**: https://docs.databricks.com/structured-streaming/index.html
- **Streaming Concepts**: https://docs.databricks.com/structured-streaming/concepts.html
- **Data Sources**: https://docs.databricks.com/structured-streaming/data-sources.html
- **Output Modes**: https://docs.databricks.com/structured-streaming/output-modes.html
- **Checkpointing**: https://docs.databricks.com/structured-streaming/production.html#checkpointing

### Streaming Transformations
- **Windowing**: https://docs.databricks.com/structured-streaming/windowing.html
- **Watermarking**: https://docs.databricks.com/structured-streaming/watermarking.html
- **Stateful Operations**: https://docs.databricks.com/structured-streaming/stateful-streaming.html
- **Triggers**: https://docs.databricks.com/structured-streaming/triggers.html

### Delta Lake Advanced Features
- **MERGE Operations**: https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-merge-into.html
- **Change Data Feed**: https://docs.databricks.com/delta/delta-change-data-feed.html
- **Streaming with Delta**: https://docs.databricks.com/structured-streaming/delta-lake.html
- **Upserts**: https://docs.databricks.com/delta/merge.html

### Change Data Capture (CDC)
- **CDC Patterns**: https://docs.databricks.com/delta/delta-change-data-feed.html
- **Slowly Changing Dimensions**: https://docs.databricks.com/delta/merge.html#slowly-changing-data-scd-type-2-operation-into-delta-tables
- **Processing Changes**: https://docs.databricks.com/structured-streaming/delta-lake.html#process-change-data

### Multi-Hop Architecture
- **Bronze Silver Gold**: https://docs.databricks.com/lakehouse/medallion.html
- **Incremental Processing**: https://docs.databricks.com/delta/merge.html
- **Pipeline Design**: https://docs.databricks.com/data-engineering/index.html

### Performance Optimization
- **Partitioning**: https://docs.databricks.com/delta/partitions.html
- **Z-Ordering**: https://docs.databricks.com/delta/optimizations/file-mgmt.html#z-ordering-multi-dimensional-clustering
- **OPTIMIZE**: https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html
- **VACUUM**: https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-vacuum.html
- **Data Skipping**: https://docs.databricks.com/delta/optimizations/file-mgmt.html#data-skipping

## Production Pipelines (20% - Days 23-27)

### Databricks Jobs
- **Jobs Overview**: https://docs.databricks.com/jobs/index.html
- **Job Configuration**: https://docs.databricks.com/jobs/jobs.html
- **Task Orchestration**: https://docs.databricks.com/jobs/jobs.html#task-orchestration
- **Job Scheduling**: https://docs.databricks.com/jobs/jobs.html#schedule-a-job
- **Job Monitoring**: https://docs.databricks.com/jobs/jobs.html#monitor-job-runs

### Version Control & CI/CD
- **Repos**: https://docs.databricks.com/repos/index.html
- **Git Integration**: https://docs.databricks.com/repos/git-operations-with-repos.html
- **CI/CD Best Practices**: https://docs.databricks.com/dev-tools/ci-cd/ci-cd-jenkins.html
- **Deployment Patterns**: https://docs.databricks.com/best-practices/deployment.html

### Delta Live Tables
- **DLT Overview**: https://docs.databricks.com/workflows/delta-live-tables/index.html
- **DLT Pipelines**: https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-quickstart.html
- **Data Quality**: https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html
- **Pipeline Monitoring**: https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-observability.html

### Testing & Quality
- **Data Quality Checks**: https://docs.databricks.com/delta/delta-constraints.html
- **Testing Strategies**: https://docs.databricks.com/best-practices/testing.html
- **Monitoring**: https://docs.databricks.com/administration-guide/workspace-settings/index.html#monitoring

### Production Best Practices
- **Error Handling**: https://docs.databricks.com/structured-streaming/production.html#handle-errors
- **Logging**: https://docs.databricks.com/clusters/log-delivery.html
- **Performance Tuning**: https://docs.databricks.com/optimizations/index.html
- **Cost Optimization**: https://docs.databricks.com/administration-guide/account-settings/usage.html
- **Security**: https://docs.databricks.com/security/index.html

## Data Governance - Unity Catalog (10% - Integrated)

### Unity Catalog Fundamentals
- **Unity Catalog Overview**: https://docs.databricks.com/data-governance/unity-catalog/index.html
- **Metastore**: https://docs.databricks.com/data-governance/unity-catalog/create-metastore.html
- **Catalogs and Schemas**: https://docs.databricks.com/data-governance/unity-catalog/create-catalogs.html
- **Tables and Views**: https://docs.databricks.com/data-governance/unity-catalog/create-tables.html

### Access Control
- **Privileges**: https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/index.html
- **GRANT/REVOKE**: https://docs.databricks.com/spark/latest/spark-sql/language-manual/security-grant.html
- **Row-Level Security**: https://docs.databricks.com/security/access-control/table-acls/object-privileges.html
- **Column Masking**: https://docs.databricks.com/security/privacy/column-level-encryption.html

### Data Lineage & Discovery
- **Data Lineage**: https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html
- **Data Discovery**: https://docs.databricks.com/data-governance/unity-catalog/data-discovery.html
- **Tagging**: https://docs.databricks.com/data-governance/unity-catalog/tags.html

## Additional Resources

### Exam Preparation
- **Exam Guide**: https://www.databricks.com/learn/certification/data-engineer-associate
- **Practice Exams**: https://www.databricks.com/learn/certification/practice-exams
- **Databricks Academy**: https://academy.databricks.com/
- **Learning Paths**: https://academy.databricks.com/learning-paths

### Hands-On Practice
- **Community Edition**: https://community.cloud.databricks.com/
- **Quickstart Tutorials**: https://docs.databricks.com/getting-started/quick-start.html
- **Sample Datasets**: https://docs.databricks.com/data/databricks-datasets.html

### Reference Materials
- **Spark SQL Reference**: https://docs.databricks.com/spark/latest/spark-sql/language-manual/index.html
- **PySpark API**: https://spark.apache.org/docs/latest/api/python/
- **Delta Lake API**: https://docs.delta.io/latest/api/python/index.html
- **Databricks CLI**: https://docs.databricks.com/dev-tools/cli/index.html

### Best Practices Guides
- **Data Engineering**: https://docs.databricks.com/best-practices/data-engineering.html
- **Performance Tuning**: https://docs.databricks.com/optimizations/index.html
- **Security**: https://docs.databricks.com/security/index.html
- **Cost Optimization**: https://docs.databricks.com/administration-guide/account-settings/usage.html

### Community Resources
- **Databricks Community**: https://community.databricks.com/
- **Stack Overflow**: https://stackoverflow.com/questions/tagged/databricks
- **GitHub Examples**: https://github.com/databricks/tech-talks
- **Blog Posts**: https://databricks.com/blog/category/engineering
