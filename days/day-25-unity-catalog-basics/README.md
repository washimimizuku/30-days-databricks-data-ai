# Day 25: Unity Catalog Basics

## Overview

Today you'll learn about Unity Catalog, Databricks' unified governance solution for data and AI. Unity Catalog provides centralized access control, auditing, lineage, and data discovery across all your workspaces.

**Time**: 2 hours  
**Focus**: Data governance, catalogs, schemas, permissions, three-level namespace

## Learning Objectives

By the end of today, you will be able to:
- Understand Unity Catalog architecture and hierarchy
- Create and manage catalogs and schemas
- Work with the three-level namespace (catalog.schema.table)
- Grant and revoke permissions using SQL
- Implement data access controls
- Use external locations and storage credentials
- Query system tables for governance insights
- Understand data lineage and auditing

## What is Unity Catalog?

Unity Catalog is a unified governance solution that provides:
- **Centralized Access Control**: Single place to manage permissions
- **Data Discovery**: Search and explore data assets
- **Audit Logging**: Track all data access
- **Data Lineage**: Understand data flow and dependencies
- **Data Sharing**: Securely share data across organizations

### Key Benefits

**Unified Governance**: One governance model across all workspaces  
**Fine-Grained Access Control**: Column and row-level security  
**Automated Lineage**: Track data from source to consumption  
**Compliance**: Meet regulatory requirements (GDPR, HIPAA, etc.)  
**Data Discovery**: Find and understand data assets  

## Unity Catalog Architecture

### Hierarchy

Unity Catalog uses a three-level namespace:

```
Metastore (Account-level)
  └── Catalog (Workspace or cross-workspace)
      └── Schema (Database)
          └── Table/View/Function
```

### Metastore

The **metastore** is the top-level container:
- One metastore per region
- Stores metadata for all catalogs
- Account-level resource
- Can be shared across workspaces

```sql
-- View current metastore
SELECT current_metastore();

-- Metastore info
DESCRIBE METASTORE;
```

### Catalog

A **catalog** is the first level of organization:
- Contains schemas (databases)
- Can be workspace-specific or shared
- Supports external data sources

```sql
-- Create catalog
CREATE CATALOG IF NOT EXISTS sales_data;

-- Use catalog
USE CATALOG sales_data;

-- Show all catalogs
SHOW CATALOGS;

-- Describe catalog
DESCRIBE CATALOG sales_data;
```

### Schema

A **schema** (database) is the second level:
- Contains tables, views, and functions
- Organizes related objects

```sql
-- Create schema
CREATE SCHEMA IF NOT EXISTS sales_data.bronze;

-- Use schema
USE SCHEMA sales_data.bronze;

-- Show schemas in catalog
SHOW SCHEMAS IN sales_data;

-- Describe schema
DESCRIBE SCHEMA sales_data.bronze;
```

### Table

**Tables** are the third level:
- Managed or external
- Support Delta Lake features

```sql
-- Create table with three-level namespace
CREATE TABLE sales_data.bronze.orders (
    order_id INT,
    customer_id INT,
    order_date DATE,
    amount DECIMAL(10,2)
);

-- Query with full namespace
SELECT * FROM sales_data.bronze.orders;

-- Show tables
SHOW TABLES IN sales_data.bronze;
```

## Three-Level Namespace

### Fully Qualified Names

Unity Catalog requires three-level names:

```sql
-- Format: catalog.schema.table
SELECT * FROM sales_data.bronze.orders;

-- Without Unity Catalog (legacy):
-- Format: schema.table
SELECT * FROM bronze.orders;
```

### Setting Default Catalog and Schema

```sql
-- Set default catalog
USE CATALOG sales_data;

-- Set default schema
USE SCHEMA bronze;

-- Now you can use short names
SELECT * FROM orders;  -- Resolves to sales_data.bronze.orders

-- Check current defaults
SELECT current_catalog();
SELECT current_schema();
SELECT current_database();  -- Alias for current_schema()
```

### Namespace Best Practices

```sql
-- ✅ GOOD: Always use fully qualified names in production
CREATE TABLE prod_catalog.gold.customer_summary AS
SELECT * FROM prod_catalog.silver.customers;

-- ⚠️ RISKY: Relying on USE statements
USE CATALOG prod_catalog;
USE SCHEMA gold;
CREATE TABLE customer_summary AS  -- Could create in wrong location
SELECT * FROM silver.customers;   -- Ambiguous reference
```

## Access Control

### Permission Model

Unity Catalog uses a hierarchical permission model:

```
Metastore
  ├── CREATE CATALOG
  └── USE METASTORE
Catalog
  ├── USE CATALOG
  ├── CREATE SCHEMA
  └── OWNERSHIP
Schema
  ├── USE SCHEMA
  ├── CREATE TABLE
  ├── CREATE VIEW
  ├── CREATE FUNCTION
  └── OWNERSHIP
Table
  ├── SELECT
  ├── MODIFY
  └── OWNERSHIP
```

### Granting Permissions

```sql
-- Grant catalog access
GRANT USE CATALOG ON CATALOG sales_data TO `data_analysts`;

-- Grant schema access
GRANT USE SCHEMA ON SCHEMA sales_data.bronze TO `data_analysts`;

-- Grant table read access
GRANT SELECT ON TABLE sales_data.bronze.orders TO `data_analysts`;

-- Grant table write access
GRANT MODIFY ON TABLE sales_data.bronze.orders TO `data_engineers`;

-- Grant all privileges
GRANT ALL PRIVILEGES ON TABLE sales_data.gold.summary TO `admin_group`;

-- Grant to specific user
GRANT SELECT ON TABLE sales_data.gold.summary TO `user@company.com`;
```

### Revoking Permissions

```sql
-- Revoke specific permission
REVOKE SELECT ON TABLE sales_data.bronze.orders FROM `data_analysts`;

-- Revoke all permissions
REVOKE ALL PRIVILEGES ON TABLE sales_data.bronze.orders FROM `data_analysts`;
```

### Viewing Permissions

```sql
-- Show grants on a table
SHOW GRANTS ON TABLE sales_data.bronze.orders;

-- Show grants for a principal
SHOW GRANTS `data_analysts` ON CATALOG sales_data;

-- Show grants on catalog
SHOW GRANTS ON CATALOG sales_data;

-- Show grants on schema
SHOW GRANTS ON SCHEMA sales_data.bronze;
```

### Permission Inheritance

Permissions flow down the hierarchy:

```sql
-- Grant USE CATALOG
GRANT USE CATALOG ON CATALOG sales_data TO `analysts`;

-- Users still need USE SCHEMA
GRANT USE SCHEMA ON SCHEMA sales_data.bronze TO `analysts`;

-- And SELECT on tables
GRANT SELECT ON TABLE sales_data.bronze.orders TO `analysts`;

-- All three are required to query the table!
```

## Managed vs External Tables

### Managed Tables

Managed tables are fully controlled by Unity Catalog:

```sql
-- Create managed table
CREATE TABLE sales_data.bronze.customers (
    customer_id INT,
    name STRING,
    email STRING
) USING DELTA;

-- Data stored in Unity Catalog managed location
-- Dropping table deletes both metadata and data
DROP TABLE sales_data.bronze.customers;  -- Data is deleted!
```

### External Tables

External tables reference data in external locations:

```sql
-- Create external location (admin only)
CREATE EXTERNAL LOCATION my_s3_bucket
URL 's3://my-bucket/data/'
WITH (STORAGE CREDENTIAL my_aws_credential);

-- Create external table
CREATE EXTERNAL TABLE sales_data.bronze.external_orders (
    order_id INT,
    amount DECIMAL(10,2)
)
LOCATION 's3://my-bucket/data/orders/';

-- Dropping table only removes metadata, not data
DROP TABLE sales_data.bronze.external_orders;  -- Data remains in S3
```

## Storage Credentials and External Locations

### Storage Credentials

Storage credentials provide access to cloud storage:

```sql
-- Create storage credential (admin only)
CREATE STORAGE CREDENTIAL my_aws_credential
WITH (
    AWS_IAM_ROLE = 'arn:aws:iam::123456789:role/databricks-access'
);

-- Show storage credentials
SHOW STORAGE CREDENTIALS;

-- Describe credential
DESCRIBE STORAGE CREDENTIAL my_aws_credential;
```

### External Locations

External locations define accessible storage paths:

```sql
-- Create external location
CREATE EXTERNAL LOCATION bronze_data
URL 's3://company-data/bronze/'
WITH (STORAGE CREDENTIAL my_aws_credential);

-- Grant access to external location
GRANT READ FILES ON EXTERNAL LOCATION bronze_data TO `data_engineers`;
GRANT WRITE FILES ON EXTERNAL LOCATION bronze_data TO `data_engineers`;

-- Show external locations
SHOW EXTERNAL LOCATIONS;
```

## System Tables

Unity Catalog provides system tables for governance:

### Access Audit

```sql
-- Query access audit logs
SELECT 
    event_time,
    user_identity.email,
    request_params.full_name_arg,
    action_name
FROM system.access.audit
WHERE action_name = 'getTable'
    AND event_date >= current_date() - 7
ORDER BY event_time DESC;
```

### Table Lineage

```sql
-- Query lineage information
SELECT 
    source_table_full_name,
    target_table_full_name,
    source_type,
    target_type
FROM system.access.table_lineage
WHERE target_table_full_name = 'sales_data.gold.customer_summary';
```

### Billing Usage

```sql
-- Query usage data
SELECT 
    usage_date,
    sku_name,
    usage_quantity,
    usage_unit
FROM system.billing.usage
WHERE usage_date >= current_date() - 30
ORDER BY usage_date DESC;
```

## Data Lineage

Unity Catalog automatically captures lineage:

```sql
-- Create tables with lineage
CREATE TABLE sales_data.bronze.raw_orders AS
SELECT * FROM csv.`/data/orders.csv`;

CREATE TABLE sales_data.silver.clean_orders AS
SELECT 
    order_id,
    customer_id,
    CAST(order_date AS DATE) as order_date,
    amount
FROM sales_data.bronze.raw_orders
WHERE amount > 0;

CREATE TABLE sales_data.gold.order_summary AS
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_amount
FROM sales_data.silver.clean_orders
GROUP BY customer_id;

-- Lineage is automatically tracked:
-- raw_orders → clean_orders → order_summary
```

## Data Discovery

### Searching for Data

```sql
-- Search for tables
SHOW TABLES IN sales_data.bronze LIKE 'order*';

-- Search across catalogs
SHOW TABLES IN CATALOG sales_data LIKE '*customer*';

-- Describe table for discovery
DESCRIBE TABLE EXTENDED sales_data.bronze.orders;

-- Show table properties
SHOW TBLPROPERTIES sales_data.bronze.orders;
```

### Table Comments and Tags

```sql
-- Add table comment
COMMENT ON TABLE sales_data.bronze.orders IS 
'Raw order data from e-commerce platform';

-- Add column comments
ALTER TABLE sales_data.bronze.orders 
ALTER COLUMN order_id COMMENT 'Unique order identifier';

-- Set table properties (tags)
ALTER TABLE sales_data.bronze.orders 
SET TBLPROPERTIES (
    'data_classification' = 'internal',
    'pii' = 'false',
    'owner' = 'data-engineering-team'
);
```

## Best Practices

### Naming Conventions

```sql
-- Use clear, descriptive names
-- ✅ GOOD
CREATE CATALOG prod_sales_data;
CREATE SCHEMA prod_sales_data.bronze_raw;
CREATE TABLE prod_sales_data.bronze_raw.orders_2024;

-- ❌ BAD
CREATE CATALOG c1;
CREATE SCHEMA c1.s1;
CREATE TABLE c1.s1.t1;
```

### Catalog Organization

```sql
-- Organize by environment
CREATE CATALOG dev_analytics;
CREATE CATALOG staging_analytics;
CREATE CATALOG prod_analytics;

-- Or by domain
CREATE CATALOG sales_data;
CREATE CATALOG marketing_data;
CREATE CATALOG finance_data;

-- Use schemas for layers
CREATE SCHEMA sales_data.bronze;
CREATE SCHEMA sales_data.silver;
CREATE SCHEMA sales_data.gold;
```

### Permission Strategy

```sql
-- Create groups for roles
-- Analysts: Read-only access to gold tables
GRANT USE CATALOG ON CATALOG sales_data TO `analysts`;
GRANT USE SCHEMA ON SCHEMA sales_data.gold TO `analysts`;
GRANT SELECT ON SCHEMA sales_data.gold TO `analysts`;

-- Engineers: Full access to bronze/silver, read gold
GRANT USE CATALOG ON CATALOG sales_data TO `engineers`;
GRANT USE SCHEMA ON SCHEMA sales_data.bronze TO `engineers`;
GRANT USE SCHEMA ON SCHEMA sales_data.silver TO `engineers`;
GRANT ALL PRIVILEGES ON SCHEMA sales_data.bronze TO `engineers`;
GRANT ALL PRIVILEGES ON SCHEMA sales_data.silver TO `engineers`;
GRANT SELECT ON SCHEMA sales_data.gold TO `engineers`;

-- Admins: Full access everywhere
GRANT ALL PRIVILEGES ON CATALOG sales_data TO `admins`;
```

### Security Best Practices

```sql
-- 1. Principle of least privilege
-- Only grant necessary permissions
GRANT SELECT ON TABLE sales_data.gold.summary TO `analysts`;
-- Don't grant: GRANT ALL PRIVILEGES

-- 2. Use groups, not individual users
GRANT SELECT ON TABLE sales_data.gold.summary TO `analyst_group`;
-- Don't: GRANT SELECT ... TO `user1@company.com`;

-- 3. Regular permission audits
SHOW GRANTS ON CATALOG sales_data;

-- 4. Document ownership
COMMENT ON CATALOG sales_data IS 'Owner: Data Engineering Team';

-- 5. Use external locations for sensitive data
CREATE EXTERNAL LOCATION secure_pii
URL 's3://secure-bucket/pii/'
WITH (STORAGE CREDENTIAL secure_credential);
```

## Common Patterns

### Multi-Environment Setup

```sql
-- Development environment
CREATE CATALOG dev_sales;
CREATE SCHEMA dev_sales.bronze;
CREATE SCHEMA dev_sales.silver;
CREATE SCHEMA dev_sales.gold;

-- Production environment
CREATE CATALOG prod_sales;
CREATE SCHEMA prod_sales.bronze;
CREATE SCHEMA prod_sales.silver;
CREATE SCHEMA prod_sales.gold;

-- Promote from dev to prod
CREATE TABLE prod_sales.gold.customer_summary AS
SELECT * FROM dev_sales.gold.customer_summary;
```

### Cross-Catalog Queries

```sql
-- Join tables from different catalogs
SELECT 
    s.customer_id,
    s.total_sales,
    m.campaign_response
FROM prod_sales.gold.customer_summary s
JOIN prod_marketing.gold.campaign_results m
    ON s.customer_id = m.customer_id;
```

### Data Sharing Pattern

```sql
-- Create shared catalog
CREATE CATALOG shared_analytics;

-- Grant access to multiple teams
GRANT USE CATALOG ON CATALOG shared_analytics TO `sales_team`;
GRANT USE CATALOG ON CATALOG shared_analytics TO `marketing_team`;

-- Create shared views with row-level security
CREATE VIEW shared_analytics.gold.regional_sales AS
SELECT *
FROM prod_sales.gold.sales
WHERE region = current_user_region();  -- Function returns user's region
```

## Exam Tips

### Key Concepts to Remember

1. **Three-level namespace**: catalog.schema.table
2. **Permission hierarchy**: Metastore → Catalog → Schema → Table
3. **Managed vs External**: Managed = UC controls data, External = data stays in place
4. **Required permissions**: USE CATALOG + USE SCHEMA + SELECT to query
5. **System tables**: system.access.audit, system.access.table_lineage

### Common Exam Questions

**Q: What permissions are needed to query a table?**
A: USE CATALOG, USE SCHEMA, and SELECT on the table

**Q: What happens when you DROP a managed table?**
A: Both metadata and data are deleted

**Q: What happens when you DROP an external table?**
A: Only metadata is deleted, data remains

**Q: How do you grant access to all tables in a schema?**
A: `GRANT SELECT ON SCHEMA catalog.schema TO principal`

**Q: What is the top level of Unity Catalog hierarchy?**
A: Metastore (account-level)

## Summary

Today you learned:
- ✅ Unity Catalog architecture and three-level namespace
- ✅ Creating and managing catalogs and schemas
- ✅ Granting and revoking permissions
- ✅ Managed vs external tables
- ✅ Storage credentials and external locations
- ✅ System tables for governance
- ✅ Data lineage and discovery
- ✅ Best practices for organization and security

## Next Steps

Tomorrow (Day 26) you'll learn about:
- Data quality checks and validation
- Testing data pipelines
- Great Expectations integration
- Monitoring data quality metrics

## Additional Resources

- [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/unity-catalog/best-practices.html)
- [Unity Catalog Privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/index.html)
- [System Tables Reference](https://docs.databricks.com/administration-guide/system-tables/index.html)

---

**Estimated Time**: 2 hours  
**Difficulty**: Intermediate  
**Prerequisites**: Days 1-24 (especially Day 5: Databases, Tables, and Views)
