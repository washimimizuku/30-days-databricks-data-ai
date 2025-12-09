# Day 25 Quiz: Unity Catalog Basics

Test your knowledge of Unity Catalog concepts.

---

## Question 1
**What is the hierarchy of Unity Catalog?**

A) Database → Schema → Table  
B) Metastore → Catalog → Schema → Table  
C) Catalog → Database → Table  
D) Workspace → Catalog → Table  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Metastore → Catalog → Schema → Table**

Explanation: Unity Catalog uses a four-level hierarchy: Metastore (account-level) → Catalog → Schema (database) → Table/View/Function.
</details>

---

## Question 2
**What is the three-level namespace format in Unity Catalog?**

A) workspace.schema.table  
B) catalog.schema.table  
C) metastore.catalog.table  
D) database.schema.table  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) catalog.schema.table**

Explanation: Unity Catalog uses a three-level namespace: catalog.schema.table (e.g., sales_data.bronze.orders).
</details>

---

## Question 3
**Which three permissions are required to query a table in Unity Catalog?**

A) READ, WRITE, EXECUTE  
B) USE CATALOG, USE SCHEMA, SELECT  
C) GRANT, SELECT, EXECUTE  
D) USE METASTORE, USE CATALOG, SELECT  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) USE CATALOG, USE SCHEMA, SELECT**

Explanation: To query a table, you need: USE CATALOG (to access the catalog), USE SCHEMA (to access the schema), and SELECT (to read the table).
</details>

---

## Question 4
**What happens when you DROP a managed table in Unity Catalog?**

A) Only metadata is deleted, data remains  
B) Both metadata and data are deleted  
C) Only data is deleted, metadata remains  
D) Nothing happens, DROP is not allowed  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Both metadata and data are deleted**

Explanation: Managed tables are fully controlled by Unity Catalog. Dropping a managed table deletes both the metadata and the underlying data files.
</details>

---

## Question 5
**What happens when you DROP an external table in Unity Catalog?**

A) Only metadata is deleted, data remains in external location  
B) Both metadata and data are deleted  
C) Only data is deleted, metadata remains  
D) An error occurs, external tables cannot be dropped  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) Only metadata is deleted, data remains in external location**

Explanation: External tables reference data in external storage. Dropping an external table only removes the metadata; the data files remain in their original location.
</details>

---

## Question 6
**How do you grant SELECT permission on a table in Unity Catalog?**

A) `GRANT SELECT TO users ON TABLE catalog.schema.table`  
B) `GRANT SELECT ON TABLE catalog.schema.table TO users`  
C) `GRANT TABLE SELECT catalog.schema.table TO users`  
D) `GRANT users SELECT ON catalog.schema.table`  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) GRANT SELECT ON TABLE catalog.schema.table TO users**

Explanation: The correct syntax is `GRANT privilege ON object_type object_name TO principal`.
</details>

---

## Question 7
**What is a metastore in Unity Catalog?**

A) A table that stores metadata  
B) The top-level container for organizing data, account-level resource  
C) A type of database  
D) A storage location for data files  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) The top-level container for organizing data, account-level resource**

Explanation: The metastore is the top-level container in Unity Catalog, typically one per region, and stores metadata for all catalogs. It's an account-level resource.
</details>

---

## Question 8
**Which SQL command shows all grants on a specific table?**

A) `LIST GRANTS ON TABLE table_name`  
B) `SHOW GRANTS ON TABLE table_name`  
C) `DESCRIBE GRANTS table_name`  
D) `GET GRANTS FOR table_name`  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) SHOW GRANTS ON TABLE table_name**

Explanation: `SHOW GRANTS ON TABLE table_name` displays all permissions granted on the specified table.
</details>

---

## Question 9
**What is the purpose of storage credentials in Unity Catalog?**

A) To encrypt data at rest  
B) To provide access to cloud storage (S3, ADLS, GCS)  
C) To authenticate users  
D) To store database passwords  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To provide access to cloud storage (S3, ADLS, GCS)**

Explanation: Storage credentials provide Unity Catalog with the necessary permissions to access external cloud storage locations.
</details>

---

## Question 10
**What is an external location in Unity Catalog?**

A) A table stored outside the workspace  
B) A defined storage path with associated credentials  
C) A remote database connection  
D) A backup location for data  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) A defined storage path with associated credentials**

Explanation: An external location defines a specific storage path (e.g., S3 bucket) and associates it with storage credentials, allowing controlled access to external data.
</details>

---

## Question 11
**Which system table provides audit logs in Unity Catalog?**

A) system.logs.audit  
B) system.access.audit  
C) system.security.audit  
D) system.catalog.audit  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) system.access.audit**

Explanation: The `system.access.audit` table contains audit logs tracking all data access and operations in Unity Catalog.
</details>

---

## Question 12
**How do you revoke a permission in Unity Catalog?**

A) `REVOKE privilege FROM principal ON object`  
B) `REVOKE privilege ON object FROM principal`  
C) `REMOVE privilege ON object FROM principal`  
D) `DELETE GRANT privilege ON object FROM principal`  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) REVOKE privilege ON object FROM principal**

Explanation: The syntax is `REVOKE privilege ON object_type object_name FROM principal`, mirroring the GRANT syntax.
</details>

---

## Question 13
**What does the USE CATALOG command do?**

A) Grants permission to use a catalog  
B) Sets the default catalog for the session  
C) Creates a new catalog  
D) Activates a catalog  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) Sets the default catalog for the session**

Explanation: `USE CATALOG catalog_name` sets the default catalog for your session, allowing you to use shorter table references.
</details>

---

## Question 14
**Which of the following is true about Unity Catalog permissions?**

A) Permissions are inherited from parent to child objects  
B) Permissions must be granted at every level independently  
C) Only table-level permissions are supported  
D) All users have read access by default  

<details>
<summary>Click to reveal answer</summary>

**Answer: A) Permissions are inherited from parent to child objects**

Explanation: Unity Catalog uses hierarchical permissions. For example, granting USE CATALOG allows access to see the catalog, but you still need USE SCHEMA and SELECT to query tables.
</details>

---

## Question 15
**What is the purpose of table properties in Unity Catalog?**

A) To improve query performance  
B) To store metadata and tags for governance  
C) To define table partitions  
D) To set table permissions  

<details>
<summary>Click to reveal answer</summary>

**Answer: B) To store metadata and tags for governance**

Explanation: Table properties (set with `SET TBLPROPERTIES`) store custom metadata like data classification, owner, PII flags, etc., for governance and discovery.
</details>

---

## Scoring Guide

- **13-15 correct (87-100%)**: Excellent! You've mastered Unity Catalog basics.
- **11-12 correct (73-80%)**: Good job! Minor review recommended.
- **9-10 correct (60-67%)**: Fair. Review the material again.
- **Below 9 (< 60%)**: Re-read Day 25 content thoroughly.

---

## Key Concepts to Remember

1. **Three-level namespace**: catalog.schema.table
2. **Permission hierarchy**: USE CATALOG + USE SCHEMA + SELECT required to query
3. **Managed vs External**: Managed = UC controls data, External = data stays in place
4. **Metastore**: Top-level account resource containing all catalogs
5. **System tables**: system.access.audit for audit logs, system.access.table_lineage for lineage
6. **Storage credentials**: Provide access to external cloud storage
7. **External locations**: Define accessible storage paths with credentials
8. **GRANT/REVOKE**: Syntax is `GRANT privilege ON object TO principal`

---

## Next Steps

- Review any questions you got wrong
- Re-read relevant sections in Day 25 README
- Practice creating catalogs and granting permissions
- Move on to Day 26: Data Quality & Testing

