# Day 1: Databricks Architecture & Workspace

## Learning Objectives

By the end of today, you will:
- Understand Databricks architecture (control plane vs data plane)
- Navigate the Databricks workspace UI
- Create and organize notebooks
- Understand workspace objects (folders, notebooks, repos)
- Know the difference between Databricks editions

## Topics Covered

### 1. Databricks Architecture

**Control Plane**:
- Managed by Databricks
- Hosts workspace UI, notebooks, jobs
- Stores metadata and configurations
- Handles authentication and authorization

**Data Plane**:
- Runs in your cloud account (AWS, Azure, GCP)
- Contains compute resources (clusters)
- Processes your data
- Stores data in your cloud storage

### 2. Workspace Organization

**Workspace Structure**:
```
Workspace
├── Users/
│   └── your-email@domain.com/
│       ├── Notebooks
│       ├── Folders
│       └── Files
├── Shared/
├── Repos/
└── Recycle Bin/
```

**Key Concepts**:
- **Workspace**: Your Databricks environment
- **Folders**: Organize notebooks and files
- **Notebooks**: Interactive documents with code, visualizations, and text
- **Repos**: Git integration for version control

### 3. Databricks UI Navigation

**Left Sidebar**:
- **Workspace**: Browse folders and notebooks
- **Repos**: Git repository integration
- **Compute**: Manage clusters
- **Workflows**: Create and manage jobs
- **Data**: Browse databases and tables
- **SQL**: SQL editor and dashboards
- **Machine Learning**: ML experiments and models

### 4. Notebooks

**Notebook Features**:
- Multiple languages: SQL, Python, Scala, R
- Magic commands: `%sql`, `%python`, `%scala`, `%r`, `%md`, `%sh`
- Visualizations: Built-in charts and graphs
- Collaboration: Comments and sharing
- Version history: Track changes

**Notebook Structure**:
- Cells: Individual code or markdown blocks
- Commands: Executable code within cells
- Results: Output displayed below cells

### 5. Databricks Editions

| Feature | Community Edition | Standard | Premium | Enterprise |
|---------|------------------|----------|---------|------------|
| **Cost** | Free | Paid | Paid | Paid |
| **Clusters** | Single node only | All types | All types | All types |
| **Users** | 1 | Multiple | Multiple | Multiple |
| **RBAC** | No | Basic | Advanced | Advanced |
| **Unity Catalog** | No | No | Yes | Yes |
| **Support** | Community | Standard | Premium | Enterprise |

## Hands-On Exercises

See `exercise.sql` for practical exercises.

## Key Takeaways

1. Databricks separates control plane (managed) from data plane (your cloud)
2. Workspace is organized hierarchically with folders and notebooks
3. Notebooks support multiple languages and magic commands
4. Community Edition is free but limited to single-node clusters
5. Understanding the UI is essential for efficient work

## Exam Tips

- Know the difference between control plane and data plane
- Understand workspace organization
- Be familiar with notebook features and magic commands
- Know the limitations of different Databricks editions

## Additional Resources

- [Databricks Architecture Overview](https://docs.databricks.com/getting-started/overview.html)
- [Workspace Guide](https://docs.databricks.com/workspace/index.html)
- [Notebooks Guide](https://docs.databricks.com/notebooks/index.html)

## Next Steps

Tomorrow: [Day 2 - Clusters & Compute](../day-02-clusters-compute/README.md)
