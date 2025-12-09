# Sample Data

This folder contains sample datasets for the bootcamp exercises.

## Available Datasets

### 1. employees.csv
Sample employee data for basic SQL exercises.

**Columns**:
- employee_id (INT)
- first_name (STRING)
- last_name (STRING)
- department (STRING)
- salary (DECIMAL)
- hire_date (DATE)

**Usage**: Days 1-7 (SQL basics)

### 2. orders.json
Sample order data for JSON processing.

**Columns**:
- order_id (INT)
- customer_id (INT)
- order_date (TIMESTAMP)
- amount (DECIMAL)
- status (STRING)

**Usage**: Days 10-15 (DataFrames and transformations)

### 3. streaming_events/
Folder with sample streaming data files.

**Format**: JSON
**Usage**: Days 16-20 (Streaming exercises)

## Using Databricks Sample Datasets

Databricks provides built-in sample datasets that you can use:

```python
# List available datasets
dbutils.fs.ls("/databricks-datasets/")

# Common datasets:
display(dbutils.fs.ls("/databricks-datasets/samples/"))
display(dbutils.fs.ls("/databricks-datasets/nyctaxi/"))
display(dbutils.fs.ls("/databricks-datasets/COVID/"))
```

## Loading Data into Databricks

### Method 1: Upload via UI
1. Click "Data" in left sidebar
2. Click "Create Table"
3. Upload file from your computer
4. Follow wizard to create table

### Method 2: DBFS Upload
```python
# Upload file to DBFS
dbutils.fs.cp("file:/path/to/local/file.csv", "dbfs:/FileStore/data/file.csv")
```

### Method 3: Read from GitHub
```python
# Read CSV from URL
df = spark.read.csv(
    "https://raw.githubusercontent.com/your-repo/data/employees.csv",
    header=True,
    inferSchema=True
)
```

## Creating Sample Data

### Generate Employee Data
```python
from pyspark.sql.functions import *
from datetime import datetime, timedelta

# Generate sample employees
employees = spark.range(1, 101).select(
    col("id").alias("employee_id"),
    concat(lit("First"), col("id")).alias("first_name"),
    concat(lit("Last"), col("id")).alias("last_name"),
    when(col("id") % 4 == 0, "Engineering")
    .when(col("id") % 4 == 1, "Sales")
    .when(col("id") % 4 == 2, "Marketing")
    .otherwise("Operations").alias("department"),
    (rand() * 50000 + 50000).cast("decimal(10,2)").alias("salary"),
    date_sub(current_date(), (rand() * 1000).cast("int")).alias("hire_date")
)

# Save as Delta table
employees.write.format("delta").mode("overwrite").saveAsTable("employees")
```

### Generate Order Data
```python
# Generate sample orders
orders = spark.range(1, 1001).select(
    col("id").alias("order_id"),
    (rand() * 100 + 1).cast("int").alias("customer_id"),
    date_sub(current_timestamp(), (rand() * 365).cast("int")).alias("order_date"),
    (rand() * 500 + 10).cast("decimal(10,2)").alias("amount"),
    when(rand() > 0.8, "cancelled")
    .when(rand() > 0.5, "completed")
    .otherwise("pending").alias("status")
)

# Save as Delta table
orders.write.format("delta").mode("overwrite").saveAsTable("orders")
```

### Generate Streaming Data
```python
import time
import json

# Generate streaming events
for i in range(100):
    event = {
        "event_id": i,
        "timestamp": datetime.now().isoformat(),
        "user_id": i % 10,
        "event_type": ["click", "view", "purchase"][i % 3],
        "value": round(random.random() * 100, 2)
    }
    
    # Write to file
    with open(f"/dbfs/FileStore/streaming_events/event_{i}.json", "w") as f:
        json.dump(event, f)
    
    time.sleep(0.1)  # Simulate streaming
```

## External Data Sources

### Public APIs
- [JSONPlaceholder](https://jsonplaceholder.typicode.com/) - Fake REST API
- [Open Weather API](https://openweathermap.org/api) - Weather data
- [COVID-19 API](https://covid19api.com/) - COVID data

### Public Datasets
- [Kaggle](https://www.kaggle.com/datasets)
- [UCI ML Repository](https://archive.ics.uci.edu/ml/)
- [AWS Open Data](https://registry.opendata.aws/)

## Data Generation Tools

### Python Libraries
```python
# Faker for realistic data
from faker import Faker
fake = Faker()

# Generate fake data
fake_data = [
    {
        "name": fake.name(),
        "email": fake.email(),
        "address": fake.address(),
        "phone": fake.phone_number()
    }
    for _ in range(100)
]

df = spark.createDataFrame(fake_data)
```

### SQL Generation
```sql
-- Generate sequence
SELECT id, 
       concat('User', id) as username,
       rand() * 100 as score
FROM range(1, 1001);
```

## Best Practices

1. **Use Delta Format**: Always save as Delta for ACID guarantees
2. **Partition Large Datasets**: Partition by date or category
3. **Document Schema**: Keep schema documentation
4. **Version Control**: Track data changes
5. **Clean Up**: Remove unused data to save costs

## Need More Data?

If you need additional datasets for practice:
1. Check Databricks built-in datasets
2. Use data generation scripts above
3. Download from Kaggle or UCI
4. Create synthetic data with Faker

---

**Note**: All sample data in this folder is for educational purposes only.
