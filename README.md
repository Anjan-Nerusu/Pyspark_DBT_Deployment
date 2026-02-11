#### 🚀 Databricks Lakehouse Pipeline using PySpark, Delta Lake & dbt

#### 📌 Project Overview
This project implements an end-to-end Databricks Lakehouse data engineering pipeline using PySpark Structured Streaming, Delta Lake, and dbt.
The pipeline ingests multiple CSV files from Databricks Volumes, loads them into a Bronze layer, applies reusable transformation logic, and incrementally upserts data into a Silver layer using Delta MERGE.
The curated datasets are further modeled using dbt models and snapshots (Slowly Changing Dimensions – SCD).

The solution is designed to be dynamic, reusable, and scalable across multiple business entities such as customers, trips, vehicles, payments, drivers, and locations.

#### 🏗️ Architecture

CSV files (Databricks Volumes)
        ↓
PySpark Structured Streaming (trigger once)
        ↓
Bronze Delta Tables
        ↓
Reusable PySpark Transformations
        ↓
Delta MERGE (CDC-based upsert)
        ↓
Silver Delta Tables
        ↓
dbt Models & Snapshots (SCD)

#### 🧩 Technologies Used

- Databricks  
- PySpark  
- Structured Streaming  
- Delta Lake  
- dbt  
- GitHub (Databricks Repos)

#### 📂 Project Structure

Pyspark_DBT_Deployment/
├── notebooks/
│   └── ingestion and transformation notebooks
├── utils/
│   └── customutils.py
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   ├── snapshots/
│   └── macros/
└── README.md

#### 🔁 End-to-End Workflow

#### 1️⃣ Source Ingestion – Bronze Layer

Source CSV files are ingested from Databricks Volumes using Structured Streaming.

```python
spark.readStream.format("csv") \
  .option("header","true") \
  .schema(schema) \
  .load("/Volumes/.../source_data/customers/")
Multiple entities are processed dynamically:

entities = ["customers","trips","vehicles","payments","drivers","locations"]
2️⃣ Streaming Write to Bronze Tables
.writeStream \
.format("delta") \
.trigger(once=True) \
.option("checkpointLocation", "...") \
.toTable("pyspark_dbt.bronze.customers")
This approach enables incremental ingestion using micro-batch streaming.

3️⃣ Reusable Transformation Framework
A reusable Python utility class is created to standardize transformations across all entities.

3.1 Deduplication using CDC logic
row_number().over(
  Window.partitionBy("dedupKey").orderBy(desc(cdc))
)
This keeps only the latest record per business key based on the CDC timestamp.

3.2 Audit column generation
df = df.withColumn("process_timestamp", current_timestamp())
This column tracks when a record is processed by the pipeline.

4️⃣ Incremental Upsert into Silver Layer
merge_condition = " AND ".join(
    [f"src.{c} = trg.{c}" for c in key_cols]
)

dlt.alias("trg") \
  .merge(df.alias("src"), merge_condition) \
  .whenMatchedUpdateAll(
      condition=f"src.{cdc} >= trg.{cdc}"
  ) \
  .whenNotMatchedInsertAll() \
  .execute()
This ensures:

existing records are updated

new records are inserted

older data does not overwrite newer data

5️⃣ Dynamic Multi-Entity Processing
for entity in entities:
    obj.upsert(...)
The same pipeline logic is reused for all datasets.

6️⃣ dbt Modeling Layer
Example dbt model:

select *
from {{ source("source_bronze", "trips") }}
Jinja templating is used to dynamically generate SQL.

7️⃣ Slowly Changing Dimensions using dbt Snapshots
dbt snapshots are used to track historical changes for dimension tables such as DimVehicle.

8️⃣ Version Control using GitHub
All notebooks, PySpark utilities, and dbt projects are maintained in a Databricks Repo and pushed to GitHub.
Only code is versioned (data and Delta tables are excluded).

📘 Concepts Implemented and Learned
✔ PySpark Structured Streaming
.trigger(once=True)
Used streaming as an incremental ingestion mechanism.

✔ Delta Lake CDC-based MERGE
.whenMatchedUpdateAll(condition="src.ts >= trg.ts")
.whenNotMatchedInsertAll()
✔ Window Functions for Deduplication
row_number().over(Window.partitionBy(...).orderBy(...))
✔ Reusable Transformation Utilities
Transformation logic is encapsulated inside a reusable Python class.

✔ Dynamic and Scalable Pipelines
for entity in entities:
✔ Schema and Merge Debugging
Handled:

merge key mismatches

data type mismatches

duplicate inserts during upserts

incorrect CDC logic

✔ dbt Jinja Templating
{% for col in cols %}
  {{ col }}
{% endfor %}
✔ dbt Sources and Snapshots
{{ source("source_bronze","trips") }}
and SCD snapshots for historical tracking.

✔ Databricks Platform Features
Unity Catalog tables

Databricks Volumes

Databricks Repos

Streaming checkpoints

✔ GitHub-based Version Control
structured repository layout

commit and push using Databricks Repos

.gitignore best practices

🏁 Summary
This project demonstrates a production-oriented data engineering solution built on the Databricks Lakehouse platform.
It combines incremental ingestion, CDC-driven upserts, reusable PySpark transformations, scalable multi-entity processing,
and dbt-based analytical modeling with SCD tracking, all managed through GitHub for version control.
