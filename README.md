# healthcare-lakehouse-azure123
Azure Databricks Lakehouse project using PySpark, SQL, Terraform and CI/CD for healthcare analytics

# Healthcare Patient Flow Lakehouse (Azure Databricks)

##  Overview

This project demonstrates an end-to-end **Lakehouse data engineering pipeline** built using Azure Databricks, PySpark, SQL, and modern data architecture principles.

The solution processes healthcare-style patient visit data to generate **analytical insights and KPI-ready datasets** for decision-making.

---

##  Architecture

The project follows a **Medallion Architecture (Bronze → Silver → Gold)**:

* **Bronze Layer**: Raw data ingestion from Unity Catalog Volumes
* **Silver Layer**: Data cleaning, transformation, and enrichment
* **Gold Layer**: Aggregated business metrics and KPIs
* **Dimensional Model**: Star schema for analytical workloads

---

##  Tech Stack

* **Databricks (Serverless Compute)**
* **PySpark**
* **SQL**
* **Delta Lake**
* **Unity Catalog (Volumes & Tables)**
* **GitHub (Version Control)**

---

##  Project Structure

```
healthcare-lakehouse-azure/
│
├── notebooks/
│   ├── 01_bronze_ingest.py
│   ├── 02_silver_transform.py
│   ├── 03_gold_metrics.py
│   └── 04_dimensional_model.sql
│
├── sql/
│   └── dimensional_model.sql
│
├── data/
│   └── patient_visits.csv
│
├── terraform/
│
├── tests/
│
└── README.md
```

---

##  Bronze Layer (Data Ingestion)

* Ingested raw CSV data from **Unity Catalog Volume**
* Stored data as **Delta table**
* Ensured scalable and structured ingestion

**Output Table:**

```
main.default.bronze_patient_raw
```

---

##  Silver Layer (Data Transformation)

* Converted timestamps to proper format
* Derived **wait time (minutes)**
* Removed nulls and duplicates
* Applied basic data quality checks

**Output Table:**

```
main.default.silver_patient_clean
```

---

##  Gold Layer (Analytics & KPIs)

* Aggregated patient metrics by department
* Calculated:

  * Average wait time
  * Total patient visits
* Built daily analytics tables

**Output Tables:**

```
main.default.gold_patient_metrics
main.default.gold_daily_metrics
```

---

##  Dimensional Modelling (Star Schema)

Implemented a **star schema** for analytical querying:

### Dimension Tables:

* `dim_department`
* `dim_date`
* `dim_triage`

### Fact Table:

* `fact_patient_visits`

This enables efficient reporting and BI integration.

---

##  Data Governance

* Used **Unity Catalog** for structured data access
* Avoided legacy DBFS usage
* Followed modern Databricks data governance practices

---

##  Key Insights Generated

* Average wait time by department
* Daily patient trends
* High-demand departments
* Patient flow distribution

---

##  Key Skills Demonstrated

* End-to-end **data pipeline development**
* **PySpark transformations** and data engineering logic
* **Dimensional modelling (Star Schema)**
* Working with **Delta Lake & Lakehouse architecture**
* Data quality and validation techniques
* Use of **modern Databricks features (Unity Catalog)**

---

##  Future Enhancements

* Infrastructure provisioning using **Terraform**
* CI/CD pipelines with **GitHub Actions**
* Integration with **Power BI dashboards**
* LLM-powered SQL query assistant

---

##  Author

**Sheikh Tania**
PhD in IT | Aspiring Data Engineer

---

##  Summary

This project showcases the design and implementation of a **modern, scalable Lakehouse data platform**, aligning with industry best practices and enterprise data engineering standards.

