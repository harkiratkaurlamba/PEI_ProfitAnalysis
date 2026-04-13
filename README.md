# PEI Profit Analysis – End-to-End Data Engineering Pipeline

## Overview

This project implements a **metadata-driven data pipeline** using **Databricks (Community Edition)** and **PySpark**, following a **Medallion Architecture (Bronze → Silver → Gold)**.

The pipeline ingests raw data from multiple formats (CSV, JSON, XLSX), standardizes and transforms it into curated datasets, and finally produces analytical tables for business insights.

---

## Architecture



### Layers

- **Bronze (Raw Ingestion)**
  - Ingests raw data as-is
  - Supports multiple formats (CSV, JSON, XLSX)
  - Adds audit columns (ingestion timestamp, source file)
  - Handles PII (hash / drop)

- **Silver (Transformation Layer)**
  - Applies schema enforcement and column standardization
  - Performs data cleaning and transformations
  - Handles deduplication using window functions
  - Applies data quality checks
  - Outputs structured, analysis-ready datasets

- **Gold (Serving Layer)**
  - Builds enriched fact table by joining datasets
  - Computes business metrics (profit)
  - Generates aggregated views for analytics

---

## Tech Stack

- **Platform:** Databricks Community Edition
- **Language:** Python (PySpark)
- **Storage Format:** Parquet / Delta Lake
- **Configuration:** YAML (metadata-driven pipelines)
- **Testing:** PyTest

---

## Assumptions and others
- Logic has been written as a one-time requirement and for the SCD-1 load.
- The profit column from the Orders dataset is used for profitability calculations.
- Unique order records are identified using irder id an product id.
- Databricks' community edition has been used for the solution, and there were limitations to using scripts, orchestration, and other Databricks features.
  

