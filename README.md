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

## Project Structure

