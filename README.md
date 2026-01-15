# LeadPulse — Delta CDF Data Engineering Project

This is a production-ready **LeadPulse CRM pipeline** using **Delta Lake CDF**, **PySpark**, **Docker**, and **Airflow**, demonstrating a **Bronze → Silver → Gold** Medallion architecture.

## Project Structure

- **data/raw** — Original dataset (UCI Online Retail)
- **data/bronze** — Raw ingestion Delta table with CDF
- **data/silver** — Cleaned + synthetic CRM events Delta table with CDF
- **data/gold** — Aggregated KPIs Delta table with CDF
- **scripts/** — PySpark scripts to generate each layer
- **dags/** — Airflow DAG for orchestration
- **docker/** — Dockerfile for containerized execution
- **notebooks/** — Optional exploration notebooks

## Pipeline Overview

1. **Bronze Layer**
   - Ingest raw Excel data
   - Store as Delta table with Change Data Feed

2. **Silver Layer**
   - Read Bronze CDF
   - Clean, de-duplicate, generate synthetic CRM events
   - Store as Delta table with CDF

3. **Gold Layer**
   - Read Silver CDF
   - Compute revenue, KPIs, status aggregation
   - Store as Delta table with CDF

4. **Airflow**
   - Orchestrates Bronze → Silver → Gold daily
   - Automatically processes only incremental changes using CDF

## Run Locally

1. Setup virtual environment:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
