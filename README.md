# ecommerce-ELT-pipeline

ELT pipeline for ecommerce data using GCS, BigQuery, Cloud Functions and Apache Airflow.

## Pipeline Overview

1. **Raw Data Ingestion** — CSV files (orders, customers, products) are uploaded to Google Cloud Storage (GCS)
2. **Auto Load to BigQuery** — A Cloud Function triggers automatically when files land in GCS and loads them into BigQuery as raw tables under the `raw_ecommerce` dataset
3. **Transformation** — Data is cleaned and transformed inside BigQuery using SQL
4. **Orchestration** — Apache Airflow schedules and manages the pipeline end to end
5. **Dashboard** — Final data is visualized in a dashboard showing revenue, orders, and product performance


## Tech Stack

- Google Cloud Storage (GCS)
- Google BigQuery
- Google Cloud Functions
- Apache Airflow
- Looker Studio / Python (Dashboard)
