# Cloud ETL Pipeline (Python + AWS S3)

## Overview

This project is a production-style data engineering pipeline that processes raw transaction data, validates it, transforms it, and loads it into AWS S3 using a reproducible infrastructure defined in Terraform.
It simulates a real-world cloud data ingestion system used in modern data platforms.


## What this project simulates

This pipeline simulates a simplified version of a real data platform used in companies like fintechs or e-commerce systems.
In real life, it would work like this:

Transaction systems / apps
        ↓
Raw data landing (CSV / API / DB extract)
        ↓
ETL pipeline (this project)
        ↓
Clean + validated dataset
        ↓
Cloud storage (AWS S3)
        ↓
Analytics tools (Athena / BI / ML models)


---


## Architecture
```
              ┌──────────────────────┐
              │  Raw CSV Data        │
              └─────────┬────────────┘
                        ↓
              ┌──────────────────────┐
              │ Extract (Pandas)     │
              └─────────┬────────────┘
                        ↓
              ┌──────────────────────┐
              │ Validate Layer       │
              │ - schema check       │
              │ - null checks        │
              └─────────┬────────────┘
                        ↓
              ┌──────────────────────┐
              │ Transform Layer      │
              │ - type casting       │
              │ - filtering          │
              │ - feature engineering│
              └─────────┬────────────┘
                        ↓
              ┌──────────────────────┐
              │ Data Quality Checks  │
              └─────────┬────────────┘
                        ↓
              ┌──────────────────────┐
              │ Idempotent Processing│ 
              │      (run_id)        │
              └─────────┬────────────┘
                        ↓
              ┌──────────────────────┐
              │ Parquet Output       │
              └─────────┬────────────┘
                        ↓
              ┌──────────────────────┐
              │ AWS S3 (Terraform)   │
              └──────────────────────┘
```


## Tech Stack

- Python 3
- Pandas
- AWS S3
- Boto3
- Terraform (IaC)
- Parquet (PyArrow)
- PyYAML (config management)

## End-to-End Flow

1. Raw CSV file is loaded
2. Data is validated (schema + quality checks)
3. Data is transformed (cleaning + enrichment)
4. Output is saved as Parquet file
5. File is uploaded to AWS S3 bucket (created via Terraform)


## Key Features

1. ETL Pipeline
   - Extracts transaction data from CSV
   - Cleans and transforms dataset
   - Generates derived metrics (e.g. transaction fee)
2. Data Validation
   - Schema validation
   - Null checks
   - Data integrity checks
3. Data Quality Monitoring
   - Row count tracking
   - Null value detection
   - Duplicate detection
4. Idempotency
   - Each pipeline run generates a unique run_id
   - Prevents overwriting data in S3
   - Ensures traceability
5. Reliability
   - Retry mechanism for failed operations
   - Structured logging for observability
6. Cloud Infrastructure
   - AWS S3 bucket provisioned via Terraform
   - Versioned and reproducible infrastructure


## Project Structure
```
cloud_etl_pipeline/
│
├── main.py
├── requirements.txt
├── config.yaml
│
├── data/
│   └── sample_data.csv
│
├── infra/
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── .terraform.lock.hcl
│
├── src/
│   ├── extract.py
│   ├── transform.py
│   ├── validate.py
│   ├── load.py
│   ├── config_loader.py
│   ├── logger.py
│   ├── retry.py
│   └── idempotency.py
│
└── README.md
```

## Example Business Logic

- Only completed transactions are processed
- Transaction fee is calculated as:
    fee = amount * 0.02
- Merchant names are normalized to uppercase
- Duplicate records are removed


## How to Run

### 1. Install dependencies
pip install -r requirements.txt

### 2. Configure AWS credentials
aws configure

### 3. Run Terraform (infrastructure)
cd infra
terraform init
terraform apply

### 4. Run pipeline
python3 main.py


## Cloud Component

- Clean dataset (Parquet format)
- Uploaded to S3 bucket:
      s3://data-etl-pipeline-bucket-12345/processed/run_id=abc123/transactions_cleaned.parquet


## Future Improvements

- Airflow orchestration (scheduled pipelines)
- AWS Glue integration
- Athena analytics layer
- CI/CD with GitHub Actions
- Data partition optimization (year/month/day)
- Migration to Spark for scalability


## Author
Adrianna Bebłowska
