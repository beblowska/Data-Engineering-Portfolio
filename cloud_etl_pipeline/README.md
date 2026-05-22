# Cloud ETL Pipeline (Python + AWS S3)

## Overview

This project is a production-style ETL pipeline that processes raw transaction data, validates it, transforms it into analytics-ready format, and loads it into AWS S3.
The infrastructure (S3 bucket) is managed using Terraform (Infrastructure as Code), and the data processing is implemented in Python (Pandas-based pipeline).


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


## End-to-End Flow

1. Raw CSV file is loaded
2. Data is validated (schema + quality checks)
3. Data is transformed (cleaning + enrichment)
4. Output is saved as Parquet file
5. File is uploaded to AWS S3 bucket (created via Terraform)

## Project Structure
```
cloud_etl_pipeline/
│
├── main.py
├── requirements.txt
├── data/
│   └── sample_data.csv
│
├── src/
│   ├── extract.py
│   ├── transform.py
│   ├── validate.py
│   ├── load.py
│   └── config.py
│
└── README.md
```

## Data Flow

1. **Extract**
   - Reads raw CSV data
   - Parses transaction dataset

3. **Validate**
   - Check required columns
   - Validate data integrity

4. **Transform**
   - Clean and standardize data
   - Apply business rules

5. **Load**
   - Save as Parquet file
   - Upload to AWS S3 bucket 


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
      s3://data-etl-pipeline-bucket/processed/


## Future Improvements

- Add Apache Airflow orchestration
- Add retry + idempotency logic
- Add logging (CloudWatch / Python logging)
- Add data partitioning in S3
- Add AWS Glue + Athena layer
- Add CI/CD (GitHub Actions)
- Replace Pandas with Spark for scale


## Author
Adrianna Bebłowska
