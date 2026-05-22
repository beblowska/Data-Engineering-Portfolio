# Cloud ETL Pipeline (Python + AWS S3)

## Overview

This project is a modular ETL (Extract, Transform, Load) pipeline built in Python.
It processes raw transaction data, validates it, transforms it into analytics-ready format, and loads it into AWS S3 in Parquet format.
The goal of this project is to simulate a production-style data engineering workflow with cloud integration.

The goal of this project is to demonstrate practical data engineering skills including:
- ETL pipeline design
- Data validation
- Data transformation
- Cloud storage integration (AWS S3)
- Efficient file formats (Parquet)
- Logging and observability

---

## Architecture

CSV (raw data)
      ↓
Extract (pandas)
      ↓
Validate (schema + quality checks)
      ↓
Transform (cleaning + enrichment)
      ↓
Parquet file
      ↓
AWS S3 upload


## Tech Stack

- Python 3.10+
- Pandas
- Boto3 (AWS SDK)
- AWS S3
- Parquet (PyArrow)


## Features

- Modular ETL architecture (extract / transform / validate / load)
- CSV ingestion with custom parsing
- Data validation layer (schema + null checks)
- Data transformations:
   - type casting
   - filtering transactions
   - feature engineering (fee calculation)
   - text normalization
- Output format: Parquet
- AWS S3 integration using boto3
- Production-style pipeline execution flow

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
   - Load CSV file into Pandas DataFrame

2. **Validate**
   - Check required columns
   - Validate data integrity

3. **Transform**
   - Clean and standardize data
   - Apply business rules

4. **Load**
   - Save as Parquet file
   - Upload to AWS S3 bucket under `processed/`


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
Make sure you have AWS CLI configured:
   aws configure

Required permissions:
- s3:PutObject
- s3:ListBucket

### 3. Run pipeline
python3 main.py


## Example Output Flow

PIPELINE STARTED
Extracting data...
Validating data...
Transforming data...
Saving parquet file...
Uploading to S3...
PIPELINE SUCCESS


## Data Validation Rules

- Required columns must exist
- No null values allowed
- Transaction status filtering (e.g. COMPLETED only)


## Transformations

- Convert amount → float
- Compute transaction fee (2%)
- Normalize merchant names (uppercase)
- Filter only valid transactions


## Cloud Component

Data is uploaded to AWS S3:
s3://<bucket-name>/processed/transactions_cleaned.parquet


## Future Improvements

- Add Apache Airflow orchestration
- Add Terraform for infrastructure provisioning (S3, IAM)
- Add retry mechanism + idempotency
- Add logging & monitoring (CloudWatch)
- Add data partitioning strategy
- Move to Spark for scalability


## Author
Adrianna Bebłowska