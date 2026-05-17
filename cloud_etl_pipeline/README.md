# Cloud ETL Pipeline (Python + AWS S3)

## Overview

This project is a cloud-style ETL (Extract, Transform, Load) pipeline built in Python.

It simulates a real-world data engineering workflow where raw transactional data is processed, validated, transformed, and stored in a cloud data lake (AWS S3) in an optimized format (Parquet).

The goal of this project is to demonstrate practical data engineering skills including:
- ETL pipeline design
- Data validation
- Data transformation
- Cloud storage integration (AWS S3)
- Efficient file formats (Parquet)
- Logging and observability

---

## Architecture

Raw CSV → Extract → Validate → Transform → Parquet → AWS S3 (processed zone)

## Tech Stack

- Python 3.11+
- Pandas
- Boto3 (AWS SDK)
- Apache Arrow (PyArrow)
- AWS S3 (data storage)

## Features

### ETL Pipeline
- Extracts transaction data from CSV
- Applies validation rules
- Transforms and cleans dataset
- Loads processed data into AWS S3

### Data Quality
- Schema validation (required columns check)
- Null value detection
- Data type normalization

### Transformations
- Filter only `COMPLETED` transactions
- Convert `amount` to numeric type
- Calculate transaction fee (2%)
- Standardize merchant names (uppercase)
- Remove duplicates
- Convert date strings to datetime format

### Output Optimization
- Stores data in Parquet format (columnar, compressed, analytics-friendly)
- Uploads processed dataset to AWS S3

## AWS Integration

The pipeline uses:

- AWS S3 bucket for storage
- boto3 for programmatic access
- IAM credentials configured via AWS CLI (`aws configure`)

---

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
pip install pandas boto3 pyarrow

### 2. Configure AWS credentials
aws configure

### 3. Run pipeline
python3 data.py


## Output
- Clean dataset in Parquet format
- Uploaded to AWS S3 bucket:
s3://<data-pipeline>/processed/


## Learning Outcomes
- This project demonstrates:
- Building modular ETL pipelines in Python
- Working with real-world data transformation logic
- Integrating with cloud services (AWS S3)
- Using Parquet for efficient data storage
- Structuring production-like data engineering code


## Future Improvements
- Add Apache Airflow orchestration
- Add retry mechanism and idempotency
- Add data quality framework (e.g. Great Expectations)
- Dockerize pipeline
- Add Terraform infrastructure provisioning
- Add CI/CD via GitHub Actions


## Author
Adrianna Bebłowska