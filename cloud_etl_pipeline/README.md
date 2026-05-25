# ☁️ Cloud ETL Pipeline (AWS S3 + Python + Terraform)

## Executive Summary

This project is a portfolio-scale implementation inspired by production cloud data architectures.

It simulates a cloud-based data ingestion system designed to process transactional data reliably using a layered ETL approach, with a strong focus on:
- data reliability
- data quality enforcement
- idempotent processing
- reproducible infrastructure (Infrastructure as Code)

---

## Problem Statement

Modern data systems must handle:
- inconsistent and potentially unreliable raw data sources
- schema variability and missing values
- duplicate or corrupted records
- need for repeatable, auditable processing pipelines

This project addresses these challenges by implementing a cloud-inspired ETL pipeline that validates, transforms, and securely stores data for downstream analytics.

---

## System Architecture

### High-Level Data Flow

Raw Data (CSV / external source)
        ↓
AWS S3 (Landing Zone)
        ↓
Python ETL Processing Layer
        ↓
Validation Layer (schema + data quality checks)
        ↓
Transformation Layer (cleaning + feature engineering)
        ↓
Idempotency Layer (run_id tracking)
        ↓
Parquet Output Layer
        ↓
AWS S3 (Processed Data Lake)

---

## Cloud Architecture Principles

This system is designed using cloud-native data engineering principles:

- Storage-first architecture using AWS S3 as the data lake foundation
- Stateless processing using Python-based ETL jobs
- Immutable outputs stored in Parquet format
- Infrastructure as Code using Terraform
- Reproducible and environment-ready design patterns

---

## Tech Stack

- Python (ETL processing)
- Pandas (data transformation)
- PyArrow (Parquet format handling)
- AWS S3 (data lake storage)
- Terraform (Infrastructure as Code)
- Boto3 (AWS integration)
- PyYAML (configuration management)

---

## Data Flow Description

1. Raw transactional CSV data is ingested into the system
2. Data is validated (schema checks, null checks, integrity rules)
3. Data is transformed (type casting, filtering, feature engineering)
4. A unique `run_id` is generated for idempotent execution
5. Clean dataset is exported in Parquet format
6. Output is stored in AWS S3 under structured paths

---

## Data Quality & Validation

The pipeline enforces data quality through:

- Schema validation before transformation
- Null value detection in critical fields
- Duplicate record removal
- Row count consistency checks
- Business rule validation (e.g. only completed transactions)

---

## Idempotency & Reliability

To ensure safe re-runs and production-safe behavior:

- Each pipeline execution is assigned a unique `run_id`
- Outputs are isolated per run
- No overwriting of previously processed data
- Safe reprocessing without side effects

---

## Infrastructure (Terraform)

Cloud infrastructure is provisioned using Terraform:

- AWS S3 bucket creation
- Version-controlled infrastructure definitions
- Reproducible deployments
- Extendable design for multi-environment setups (DEV / PROD)

---

## Business Logic

- Only completed transactions are processed
- Transaction fee is calculated as:
  fee = amount * 0.02
- Merchant names are normalized to uppercase
- Duplicate records are removed prior to processing

---

## Key Engineering Decisions

### Why AWS S3?
- Highly scalable object storage
- Industry-standard data lake foundation
- Durable and cost-efficient storage layer

### Why Parquet?
- Columnar format optimized for analytics workloads
- Reduced storage footprint
- Faster query performance in downstream systems

### Why idempotent design?
- Enables safe retries without duplication
- Critical for production-grade pipelines
- Ensures consistent and predictable outputs

---

## Future Improvements

- Add Airflow orchestration for scheduled pipeline execution
- Integrate AWS Glue for managed ETL workflows
- Enable Athena for SQL-based analytics layer
- Add CI/CD pipeline using GitHub Actions
- Implement partitioning strategy (date-based storage)
- Scale processing layer using Apache Spark

---

## Key Takeaway

This project demonstrates a cloud data pipeline designed with production-inspired engineering principles:
reliability, reproducibility, and data quality at its core.

It serves as a foundational simulation of how modern cloud data platforms are engineered in real-world environments.


## Author
Adrianna Bebłowska
