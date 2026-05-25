# Data Engineering Portfolio

## Cloud Data Engineering | Data Quality | Automation | Governance-Focused Design

This repository showcases production-inspired data engineering projects focused on building reliable, automated, and scalable data systems.

The emphasis is not only on moving data, but on ensuring that data is:
- validated
- standardized
- auditable
- reproducible
- safe for downstream consumption

---

## Portfolio Purpose

Modern data platforms fail not due to lack of pipelines, but due to:

- inconsistent data quality across sources
- manual and error-prone reporting processes
- lack of standardization between teams/vendors
- weak environment separation
- absence of traceability and auditability

This portfolio demonstrates how structured data engineering practices can solve these problems through automation, validation, and cloud-based architecture patterns.

---

## Engineering Impact (Simulation-Based)

These projects simulate real-world enterprise data challenges and demonstrate measurable engineering improvements:

- 60–80% reduction in manual validation effort
- ~90% reduction in human data errors in reporting workflows
- Faster onboarding of external vendors via config-driven validation
- Elimination of silent data quality failures through explicit rejection tracking
- Reduced operational overhead through automation and orchestration

---

## Core Engineering Principles

Across all projects, the following principles are consistently applied:

- Config-driven design (YAML-based business rules)
- Environment-aware execution (local / dev / prod patterns)
- Idempotent and deterministic processing
- Explicit validation before transformation
- Separation of ingestion, processing, and output layers
- Audit-ready outputs with traceable data lineage
- Automation-first workflow design

---

# Projects

---

## 1. Cloud ETL Pipeline (AWS S3 + Terraform)

### Overview

A cloud-based ETL pipeline simulating a production data lake ingestion system using AWS S3 and Infrastructure as Code (Terraform).

The pipeline processes raw transactional CSV data, applies validation and transformation rules, and stores clean outputs in a structured cloud data lake format.

---

### Business Problem

Organizations need reliable pipelines to process transactional data while ensuring:

- data quality enforcement
- reproducibility of processing
- safe storage in cloud environments
- idempotent execution without duplication

---

### Solution

The pipeline:

- ingests raw CSV data
- validates schema and data integrity
- transforms and enriches datasets
- applies idempotent processing via run_id
- outputs data in Parquet format
- stores results in AWS S3 (data lake structure)
- provisions infrastructure using Terraform

---

### Architecture

Raw Data → AWS S3 (Landing Zone) → Python ETL → Validation Layer → Transformation Layer → Idempotency Layer → Parquet Output → AWS S3 (Processed Zone)

---

### Tech Stack

Python, Pandas, AWS S3, Terraform, Boto3, PyArrow, PyYAML

---

### Key Features

- Data validation (schema, null checks, integrity rules)
- Idempotent processing (run-based isolation)
- Parquet optimized outputs
- Infrastructure as Code (Terraform-managed AWS S3)
- Reproducible pipeline execution

---

### Engineering Focus

- Cloud storage-first design
- Stateless ETL processing model
- Audit-friendly execution flow
- Production-inspired data architecture

---

## 2. Data Quality Monitoring & Vendor Validation

### Overview

A config-driven data quality framework designed to validate and standardize vendor-delivered datasets before downstream processing.

---

### Business Problem

External vendors deliver inconsistent data with:

- different schemas
- inconsistent naming conventions
- varying formats (dates, currencies)
- missing or duplicate records

Manual validation is slow, error-prone, and not scalable.

---

### Solution

The pipeline:

- validates incoming vendor datasets
- enforces schema standardization via YAML rules
- applies data quality checks (nulls, duplicates, formats)
- separates valid and rejected records
- generates traceable rejection reports

---

### Tech Stack

Python, Pandas, YAML, Apache Airflow

---

### Key Features

- Schema standardization via configuration
- Deterministic validation rules
- Rejection tracking with explanations
- Fully automated execution via Airflow
- Clean downstream-ready dataset generation

---

### Engineering Focus

- Data quality enforcement layer
- Config-driven validation architecture
- Traceable rejection pipeline design
- Deterministic and repeatable processing

---

## 3. Report Consolidation Pipeline (Multi-Source SQL + Excel Automation)

### Overview

A cross-source reporting pipeline that consolidates data from multiple SQL databases into a unified, business-ready Excel report.

---

### Business Problem

Organizations often rely on manual report consolidation from multiple systems, leading to:

- inconsistent schemas across databases
- repetitive manual Excel processing
- high risk of human error
- lack of scalability

---

### Solution

The pipeline:

- extracts data from multiple SQL sources (SQLite simulation)
- normalizes schemas across systems
- validates key business rules (e.g. expiry logic)
- detects inconsistencies (e.g. currency mismatches)
- generates formatted Excel reports with validation rules
- automates execution using Airflow

---

### Tech Stack

Python, Pandas, SQL (SQLite), Apache Airflow, openpyxl

---

### Key Features

- Multi-source data extraction
- Schema normalization across systems
- Business rule validation (e.g. expiry thresholds)
- Conditional formatting in Excel outputs
- Environment-aware execution (local / dev / prod simulation)

---

### Engineering Focus

- Cross-system data integration
- Reporting automation
- Separation of business logic and orchestration
- Excel as a delivery layer (not processing layer)

---

## Common Architecture Pattern

All projects follow a consistent layered structure:

project/
│
├── configs/        # Environment-specific rules (YAML)
├── data/
│   ├── inputs/     # Raw untrusted data
│   └── outputs/    # Processed validated data
├── jobs/           # Business logic layer
├── dags/           # Orchestration (Airflow)
│
Raw data is always treated as untrusted.
All transformations are explicit, deterministic, and validated before downstream use.

---

## Environment Design

Each pipeline supports environment-aware execution:

- Local development
- Testing / staging simulation
- Production-like execution mode

Configuration is externalized to ensure reproducibility and avoid environment leakage.

---

## Output Principles

All outputs across projects follow:

- Clean schema consistency
- Deduplication where applicable
- Downstream-ready structure
- Audit-friendly traceability
- Explicit rejection handling instead of silent failures

---

## Engineering Controls

### Data Controls
- Schema enforcement
- Null and duplicate validation
- Format standardization

### Operational Controls
- Idempotent execution patterns
- Config-driven pipeline behavior
- Clear separation of concerns

### Cloud & Automation Controls
- AWS S3-based data lake design
- Terraform-based infrastructure provisioning
- Airflow orchestration for scheduling

---

## What This Portfolio Demonstrates

This portfolio reflects practical Data Engineering capabilities with emphasis on:

- cloud-inspired data architecture design
- data reliability and validation systems
- automation-first engineering mindset
- cross-system data integration
- governance-aligned data processing
- production-style workflow structuring

---

## Author

Adrianna Bebłowska
