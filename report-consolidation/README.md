# Report Consolidation

## Project Overview
This project automates the consolidation of daily reports coming from three independent SQL databases with 
different schemas but overlapping business data.  
The workflow extracts data from multiple sources, normalizes and compares key fields, 
validates time-sensitive attributes, and produces a formatted Excel report ready for business consumption.  

The solution is designed to be:
- Repeatable
- Environment-aware
- Cross-team safe
- Easily scalable

## Business Problem
- Data originates from **3 separate SQL databases**  
- Column names differ across systems  
- Reports must be **consolidated daily**  
- Data requires **validation** (e.g., expiry dates within 14 days)  
- Output must be **appended and formatted**  
- Manual processing costs ~1–2h per day and is **error-prone**

## Solution

### Core Technologies
- Python (Pandas, openpyxl)  
- SQLite (mocked data sources)  
- Apache Airflow  
- CyberArk-compatible Secrets Backend  
- Excel with conditional formatting  

### Workflow
1. Extract data from three SQL sources  
2. Normalize schemas  
3. Compare critical business fields (currency mismatch detection)  
4. Validate expiry dates  
5. Generate formatted Excel report  
6. Orchestrate daily execution with Airflow  

# Airflow Execution
- The DAG report_comparison runs daily.
- Secrets are retrieved via Airflow Connections (excel_report_db) using a Secrets Backend (e.g., CyberArk).

# Environment Awareness
- The pipeline behavior is controlled by the ENV variable:
- ENV	Behavior
  local	    -->    Creates mock databases  || 
  dev/prod	-->    Uses existing data sources

# Output
- Daily Excel report: report_YYYYMMDD.xlsx
- Conditional formatting for:
- Records requiring attention
- Currency mismatches
- Data validation dropdown for manual review

# Outcome
- Automated cross-team reporting
- Reduced manual work by ~1–2 hours per day
- Near-zero human error
- Scalable and reusable ETL pattern

# Author
Adrianna Beblowska


