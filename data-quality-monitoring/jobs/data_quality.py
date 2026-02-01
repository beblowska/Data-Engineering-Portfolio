import os
import pandas as pd
import yaml
from datetime import datetime

# --- PATHS ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "dq_rules.yaml")
INPUT_FOLDER = os.path.join(BASE_DIR, "data", "inputs")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "data", "sample_outputs")

# --- LOAD CONFIG ---
with open(CONFIG_PATH, "r") as f:
    rules = yaml.safe_load(f)

EXPECTED_COLUMNS = rules["expected_columns"]
COLUMN_MAPPING = rules["column_mapping"]
BLOCKED_IDS = set(rules["blocked_ids"])
BLOCKED_COUNTRIES = set(rules["blocked_countries"])
DATE_FORMAT = rules["date_format"]

# --- HELPERS ---
def init_rejection_df(df):
    df["_rejection_reason"] = ""
    return df

def add_reason(df, mask, reason):
    df.loc[mask, "_rejection_reason"] += reason + ";"
    return df

# --- PIPELINE STEPS ---
def standardize_columns(df):
    df = df.rename(columns={k: v for k, v in COLUMN_MAPPING.items() if k in df.columns})
    missing = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return df[EXPECTED_COLUMNS]

def apply_business_rules(df):
    df = init_rejection_df(df)

    df = add_reason(df, df["record_id"].isin(BLOCKED_IDS), "blocked_record_id")
    df = add_reason(df, df["country"].isin(BLOCKED_COUNTRIES), "blocked_country")

    df["currency"] = df["currency"].astype(str).str.upper()
    df = add_reason(df, df["currency"].str.len() != 3, "invalid_currency")

    for col in ["trade_date", "expiry_date"]:
        parsed = pd.to_datetime(df[col], format=DATE_FORMAT, errors="coerce")
        df = add_reason(df, parsed.isna(), "invalid_date_format")
        df[col] = parsed

    return df

def apply_quality_checks(df):
    df = add_reason(df, df.isna().any(axis=1), "null_values")
    df = add_reason(df, df.duplicated(), "duplicate_record")
    return df

def split_clean_and_rejected(df):
    rejected = df[df["_rejection_reason"] != ""].copy()
    clean = df[df["_rejection_reason"] == ""].copy()

    rejected["_rejection_reason"] = rejected["_rejection_reason"].str.rstrip(";")

    rejected = rejected.drop_duplicates()

    clean = clean.drop(columns=["_rejection_reason", "input_row_number"], errors="ignore")

    return clean, rejected


def process_file(path):
    df = pd.read_csv(path, encoding="utf-8-sig")
    df = standardize_columns(df)
    df["input_row_number"] = range(1, len(df) + 1)
    df = apply_business_rules(df)
    df = apply_quality_checks(df)
    return split_clean_and_rejected(df)

# --- RUN ---
def run_pipeline():
    clean_all, rejected_all = [], []

    for file in os.listdir(INPUT_FOLDER):
        if not file.endswith(".csv"):
            continue

        try:
            clean, rejected = process_file(os.path.join(INPUT_FOLDER, file))
            if not clean.empty:
                clean_all.append(clean)
            if not rejected.empty:
                rejected["source_file"] = file
                rejected_all.append(rejected)
        except Exception as e:
            print(f"{file} failed: {e}")

    today = datetime.today().strftime("%Y%m%d")

    if clean_all:
        pd.concat(clean_all).to_csv(
            os.path.join(OUTPUT_FOLDER, f"clean_report_{today}.csv"),
            index=False
        )

    if rejected_all:
        final_rejected = pd.concat(rejected_all, ignore_index=True)
        cols = ["input_row_number"] + [c for c in final_rejected.columns if c != "input_row_number"]
        final_rejected = final_rejected[cols]
        final_rejected.to_csv(os.path.join(OUTPUT_FOLDER, f"rejected_records_{today}.csv"), index=False)

    print("Pipeline finished")

# --- LOCAL ---
if __name__ == "__main__":
    run_pipeline()
