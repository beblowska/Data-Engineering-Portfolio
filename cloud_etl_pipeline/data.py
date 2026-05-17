import pandas as pd
import boto3
import logging

# -----------------------
# CONFIG
# -----------------------

BUCKET_NAME = "data-pipline"
INPUT_FILE = "transactions.csv"
OUTPUT_FILE = "transactions_cleaned.parquet"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -----------------------
# EXTRACT
# -----------------------

def extract(file_path: str) -> pd.DataFrame:
    logging.info("Extracting data...")

    df = pd.read_csv(file_path, sep=";")

    logging.info(f"Rows loaded: {len(df)}")
    return df


# -----------------------
# VALIDATION
# -----------------------

def validate(df: pd.DataFrame) -> None:
    logging.info("Validating data...")

    required_columns = [
        "transaction_id", "customer_id", "transaction_date",
        "currency", "amount", "merchant", "country",
        "status", "payment_method"
    ]

    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    if df["amount"].isnull().any():
        raise ValueError("Null values in amount")

    logging.info("Validation passed")


# -----------------------
# TRANSFORM
# -----------------------

def transform(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Transforming data...")

    df = df[df["status"] == "COMPLETED"].copy()

    df["amount"] = df["amount"].astype(float)
    df["fee"] = (df["amount"] * 0.02).round(3)

    df["merchant"] = df["merchant"].str.upper()

    df["transaction_date"] = pd.to_datetime(df["transaction_date"])

    df = df.drop_duplicates()

    logging.info(f"Rows after transform: {len(df)}")

    return df


# -----------------------
# LOAD
# -----------------------

def load(df: pd.DataFrame, output_path: str) -> None:
    logging.info("Saving parquet file...")

    df.to_parquet(output_path, index=False)

    logging.info("Uploading to S3...")

    s3 = boto3.client("s3")

    s3.upload_file(
        output_path,
        BUCKET_NAME,
        f"processed/{output_path}"
    )

    logging.info("Upload completed")


# -----------------------
# PIPELINE
# -----------------------

def run_pipeline():
    try:
        logging.info("PIPELINE STARTED")

        df = extract(INPUT_FILE)
        validate(df)
        df = transform(df)
        load(df, OUTPUT_FILE)

        logging.info("PIPELINE SUCCESS")

    except Exception as e:
        logging.error(f"PIPELINE FAILED: {e}")
        raise


# -----------------------
# MAIN
# -----------------------

if __name__ == "__main__":
    run_pipeline()