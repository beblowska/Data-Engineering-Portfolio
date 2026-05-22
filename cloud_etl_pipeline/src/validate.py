import pandas as pd

REQUIRED_COLUMNS = [
    "transaction_id",
    "customer_id",
    "transaction_date",
    "currency",
    "amount",
    "merchant",
    "country",
    "status",
    "payment_method"
]

def validate_data(df: pd.DataFrame) -> bool:
    print("Validating data...")

    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]

    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")

    if df.isnull().any().any():
        raise ValueError("Dataset contains null values")

    print("Validation passed")
    return True