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
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]

    if missing:
        raise ValueError(f"Missing columns: {missing}")

    if df.empty:
        raise ValueError("Dataset is empty")

    if df.isnull().any().any():
        raise ValueError("Null values detected")

    return True