import pandas as pd

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    print("Transforming data...")

    # filter only completed transactions
    df = df[df["status"] == "COMPLETED"].copy()

    # ensure numeric type
    df["amount"] = df["amount"].astype(float)

    # calculate fee (2%)
    df["fee"] = df["amount"] * 0.02

    # normalize merchant names
    df["merchant"] = df["merchant"].str.upper()

    print(f"Rows after transform: {len(df)}")
    return df