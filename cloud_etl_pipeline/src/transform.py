import pandas as pd

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["amount"] = df["amount"].astype(float)
    df["fee"] = (df["amount"] * 0.02).round(3)
    df["merchant"] = df["merchant"].str.upper()

    df = df[df["status"] == "COMPLETED"]

    return df