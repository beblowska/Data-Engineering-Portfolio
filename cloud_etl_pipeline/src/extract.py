import pandas as pd

import pandas as pd

def extract_data(file_path: str):
    print("Extracting data...")

    df = pd.read_csv(file_path, sep=";")

    print(f"Rows loaded: {len(df)}")

    return df