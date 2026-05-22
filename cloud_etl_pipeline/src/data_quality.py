def data_quality_report(df):
    report = {
        "rows": len(df),
        "null_values": int(df.isnull().sum().sum()),
        "duplicates": int(df.duplicated().sum()),
        "columns": len(df.columns)
    }
    return report