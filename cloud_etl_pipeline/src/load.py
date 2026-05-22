import boto3
from datetime import datetime

def load_data(df, output_file, bucket, prefix):
    s3 = boto3.client("s3")

    timestamp = datetime.now().strftime("%Y-%m-%d")
    s3_key = f"{prefix}{timestamp}/{output_file}"

    df.to_parquet(output_file, index=False)

    s3.upload_file(output_file, bucket, s3_key)

    print(f"Uploaded to s3://{bucket}/{s3_key}")