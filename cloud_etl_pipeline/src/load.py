import boto3
from src.idempotency import generate_run_id

def load_data(df, output_file, bucket, prefix):
    s3 = boto3.client("s3")

    run_id = generate_run_id(df)

    s3_key = f"{prefix}run_id={run_id}/{output_file}"

    df.to_parquet(output_file, index=False)

    s3.upload_file(output_file, bucket, s3_key)

    print(f"Uploaded to s3://{bucket}/{s3_key}")