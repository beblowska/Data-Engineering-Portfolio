import boto3

def load_data(df, output_path: str, bucket_name: str):
    print("Saving parquet file...")

    # save locally as parquet
    df.to_parquet(output_path, index=False)

    print("Uploading to S3...")

    s3 = boto3.client("s3")

    s3.upload_file(
        output_path,
        bucket_name,
        f"processed/{output_path}"
    )

    print("Upload completed")