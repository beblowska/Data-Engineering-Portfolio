from src.extract import extract_data
from src.validate import validate_data
from src.transform import transform_data
from src.load import load_data
from src.config import INPUT_FILE, OUTPUT_FILE, BUCKET_NAME

def run_pipeline():
    print("PIPELINE STARTED")

    df = extract_data(INPUT_FILE)
    validate_data(df)
    df = transform_data(df)
    load_data(df, OUTPUT_FILE, BUCKET_NAME)

    print("PIPELINE SUCCESS")

if __name__ == "__main__":
    run_pipeline()