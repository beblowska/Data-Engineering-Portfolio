from src.extract import extract_data
from src.validate import validate_data
from src.transform import transform_data
from src.load import load_data

from src.logger import get_logger
from src.config_loader import load_config
from src.retry import retry

logger = get_logger(__name__)

def run_pipeline():

    config = load_config()

    logger.info("PIPELINE STARTED")

    try:
        logger.info("Extracting data...")
        df = extract_data(config["input_file"])

        logger.info("Validating data...")
        validate_data(df)

        logger.info("Transforming data...")
        df = transform_data(df)

        logger.info("Loading data...")
        retry(lambda: load_data(
            df,
            config["output_file"],
            config["s3_bucket"],
            config["s3_prefix"]
        ), retries=config["retry_count"])

        logger.info("PIPELINE SUCCESS")

    except Exception as e:
        logger.error(f"PIPELINE FAILED: {e}")
        raise

if __name__ == "__main__":
    run_pipeline()