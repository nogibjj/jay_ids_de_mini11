"""
Main workflow for the US_birth dataset using Databricks.
"""

import os
from mylib.extract import extract
from mylib.transform_load import transform_data, loadDataToDelta
from mylib.query import query
from dotenv import load_dotenv
import logging

# Load environment variables from .env
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def verify_file_path(path, access_token, server_h):
    """
    Verifies if a file exists in DBFS.
    """
    import requests

    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://{server_h}/api/2.0/dbfs/get-status?path={path}"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        logging.info(f"File exists at path: {path}")
        return True
    else:
        logging.error(f"File does not exist at path: {path}")
        raise FileNotFoundError(f"File not found at {path}. Response: {response.text}")


def main_workflow():
    """
    Executes the end-to-end workflow for the US_birth dataset.
    """
    try:
        access_token = os.getenv("ACCESS_TOKEN")
        server_h = os.getenv("SERVER_HOSTNAME")

        if not access_token or not server_h:
            raise ValueError(
                "ACCESS_TOKEN or SERVER_HOSTNAME not set in environment variables."
            )

        file_path = "dbfs:/FileStore/tables/US_birth.csv"
        delta_table_path = "dbfs:/FileStore/tables/US_birth_delta_table"

        # Verify file path exists
        verify_file_path(file_path, access_token, server_h)

        # Extract
        logging.info("Starting data extraction...")
        extract(file_path=file_path)

        # Transform and Load
        logging.info("Transforming and loading data...")
        transform_data(file_path)
        loadDataToDelta(file_path, delta_table_path)

        # Query
        logging.info("Running query on Delta table...")
        query()

        logging.info("Workflow completed successfully!")

    except Exception as e:
        logging.error(f"An error occurred during the workflow: {e}")
        raise


if __name__ == "__main__":
    main_workflow()
