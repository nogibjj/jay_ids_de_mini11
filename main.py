"""
Main workflow for the US_birth dataset using Databricks.
"""

from mylib.extract import extract
from mylib.transform_load import transform_data, loadDataToDelta
from mylib.query import query
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()


def main_workflow():
    """
    Executes the end-to-end workflow for the US_birth dataset.
    """
    access_token = os.getenv("ACCESS_TOKEN")
    server_h = os.getenv("SERVER_HOSTNAME")

    if not access_token or not server_h:
        raise ValueError(
            "ACCESS_TOKEN or SERVER_HOSTNAME not set in environment variables."
        )

    print("Extracting data...")
    extract(file_path="dbfs:/FileStore/tables/US_birth.csv")

    print("Loading data into Spark DataFrame...")
    transform_data("dbfs:/FileStore/tables/US_birth.csv")

    delta_table_path = "dbfs:/FileStore/tables/US_birth_delta_table"
    dbfs_file_path = "dbfs:/FileStore/tables/US_birth.csv"

    print("Writing data to Delta Lake...")
    loadDataToDelta(dbfs_file_path, delta_table_path)

    print("Running query on Delta table...")
    query()


if __name__ == "__main__":
    try:
        main_workflow()
    except Exception as e:
        print(f"An error occurred: {e}")
