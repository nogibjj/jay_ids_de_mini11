"""
Main workflow for the US_birth dataset using Databricks.
"""

# Import functions from modularized files
from mylib.extract import extract
from mylib.transform_load import transform_data, loadDataToDelta
from mylib.query import query

def main_workflow():
    """
    Executes the end-to-end workflow for the US_birth dataset.
    """

    print("Extracting data...")
    extract(
        file_path="dbfs:/mnt/data/US_birth.csv"
    )

    print("Loading data into Spark DataFrame...")
    transform_data("dbfs:/FileStore/nmc58_mini_project11/US_birth.csv")

    delta_table_path = ("dbfs:/FileStore/nmc58_mini_project11/"
                        "nmc58_mini_project11_delta_table")
    dbfs_file_path = "dbfs:/FileStore/nmc58_mini_project11/US_birth.csv"

    print("Writing data to Delta Lake...")
    loadDataToDelta(dbfs_file_path, delta_table_path)
    
    print("Running example query...")
    query()


if __name__ == "__main__":
    main_workflow()
