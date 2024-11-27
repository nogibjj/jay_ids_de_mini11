"""
Test Databricks functionality for US_birth.csv file existence.
"""

import requests
from dotenv import load_dotenv
import os
import logging

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/tables/US_birth.csv"
url = f"https://{server_h}/api/2.0"

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# Function to check if a file path exists on DBFS
def check_filestore_path(path, headers):
    """
    Checks if a file exists at the given DBFS path.

    Args:
        path (str): The DBFS file path to check.
        headers (dict): Headers containing authentication information.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    try:
        response = requests.get(url + f"/dbfs/get-status?path={path}", headers=headers)
        if response.status_code == 200:
            logging.info(f"File exists at path: {path}")
            return True
        elif response.status_code == 404:
            logging.error(f"File not found at path: {path}")
            return False
        else:
            logging.error(
                f"Unexpected response code {response.status_code} for path: {path}. Response: {response.text}"
            )
            return False
    except requests.exceptions.RequestException as e:
        logging.error(f"Error checking file path: {e}")
        return False


# Test if the specified FILESTORE_PATH exists
def test_file_existence():
    """
    Tests if the US_birth.csv file exists in the specified DBFS path.
    """
    if not access_token or not server_h:
        raise ValueError(
            "SERVER_HOSTNAME or ACCESS_TOKEN not set in environment variables."
        )

    headers = {"Authorization": f"Bearer {access_token}"}
    logging.info(f"Testing file existence at: {FILESTORE_PATH}")
    assert (
        check_filestore_path(FILESTORE_PATH, headers) is True
    ), f"File does not exist at path: {FILESTORE_PATH}"


if __name__ == "__main__":
    try:
        test_file_existence()
        logging.info("Test passed successfully.")
    except AssertionError as e:
        logging.error(f"Assertion error during test: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")