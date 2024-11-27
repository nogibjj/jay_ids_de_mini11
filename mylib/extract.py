"""
Extract module for the US_birth dataset.
"""

import os
import requests
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Validate environment variables
if not server_h or not access_token:
    raise ValueError(
        "SERVER_HOSTNAME or ACCESS_TOKEN is not set in the environment variables."
    )

headers = {"Authorization": f"Bearer {access_token}"}
url = f"https://{server_h}/api/2.0"

# Default file path for the dataset in DBFS
FILESTORE_PATH = "dbfs:/FileStore/tables/US_birth.csv"


def check_filestore_path(path, headers):
    """
    Checks if the file exists in DBFS.

    Args:
        path (str): Path to the file in DBFS.
        headers (dict): HTTP headers with authentication.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    try:
        response = requests.get(url + f"/dbfs/get-status?path={path}", headers=headers)
        response.raise_for_status()
        logging.info(f"File exists at: {path}")
        return response.json().get("path") is not None
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Request error occurred: {req_err}")
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
    return False


def extract(file_path=FILESTORE_PATH):
    """
    Verifies that the dataset exists in the specified DBFS path.

    Args:
        file_path (str): Path to the file in DBFS.

    Returns:
        str: Verified file path.

    Raises:
        FileNotFoundError: If the file does not exist at the specified path.
    """
    logging.info(f"Verifying the dataset at: {file_path}...")
    if not check_filestore_path(file_path, headers):
        raise FileNotFoundError(f"The file at path {file_path} does not exist.")
    logging.info(f"Dataset verified at: {file_path}")
    return file_path


if __name__ == "__main__":
    # Test the extraction process
    try:
        extracted_file_path = extract()
        logging.info(f"Extracted file path: {extracted_file_path}")
    except Exception as e:
        logging.error(f"Error during extraction: {e}")

from dotenv import load_dotenv
import os

load_dotenv()

print(f"Access Token: {os.getenv('ACCESS_TOKEN')}")
print(f"Server Hostname: {os.getenv('SERVER_HOSTNAME')}")
