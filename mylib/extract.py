"""
Extract module for the US_birth dataset.
"""

import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
headers = {"Authorization": f"Bearer {access_token}"}
url = f"https://{server_h}/api/2.0"

# File path for the dataset in DBFS
FILESTORE_PATH = "dbfs:/FileStore/tables/US_birth.csv"


def check_filestore_path(path, headers):
    """
    Checks if the file exists in DBFS.
    """
    try:
        response = requests.get(url + f"/dbfs/get-status?path={path}", headers=headers)
        response.raise_for_status()
        return response.json().get("path") is not None
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as e:
        print(f"Error checking file path: {e}")
    return False


def extract(file_path=FILESTORE_PATH):
    """
    Verifies that the dataset exists in the specified DBFS path.
    If the file does not exist, raises an error.
    """
    print(f"Verifying the dataset at: {file_path}...")
    if not check_filestore_path(file_path, headers):
        raise FileNotFoundError(f"The file at path {file_path} does not exist.")
    print(f"Dataset verified at: {file_path}")
    return file_path


if __name__ == "__main__":
    # Test the extraction process
    try:
        extracted_file_path = extract()
        print(f"Extracted file path: {extracted_file_path}")
    except Exception as e:
        print(f"Error during extraction: {e}")
