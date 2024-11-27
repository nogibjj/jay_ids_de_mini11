"""
Test module for Databricks functionality and file availability.
"""

import requests
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Load server hostname and access token
access_token = os.getenv("ACCESS_TOKEN")
server_h = os.getenv("SERVER_HOSTNAME")

if not server_h or not access_token:
    raise ValueError(
        "Environment variables SERVER_HOSTNAME or ACCESS_TOKEN are not set."
    )

FILESTORE_PATH = "dbfs:/FileStore/tables/US_birth.csv"
url = f"https://{server_h}/api/2.0"


def check_filestore_path(path, headers):
    """
    Checks if the file exists in DBFS.
    """
    try:
        response = requests.get(url + f"/dbfs/get-status?path={path}", headers=headers)
        response.raise_for_status()
        return response.json().get("path") is not None
    except Exception as e:
        print(f"Error checking file path: {e}")
        return False


def test_file_existence():
    """
    Tests if the US_birth.csv file exists in the specified DBFS path.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    print(f"Testing file existence at: {FILESTORE_PATH}")
    assert (
        check_filestore_path(FILESTORE_PATH, headers) is True
    ), f"File does not exist at path: {FILESTORE_PATH}"


if __name__ == "__main__":
    try:
        print("Testing file existence...")
        test_file_existence()
        print(f"Test passed: File exists at {FILESTORE_PATH}")
    except AssertionError as e:
        print(f"Test failed: {e}")
    except Exception as e:
        print(f"Unexpected error during testing: {e}")
