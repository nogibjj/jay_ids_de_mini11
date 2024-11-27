"""
Test Databricks functionality for the US_birth dataset pipeline.
"""

import requests
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/nmc58_mini_project11"
url = f"https://{server_h}/api/2.0"


# Function to check if a file path exists and auth settings still work
def check_filestore_path(path, headers):
    try:
        # Make a request to check the file path's status in DBFS
        response = requests.get(f"{url}/dbfs/get-status?path={path}", headers=headers)
        response.raise_for_status()
        return response.json().get("path") is not None
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as e:
        print(f"Error checking file path: {e}")
    return False


# Test if the specified FILESTORE_PATH exists
def test_databricks():
    """
    Tests whether the specified FILESTORE_PATH exists in Databricks DBFS.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    result = check_filestore_path(FILESTORE_PATH, headers)
    assert (
        result is True
    ), f"Test failed: Path {FILESTORE_PATH} does not exist or is inaccessible."


if __name__ == "__main__":
    test_databricks()
    print(f"Test passed: Path {FILESTORE_PATH} is accessible.")
