import requests
from dotenv import load_dotenv
import os
import json
import base64

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/nmc58_mini_project11"
headers = {"Authorization": "Bearer %s" % access_token}
url = "https://" + server_h + "/api/2.0"

LOG_FILE = "final_pyspark_output.md"


def log_output(operation, output, query=None):
    """Logs output to a markdown file."""
    with open(LOG_FILE, "a") as file:
        file.write(f"The operation is {operation}\n\n")
        if query:
            file.write(f"The query is {query}\n\n")
        file.write("The truncated output is: \n\n")
        file.write(output)
        file.write("\n\n")


def perform_request(path, method="POST", data=None):
    """Performs an HTTP request to the Databricks API."""
    session = requests.Session()
    response = session.request(
        method=method,
        url=f"{url}{path}",
        headers=headers,
        data=json.dumps(data) if data else None,
        verify=True,
    )
    return response.json()


def upload_file_from_url(url, dbfs_path, overwrite):
    """Uploads a file from a URL to DBFS."""
    response = requests.get(url)
    if response.status_code == 200:
        content = response.content
        # Create file handle
        handle = perform_request(
            "/dbfs/create", data={"path": dbfs_path, "overwrite": overwrite}
        )["handle"]
        print(f"Uploading file: {dbfs_path}")
        # Add file content in chunks
        for i in range(0, len(content), 2**20):
            perform_request(
                "/dbfs/add-block",
                data={
                    "handle": handle,
                    "data": base64.standard_b64encode(content[i : i + 2**20]).decode(),
                },
            )
        # Close the handle
        perform_request("/dbfs/close", data={"handle": handle})
        print(f"File {dbfs_path} uploaded successfully.")
    else:
        print("Failed to download")


def extract(
    file_path="https://raw.githubusercontent.com/jayliu1016/Datasets/main/US_birth.csv",
    destination_path="data/US_birth.csv",
):
    """Extracts the US_birth dataset to a local directory."""

    # Ensure the directory exists
    os.makedirs(os.path.dirname(destination_path), exist_ok=True)

    try:
        # Copy the uploaded file to the local destination
        with open(file_path, "rb") as source_file:
            with open(destination_path, "wb") as dest_file:
                dest_file.write(source_file.read())
        print(f"File copied successfully: {destination_path}")
        return destination_path
    except Exception as e:
        print(f"Failed to copy file. Error: {e}")
        return None
