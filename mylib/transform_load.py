import os
import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import json
import base64

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("DATABRICKS_KEY")
FILESTORE_PATH = "dbfs:/FileStore/nmc58_mini_project11"
headers = {"Authorization": "Bearer %s" % access_token}
url = "https://" + server_h + "/api/2.0"
dbfs_path = FILESTORE_PATH + "/US_birth.csv"  # The target path in DBFS

# Initialize Spark session
spark = (
    SparkSession.builder.appName("Spark App")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1")
    .getOrCreate()
)


# Transform and clean the data using Spark
def transform_data(file_path):
    try:
        # Load the CSV file into a Spark DataFrame
        data = spark.read.csv(file_path, header=True, inferSchema=True)
        print(f"File loaded successfully: {file_path}")
    except Exception as e:
        print(f"Error loading file: {e}")
        return None

    return data


def perform_query(path, headers, data={}):
    session = requests.Session()
    resp = session.request(
        "POST", url + path, data=json.dumps(data), verify=True, headers=headers
    )
    return resp.json()


def loadDataToDBFS(pathLocal, pathDBFS, headers):
    if not os.path.exists(pathLocal):
        print(f"Error: The file {pathLocal} does not exist.")
        return

    # Open and read the file content
    with open(pathLocal, "rb") as file:
        content = file.read()

    # Create the file in DBFS
    create_data = {"path": pathDBFS, "overwrite": True}
    handle = perform_query("/dbfs/create", headers, data=create_data)["handle"]

    # Upload content in chunks
    for i in range(0, len(content), 2**20):
        chunk = base64.standard_b64encode(content[i : i + 2**20]).decode()
        perform_query(
            "/dbfs/add-block", headers, data={"handle": handle, "data": chunk}
        )

    # Close the file handle
    perform_query("/dbfs/close", headers, data={"handle": handle})
    print(f"File {pathLocal} uploaded to {pathDBFS} successfully.")


def loadDataToDelta(file_path, delta_table_path):
    try:
        # Load the CSV file from DBFS
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        print(f"File loaded successfully from: {file_path}")

        # Write the DataFrame to Delta Lake
        df.write.format("delta").mode("overwrite").save(delta_table_path)
        print(f"Data successfully written to Delta Lake at: {delta_table_path}")

    except Exception as e:
        print(f"Error while loading data to Delta Lake: {e}")


# Define paths
file_location = "data/US_birth.csv"  # Local path to the US_birth.csv file
delta_table_path = (
    "dbfs:/FileStore/nmc58_mini_project11/" "nmc58_mini_project11_delta_table"
)
dbfs_file_path = "dbfs:/FileStore/nmc58_mini_project11/US_birth.csv"

# Run the steps
loadDataToDBFS(file_location, dbfs_path, headers)  # Upload local file to DBFS
transform_data(dbfs_file_path)  # Load and transform data in Spark
loadDataToDelta(dbfs_file_path, delta_table_path)  # Write data to Delta Lake
