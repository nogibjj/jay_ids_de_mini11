"""
Query module for the US_birth dataset.
"""

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("US Birth Query").getOrCreate()

# File path for the dataset in DBFS
FILESTORE_PATH = "dbfs:/FileStore/tables/US_birth.csv"


def load_data(file_path):
    """
    Loads the US_birth.csv dataset from the given DBFS path into a Spark DataFrame.
    """
    try:
        print(f"Loading data from: {file_path}")
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        print("Data loaded successfully.")
        return df
    except Exception as e:
        print(f"Error loading data from {file_path}: {e}")
        raise


def create_temp_view(df, view_name):
    """
    Creates a temporary view from the given Spark DataFrame.
    """
    try:
        print(f"Creating temporary view: {view_name}")
        df.createOrReplaceTempView(view_name)
        print(f"Temporary view '{view_name}' created successfully.")
    except Exception as e:
        print(f"Error creating temporary view: {e}")
        raise


def run_query(query, view_name="US_birth_view"):
    """
    Executes the given SQL query on the temporary view and returns the results.
    """
    try:
        print(f"Running query on view '{view_name}':\n{query}")
        result = spark.sql(query)
        print("Query executed successfully.")
        return result
    except Exception as e:
        print(f"Error executing query: {e}")
        raise


def query():
    """
    Main query function to demonstrate SQL queries on the US_birth dataset.
    """
    # Load the data
    df = load_data(FILESTORE_PATH)

    # Create a temporary SQL view
    view_name = "US_birth_view"
    create_temp_view(df, view_name)

    # Example query: Fetch top 10 rows
    example_query = """
        SELECT *
        FROM US_birth_view
        LIMIT 10
    """
    result = run_query(example_query, view_name)
    result.show()


if __name__ == "__main__":
    try:
        query()
    except Exception as e:
        print(f"Error in query module: {e}")
