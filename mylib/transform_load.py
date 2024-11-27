"""
Transform and Load module for the US_birth dataset.
"""

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("US Birth Transform and Load").getOrCreate()

# File path for the dataset in DBFS
FILESTORE_PATH = "dbfs:/FileStore/tables/US_birth.csv"
DELTA_TABLE_PATH = "dbfs:/FileStore/tables/US_birth_delta_table"


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


def transform_data(df):
    """
    Transforms the dataset by performing basic cleaning or feature engineering.
    """
    try:
        print("Transforming data...")
        # Example transformation: Rename columns for clarity
        transformed_df = (
            df.withColumnRenamed("col1", "Year")
            .withColumnRenamed("col2", "Month")
            .withColumnRenamed("col3", "Births")
        )
        print("Data transformed successfully.")
        return transformed_df
    except Exception as e:
        print(f"Error transforming data: {e}")
        raise


def write_to_delta(df, delta_table_path):
    """
    Writes the transformed DataFrame to Delta Lake in DBFS.
    """
    try:
        print(f"Writing data to Delta Lake at: {delta_table_path}")
        df.write.format("delta").mode("overwrite").save(delta_table_path)
        print("Data written to Delta Lake successfully.")
    except Exception as e:
        print(f"Error writing data to Delta Lake: {e}")
        raise


def transform_load(file_path=FILESTORE_PATH, delta_table_path=DELTA_TABLE_PATH):
    """
    Main function to load, transform, and write the dataset.
    """
    # Step 1: Load the data
    df = load_data(file_path)

    # Step 2: Transform the data
    transformed_df = transform_data(df)

    # Step 3: Write transformed data to Delta Lake
    write_to_delta(transformed_df, delta_table_path)


if __name__ == "__main__":
    try:
        transform_load()
    except Exception as e:
        print(f"Error in transform_load module: {e}")
