from pyspark.sql import SparkSession

# Initialize Spark session
spark = (
    SparkSession.builder.appName("US Birth Query App")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1")
    .getOrCreate()
)


# Markdown file to log the SQL functions and queries
def logQuery(query):
    with open("queryLog.md", "a") as file:
        file.write(f"```sql\n{query}\n```\n\n")


# Register the Delta table as a temporary view
delta_table_path = "dbfs:/FileStore/nmc58_mini_project11/US_birth_delta_table"
try:
    spark.read.format("delta").load(delta_table_path).createOrReplaceTempView(
        "US_birth_delta_table"
    )
except Exception as e:
    print(f"Failed to load Delta table: {e}")


# Define a query function
def query():
    query = """
        SELECT year, month, gender, SUM(births) as total_births
        FROM US_birth_delta_table
        WHERE month = 5
        GROUP BY year, month, gender
        ORDER BY year, gender
    """
    # Log the query
    logQuery(query)
    try:
        query_result = spark.sql(query)
        query_result.show()
    except Exception as e:
        print(f"Query execution failed: {e}")


# Call the query function
if __name__ == "__main__":
    query()
