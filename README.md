[![CI](https://github.com/nogibjj/python-ruff-template/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/python-ruff-template/actions/workflows/cicd.yml)
## Template for Python projects with RUFF linter

Data Pipeline with Databricks
Overview
This project focuses on building a Data Extraction and Transformation Pipeline to process tennis match data from the FiveThirtyEight datasets. The pipeline leverages Databricks, the Databricks API, and Python libraries for efficient data retrieval, transformation, and visualization.

Key Components
1. Data Extraction
Fetches tennis match data from specified URLs using the requests library.
Stores the downloaded data in the Databricks FileStore for further processing.
2. Databricks Environment Setup
Establishes a secure connection to the Databricks environment.
Utilizes environment variables (SERVER_HOSTNAME and ACCESS_TOKEN) for authentication.
3. Data Transformation and Loading
Converts the CSV data into a Spark DataFrame.
Transforms the DataFrame into a Delta Lake table for efficient querying and stores it in the Databricks environment.
4. Query Transformation and Visualization
Executes predefined Spark SQL queries to perform data transformations.
Creates visualizations from the transformed data using Spark DataFrames.
5. File Path Checking
Implements a utility function to verify the existence of file paths in the Databricks FileStore.
As many functions are exclusive to Databricks, the GitHub environment is limited to API connection tests via the Databricks API and requests library.
6. Automated Trigger with GitHub Push
Configures the Databricks API to initiate a pipeline job when changes are pushed to this repository.
Preparation Steps
Create a Databricks workspace on Azure.
Connect your GitHub account to the Databricks workspace.
Set up a global init script to store environment variables during cluster initialization.
Create a Databricks cluster with PySpark support.
Clone this repository into your Databricks workspace.
Define a Databricks job to execute the pipeline:
Extract Task: mylib/extract.py
Transform and Load Task: mylib/transform_load.py
Query and Visualization Task: mylib/query_viz.py
Automated Job Workflow
Triggers automatically upon repository updates.
Executes the full data pipeline in the Databricks environment.
Development Instructions
Check format and resolve errors:
Open Codespaces or run the repository locally.
Format code: make format
Lint code: make lint
Visualizations:
Example outputs include viz1 and viz2.
References
Python Ruff Template
Global Environment Variables in Databricks
Databricks FileStore Documentation
Delta Lake Overview
Azure Databricks Data Engineer Path
Getting Started with Databricks Data Pipelines
