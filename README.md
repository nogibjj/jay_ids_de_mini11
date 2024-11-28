[![CI](https://github.com/nogibjj/python-ruff-template/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/python-ruff-template/actions/workflows/cicd.yml)
## Template for Python projects with RUFF linter

## Databricks
🏗️ Requirements
Create a data pipeline using Databricks.
Include at least one data source (US_birth.csv) and one data sink (Delta Lake).
🛠️ Setup Instructions
Cloning the Repository into Databricks
Log into Databricks and navigate to the Workspaces section.
Under the Users tab, locate your email ID and click on it.
Click the Create button in the top-right corner and select Git Folder.
Paste the GitHub repository URL:
https://github.com/nogibjj/jay_ids_de_mini11 <br>

Setting Up the Compute Cluster
Navigate to the Compute section in Databricks.
Click Create and select Personal Compute.
Configure your cluster as shown in the image below:

Installing Required Libraries in Your Compute Cluster
Open your personal compute cluster and navigate to the Libraries tab.

Test locally
<img width="1512" alt="Screenshot 2024-11-27 at 10 11 53 AM" src="https://github.com/user-attachments/assets/a4def9e8-e792-4dd8-a75c-63d5cc3c2553">
test on databricks
<img width="1512" alt="Screenshot 2024-11-27 at 10 12 03 AM" src="https://github.com/user-attachments/assets/5bc530b8-c0d2-4586-bf92-7d84f67c4d39">

Add the following libraries via PyPI to ensure smooth project execution:

pandas
pytest
python-dotenv
Follow the Databricks prompts to install the libraries. <br>

Installing libraries through PyPI:

Linking Databricks to Your GitHub Account
Navigate to Account Settings by clicking your profile icon in the top-right corner.
Select Linked Accounts and integrate your GitHub account with Databricks using a Personal Access Token. <br>

🚀 ETL Pipeline
Setting Up the ETL Pipeline
Navigate to the Workflows section in Databricks.
Click Create Job.
This project involves three stages of the ETL process, with separate tasks for Extract, Transform_Load, and Query.

Task 1: Extract
Start a new task named Extract.
Set the type to Python Script.
Set the source to Workspaces.
Specify the path to the extract.py file within your repository.
Assign the compute to your personal compute cluster.
Click Create Task. <br>

Task 2: Transform_Load
Click Add Task and name it Transform_Load.
Use the same configuration as Extract.
Under Depends On, add the Extract task.
Task 3: Query
Click Add Task and name it Query.
Use the same configuration as the previous tasks.
Under Depends On, add the Transform_Load task.
Final Pipeline
The tasks are executed sequentially:
Extract → Transform_Load → Query.
The final pipeline should look like this:
Running the Pipeline
Once all tasks are set up, click the Run button to execute the pipeline.
