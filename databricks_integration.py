import os
import requests
import json
from base64 import b64encode
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import files as d_files
from databricks.sdk.service import jobs as d_jobs
import logging

logger = logging.getLogger(__name__)

# --- Databricks Configuration ---
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG", "main")
DATABRICKS_SCHEMA = os.getenv("DATABRICKS_SCHEMA", "default")
DATABRICKS_VOLUME = os.getenv("DATABRICKS_VOLUME", "uploads")
DATABRICKS_CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID") # Required for running commands directly
DATABRICKS_SQL_WAREHOUSE_ID = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID") # For SQL statement execution

if not all([DATABRICKS_HOST, DATABRICKS_TOKEN]):
    raise ValueError(
        "DATABRICRICKS_HOST and DATABRICKS_TOKEN "
        "environment variables must be set."
    )

# Initialize Databricks SDK client (optional, but good for robust interaction)
# This will pick up credentials from environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN)
# or .databrickscfg by default.
w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)


def upload_csv_to_databricks(
    file_content: bytes, databricks_path: str
) -> bool:
    """
    Uploads the CSV file content to a Databricks Unity Catalog Volume.
    The databricks_path should be in the format /Volumes/{catalog}/{schema}/{volume}/your_file.csv
    """
    full_volume_path = (
        f"/Volumes/{DATABRICKS_CATALOG}/"
        f"{DATABRICKS_SCHEMA}/{DATABRICKS_VOLUME}/{databricks_path}"
    )
    logger.info(f"Uploading file to Databricks Volume: {full_volume_path}")

    # For larger files, you'd use the streaming upload API
    # For simplicity, using `put` for now (limited to 1MB per request when not streaming content parameter)
    # The Databricks SDK `files` service can handle larger files more gracefully
    try:
        # Ensure the parent directory exists if using standard filesystem-like operations
        # For Unity Catalog Volumes, the path structure takes care of this
        # The WorkspaceClient.files.upload method simplifies this a lot
        w.files.upload(
            full_volume_path,
            contents=file_content,
            overwrite=True,  # Overwrite if file exists
        )
        logger.info(f"File uploaded successfully to {full_volume_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload file to Databricks: {e}")
        return False
    
def trigger_csv_to_table(filename):
    try:   
        # Trigger the job run
        print(f"Triggering job ID: {491321462338714} with parameters: file_name = {filename}")
        new_run = w.jobs.run_now(job_id=491321462338714, job_parameters={"file_name": filename})

        print(f"Job triggered successfully. Run ID: {new_run.run_id}")
        return new_run.run_id

    except Exception as e:
        print(f"An error occurred: {e}")
