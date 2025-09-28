import os
from databricks.sdk import WorkspaceClient
# --- Databricks Configuration ---
DATABRICKS_HOST = os.getenv("DB_SERVER_HOSTNAME")
DATABRICKS_TOKEN = os.getenv("DB_ACCESS_TOKEN")
DATABRICKS_CATALOG = os.getenv("DB_CATALOG", "workspace")
DATABRICKS_SCHEMA = os.getenv("DB_SCHEMA", "dev")
DATABRICKS_VOLUME = os.getenv("DB_VOLUME", "files")

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

    w.files.upload(
        full_volume_path,
        contents=file_content,
        overwrite=True,
    )
    return True
    
def trigger_csv_to_table(filename):
    try:   
        # Trigger the job run
        print(f"Triggering job ID: {148324980352233} with parameters: file_name = {filename}")
        new_run = w.jobs.run_now(job_id=148324980352233, job_parameters={"file_name": filename})

        print(f"Job triggered successfully. Run ID: {new_run.run_id}")
        return new_run.run_id

    except Exception as e:
        print(f"An error occurred: {e}")
