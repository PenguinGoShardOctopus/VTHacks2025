from datetime import datetime

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from databricks_integration import upload_csv_to_databricks, trigger_csv_to_table
from google.genai import Client

app = FastAPI()
client = Client()


@app.post("/trigger")
def trigger(query: str):
    """
    Accepts a user query, uses the LLM to select the relevant table, 
    and returns the schema (column names and types).
    """
    chat = client.chats.create(
        model='gemini-2.5-flash'
    )
    del chat
    return

@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    """
    Receives a CSV file, uploads it to Databricks, and triggers table creation.
    """
    if not file.filename.endswith(".csv"):
        raise HTTPException(
            status_code=400, detail="Only CSV files are allowed."
        )
    try:
        file_content = await file.read()

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        databricks_file_name = f"{file.filename.replace('.csv', '')}.csv"

        upload_success = upload_csv_to_databricks(
            file_content, databricks_file_name
        )

        if not upload_success:
            raise HTTPException(
                status_code=500, detail="Failed to upload CSV to Databricks."
            )

        run_id = trigger_csv_to_table(databricks_file_name.split(".")[0])

        return JSONResponse(
            status_code=200,
            content={
                "message": "CSV uploaded and table creation initiated successfully.",
                "databricks_path": databricks_file_name,
                "run_id": run_id
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")