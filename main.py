from datetime import datetime
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from databricks_integration import upload_csv_to_databricks, trigger_csv_to_table
from pydantic import BaseModel
from databricks_flow import generate_visualization_from_query

app = FastAPI()

# This defines the expected JSON body structure for the POST request.
class QueryInput(BaseModel):
    query: str


@app.post("/generate_visualization")
def generate_viz(input_data: QueryInput):
    """
    Accepts a user query and returns a full JSON payload with
    a recommended visualization and the corresponding data.
    """
    try:
        # The entire complex workflow is handled by this single function call.
        visualization_data = generate_visualization_from_query(input_data.query)
        
        # FastAPI automatically converts the final Python dictionary into a JSON response.
        return visualization_data
    except (ValueError, LookupError) as e:
        # Handle known errors from our flow (e.g., no tables found, LLM returns invalid format)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Catch any other unexpected errors (e.g., DB connection, LLM API failure)
        raise HTTPException(status_code=500, detail=f"An internal error occurred: {e}")



@app.post("/upload/")
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
