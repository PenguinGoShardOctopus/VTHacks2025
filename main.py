from fastapi import FastAPI
from pydantic import BaseModel
import sys

# --- 1. Import Core Logic ---
# Ensure your complex setup (databricks_flow.py) is ready and accessible.
try:
    from databricks_flow import get_schema_for_user_query
except ImportError:
    print("FATAL: Cannot import 'databricks_flow.py'. Make sure the file exists.")
    sys.exit(1)
except Exception as e:
    # Handles immediate environment loading errors from the imported file
    print(f"FATAL: Error during dependency setup: {e}")
    sys.exit(1)


app = FastAPI()

# --- 2. Input Model ---
# This defines the expected JSON body structure for the POST request.
class QueryInput(BaseModel):
    query: str


@app.post("/get_schema")
def get_schema(input_data: QueryInput):
    """
    Accepts a user query, uses the LLM to select the relevant table, 
    and returns the schema (column names and types).
    """
    # The complexity is handled entirely inside this function call.
    schema = get_schema_for_user_query(input_data.query)
    
    # FastAPI automatically converts the Python list of dictionaries (schema) into JSON.
    return schema
