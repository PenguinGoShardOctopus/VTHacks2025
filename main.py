from fastapi import FastAPI
from pydantic import BaseModel
import sys
from databricks_flow import get_schema_for_user_query



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
