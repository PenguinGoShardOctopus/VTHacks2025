import os
from dotenv import load_dotenv
import databricks.sql
from google import genai
import sys 

# Load all environment variables
load_dotenv()

# --- Connection and API Setup ---
DB_HOST = os.environ['DB_SERVER_HOSTNAME']
DB_PATH = os.environ['DB_HTTP_PATH']
DB_TOKEN = os.environ['DB_ACCESS_TOKEN']
DB_CATALOG = os.environ['DB_CATALOG']
DB_SCHEMA = os.environ['DB_SCHEMA']

client = genai.Client()
    

# --- Helper Functions (Unchanged) ---
def get_table_schema(table):
    """Retrieves schema using only the table name."""
    with databricks.sql.connect(server_hostname=DB_HOST, http_path=DB_PATH, access_token=DB_TOKEN) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT column_name, data_type
                FROM {DB_CATALOG}.information_schema.columns
                WHERE table_schema = '{DB_SCHEMA}' AND table_name = '{table}'
                ORDER BY ordinal_position
                """
            )
            return [{"column_name": r[0], "data_type": r[1]} for r in cursor.fetchall()]

def get_all_tables_metadata():
    """Retrieves the table name and description (comment) for all tables."""
    with databricks.sql.connect(server_hostname=DB_HOST, http_path=DB_PATH, access_token=DB_TOKEN) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT table_name, comment
                FROM {DB_CATALOG}.information_schema.tables
                WHERE table_schema = '{DB_SCHEMA}'
                ORDER BY table_name
                """
            )
            return [
                {"table_name": r[0], "description": r[1] if r[1] else "No description available."} 
                for r in cursor.fetchall()
            ]


# --- Core Execution Logic for FastAPI ---
def get_schema_for_user_query(user_query: str):
    """
    Executes the full chain: Gathers metadata, calls LLM, retrieves schema.
    """
    
    # 1. Get Table Metadata
    table_metadata = get_all_tables_metadata()
    if not table_metadata:
        raise ValueError(f"No tables found in {DB_CATALOG}.{DB_SCHEMA}.")

    # 2. Format Context for LLM
    table_context_list = []
    for meta in table_metadata:
        table_context_list.append(
            f"Table Name: {meta['table_name']}\nDescription: {meta['description']}"
        )
    table_info = "\n---\n".join(table_context_list)
    
    # 3. Define LLM Instruction
    llm_instruction = (
        "The user will inquire and ask to visualize some type of data. "
        "You will be given a list of table names along with their description. "
        "You are supposed to pick the most relevant table, and strictly return the name, "
        "DO NOT return anything but the name of the most relevant table."
    )

    # 4. Assemble Final Prompt
    final_prompt = f"{llm_instruction}\n\nuser query: {user_query}\n\ntable(s):\n{table_info}"
    
    # 5. Call the LLM API
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=final_prompt
    )
    
    # 6. Process LLM Output
    selected_table = response.text.strip()
    
    # 7. Execute the original schema function with the LLM's choice
    schema_data = get_table_schema(selected_table)
    
    if not schema_data:
         # Handle case where LLM selects a table that doesn't exist
         raise LookupError(f"Table '{selected_table}' was selected by LLM but not found.")

    return schema_data # Return the data to FastAPI