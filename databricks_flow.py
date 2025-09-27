import os
from dotenv import load_dotenv
import databricks.sql
from google import genai
import sys # Used for gracefully exiting on error

# Load all environment variables
load_dotenv()

# --- Connection and API Setup ---
try:
    # Databricks Config
    DB_HOST = os.environ['DB_SERVER_HOSTNAME']
    DB_PATH = os.environ['DB_HTTP_PATH']
    DB_TOKEN = os.environ['DB_ACCESS_TOKEN']
    DB_CATALOG = os.environ['DB_CATALOG']
    DB_SCHEMA = os.environ['DB_SCHEMA']

    # Gemini API Client
    client = genai.Client()
    
except KeyError as e:
    print(f"FATAL: Missing environment variable {e}. Check your .env file and API key.")
    sys.exit(1)


# --- Existing Schema Retrieval Function (Unchanged) ---
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


# --- Metadata Retrieval Function (Unchanged) ---
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


# --- Core Logic: Prepare Prompt and Call LLM ---
def run_table_selection_flow():
    """
    1. Gathers table metadata.
    2. Takes user input.
    3. Formats the full prompt.
    4. Calls the Gemini API to get the relevant table name.
    5. Calls the schema function with the result.
    """
    
    # 1. Get Table Metadata
    table_metadata = get_all_tables_metadata()
    if not table_metadata:
        print(f"Error: No tables found in {DB_CATALOG}.{DB_SCHEMA}.")
        return

    # 2. Format Context for LLM
    table_context_list = []
    for meta in table_metadata:
        table_context_list.append(
            f"Table Name: {meta['table_name']}\nDescription: {meta['description']}"
        )
    table_info = "\n---\n".join(table_context_list)
    
    # 3. Get User Input
    print("\n--- Table Selection Flow ---")
    user_query = input("Enter your visualization query (e.g., 'Analyze housing prices'):\n> ")

    # 4. Define LLM Instruction
    llm_instruction = (
        "The user will inquire and ask to visualize some type of data. "
        "You will be given a list of table names along with their description. "
        "You are supposed to pick the most relevant table, and strictly return the name, "
        "DO NOT return anything but the name of the most relevant table."
    )

    # 5. Assemble Final Prompt
    final_prompt = f"{llm_instruction}\n\nuser query: {user_query}\n\ntable(s):\n{table_info}"
    
    # 6. Call the LLM API
    print("\nSending prompt to Gemini...")
    
    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=final_prompt
        )
        
        # 7. Process LLM Output
        selected_table = response.text.strip()
        print(f"\n[Gemini Selection] Selected Table: '{selected_table}'")
        
        # 8. Execute the original schema function with the LLM's choice
        print(f"Retrieving schema for {selected_table}...")
        schema_data = get_table_schema(selected_table)

        # 9. Print final results
        if schema_data:
            print(f"\n--- SUCCESS: SCHEMA FOR {selected_table.upper()} ---")
            for col in schema_data:
                print(f"- {col['column_name'].ljust(30)} {col['data_type']}")
        else:
            print(f"\nWARNING: Schema retrieval failed for table '{selected_table}'.")

    except Exception as e:
        print(f"\n[API ERROR] Failed to connect to or process response from Gemini/Databricks: {e}")


# ----------------- EXECUTION BLOCK -----------------

if __name__ == "__main__":
    run_table_selection_flow()