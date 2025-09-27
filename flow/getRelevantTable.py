import os
from dotenv import load_dotenv
import databricks.sql

# 1. Load Environment Variables (Assuming this block is still at the top)
load_dotenv()
# ... (Global constants DB_HOST, DB_PATH, DB_TOKEN, DB_CATALOG, DB_SCHEMA are loaded here) ...

# Ensure global constants are loaded for the example
try:
    DB_HOST = os.environ['DB_SERVER_HOSTNAME']
    DB_PATH = os.environ['DB_HTTP_PATH']
    DB_TOKEN = os.environ['DB_ACCESS_TOKEN']
    DB_CATALOG = os.environ['DB_CATALOG']
    DB_SCHEMA = os.environ['DB_SCHEMA']
except KeyError:
    # Simplified error handling for demonstration
    print("FATAL: Environment variables not loaded.")
    exit(1)


# --- Existing Function (for context, but unchanged) ---
def get_table_schema(table):
    """Retrieves schema using only the table name."""
    
    with databricks.sql.connect(
        server_hostname=DB_HOST,
        http_path=DB_PATH,
        access_token=DB_TOKEN
    ) as connection:
        
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT column_name, data_type
                FROM {DB_CATALOG}.information_schema.columns
                WHERE table_schema = '{DB_SCHEMA}' AND table_name = '{table}'
                ORDER BY ordinal_position
                """
            )
            result = cursor.fetchall()
            
            # Returns a list of dictionaries
            return [{"column_name": r[0], "data_type": r[1]} for r in result]


# --- NEW FUNCTION: Get all Tables and Descriptions ---
def get_all_tables_metadata():
    """
    Retrieves the table name and description (comment) for all tables 
    in the configured catalog and schema.
    """
    
    with databricks.sql.connect(
        server_hostname=DB_HOST,
        http_path=DB_PATH,
        access_token=DB_TOKEN
    ) as connection:
        
        with connection.cursor() as cursor:
            # Querying the information_schema.tables view
            cursor.execute(
                f"""
                SELECT table_name, comment
                FROM {DB_CATALOG}.information_schema.tables
                WHERE table_schema = '{DB_SCHEMA}'
                ORDER BY table_name
                """
            )
            result = cursor.fetchall()
            
            # Formatting the output for the LLM context
            return [
                {
                    "table_name": r[0], 
                    # Handle NULL comments by using an empty string
                    "description": r[1] if r[1] else "No description available."
                } 
                for r in result
            ]

# ----------------- EXECUTION BLOCK FOR TESTING -----------------

if __name__ == "__main__":
    
    # 1. Test retrieving all table metadata
    print(f"Retrieving table metadata from {DB_CATALOG}.{DB_SCHEMA}...")
    try:
        table_metadata = get_all_tables_metadata()
        
        if table_metadata:
            print(f"\n--- FOUND {len(table_metadata)} TABLES ---")
            
            llm_context = []
            for meta in table_metadata:
                # Store data in a clean format for the LLM
                context_string = (
                    f"Table Name: {meta['table_name']}\n"
                    f"Description: {meta['description']}\n"
                )
                llm_context.append(context_string)
                
                print(context_string)

            # 2. Example of the final context you would send to the LLM
            final_llm_prompt_context = "\n".join(llm_context)
            
            print("\n--- LLM CONTEXT STRING EXAMPLE (for prompt engineering) ---")
            print(final_llm_prompt_context)

        else:
            print(f"\nWARNING: No tables found in {DB_CATALOG}.{DB_SCHEMA}.")
            
    except Exception as e:
        print(f"\n[ERROR] Execution failed: {e}")