import os
from dotenv import load_dotenv
import databricks.sql

# 1. Load Environment Variables
load_dotenv()

# Load configuration into global constants
try:
    DB_HOST = os.environ['DB_SERVER_HOSTNAME']
    DB_PATH = os.environ['DB_HTTP_PATH']
    DB_TOKEN = os.environ['DB_ACCESS_TOKEN']
    DB_CATALOG = os.environ['DB_CATALOG']
    DB_SCHEMA = os.environ['DB_SCHEMA']
except KeyError as e:
    # Minimal check to prevent script from failing immediately if .env is not present
    print(f"Error: Missing environment variable {e}. Please check your .env file.")
    exit(1)


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

# ----------------- EXECUTION BLOCK FOR TESTING -----------------

if __name__ == "__main__":
    
    table_to_test = 'housing' 
    
    print(f"Targeting: {DB_CATALOG}.{DB_SCHEMA}.{table_to_test}")
    
    try:
        schema = get_table_schema(table_to_test)
        
        if schema:
            print("\n--- SCHEMA RETRIEVED ---")
            for col in schema:
                print(f"- {col['column_name'].ljust(30)} {col['data_type']}")
        else:
            print(f"\nWARNING: Table '{table_to_test}' not found in the specified location.")
            
    except Exception as e:
        print(f"\n[ERROR] Execution failed (check token, path, and endpoint status): {e}")