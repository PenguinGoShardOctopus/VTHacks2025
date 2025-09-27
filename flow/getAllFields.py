import os
from dotenv import load_dotenv
import databricks.sql

# 1. Load Environment Variables
load_dotenv()

# Load configuration into global constants (simplifies function signature)
DB_HOST = os.environ['DB_SERVER_HOSTNAME']
DB_PATH = os.environ['DB_HTTP_PATH']
DB_TOKEN = os.environ['DB_ACCESS_TOKEN']
DB_CATALOG = os.environ['DB_CATALOG']
DB_SCHEMA = os.environ['DB_SCHEMA']

table = 'housing'

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

# Example usage (assuming 'users' is the table name):
# schema = get_table_schema("users")
# print(schema)