import os
from dotenv import load_dotenv
import databricks.sql
from google import genai
import json

# Load all environment variables
load_dotenv()

# --- Connection and API Setup ---
DB_HOST = os.environ.get('DB_SERVER_HOSTNAME')
DB_PATH = os.environ.get('DB_HTTP_PATH')
DB_TOKEN = os.environ.get('DB_ACCESS_TOKEN')
DB_CATALOG = os.environ.get('DB_CATALOG')
DB_SCHEMA = os.environ.get('DB_SCHEMA')

client = genai.Client()

# --- Constants ---
CHART_TYPES = [
    "line", "bar", "pie",
    "radar", "scatter"
]

# --- Helper Functions (Mostly Unchanged) ---
def get_table_schema(table_name: str, connection):
    """Retrieves schema for a specific table using an existing connection."""
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT column_name, data_type
            FROM {DB_CATALOG}.information_schema.columns
            WHERE table_schema = '{DB_SCHEMA}' AND table_name = '{table_name}'
            ORDER BY ordinal_position
            """
        )
        return [{"column_name": r[0], "data_type": r[1]} for r in cursor.fetchall()]

def get_all_tables_metadata(connection):
    """Retrieves metadata for all tables using an existing connection."""
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

# --- LLM-Powered Functions ---

def choose_visualization(user_query: str, schema: list) -> dict:
    """
    Uses an LLM to choose the best visualization type based on the user query and table schema.
    
    Returns:
        A dictionary like {"type": "scatter", "justification": "..."}
    """
    schema_str = "\n".join([f"- {col['column_name']} ({col['data_type']})" for col in schema])
    
    llm_instruction = (
        "You are an expert data analyst. Your task is to recommend the best chart type to answer a user's question "
        "based on the available data columns. You must choose exactly one type from the provided list.\n"
        f"Available chart types: {', '.join(CHART_TYPES)}\n"
        "Your response MUST be a single, valid JSON object with two keys: 'type' and 'justification'. "
        "Do not add any other text, explanation, or markdown formatting outside of the JSON object."
        "Do not refer to the user or their specific query. Frame it as a general best practice."

    )
    
    final_prompt = (
        f"{llm_instruction}\n\n"
        f"User Query: \"{user_query}\"\n\n"
        f"Available Columns:\n{schema_str}"
    )
    
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=final_prompt
    )
    
    try:
        # Clean up potential markdown formatting from the LLM response
        cleaned_response = response.text.strip().replace("```json", "").replace("```", "").strip()
        return json.loads(cleaned_response)
    except (json.JSONDecodeError, AttributeError) as e:
        print(f"Error decoding JSON from LLM for visualization choice: {e}")
        raise ValueError("LLM failed to return a valid JSON for visualization type.")


def generate_data_insights(user_query: str, viz_info: dict, data_sample: list) -> str:
    """
    Uses an LLM to generate a brief insight about the data based on the results.
    """
    if not data_sample:
        return "No data was returned from the query, so no insights could be generated."

    # Convert the data sample to a more readable string format for the prompt
    data_str = json.dumps(data_sample, indent=2)

    llm_instruction = (
        "You are a helpful data analyst. Your task is to provide a brief, human-readable insight based on a user's query, the chosen visualization, and a sample of the resulting data. "
        "The insight should be a short observation about patterns, trends, or notable points in the data. "
        "Focus on what the data reveals in the context of the user's question. "
        "Your response should be a single, concise string of one or two sentences. Do not add any other text or explanation."
    )

    final_prompt = (
        f"{llm_instruction}\n\n"
        f"--- CONTEXT ---\n"
        f"Original User Query: \"{user_query}\"\n"
        f"Chosen Visualization: {viz_info['type']}\n"
        f"Justification for Chart: {viz_info['justification']}\n"
        f"--- DATA SAMPLE (first {len(data_sample)} rows) ---\n"
        f"{data_str}\n"
        f"--- END CONTEXT ---\n\n"
        f"Insight:"
    )

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=final_prompt
    )

    return response.text.strip()


def generate_and_execute_sql(user_query: str, schema: list, viz_info: dict, table_name: str, connection) -> list:
    """
    Uses an LLM to generate a Databricks SQL query and then executes it.
    The LLM is instructed to create new columns on-the-fly using CTEs if needed.
    
    Returns:
        A list of dictionaries representing the query results.
    """
    schema_str = "\n".join([f"- {col['column_name']} ({col['data_type']})" for col in schema])
    
    llm_instruction = (
        "You are a Databricks SQL expert. Your goal is to write a SINGLE SQL query to fetch data that can be used to create a specified visualization. "
        "The query should be tailored to the user's request.\n"
        "IMPORTANT RULES:\n"
        "1. If a necessary column doesn't exist but can be derived from existing columns (e.g., extracting a year from a date, creating a price category), "
        "you MUST generate it on-the-fly using a Common Table Expression (CTE) with a `WITH` clause.\n"
        "2. DO NOT use `CREATE TABLE` or any other DDL statements. The query must only read data.\n"
        "3. Your response must be ONLY the raw SQL query. Do not include any explanations, comments, or markdown formatting like ```sql."
    )
    
    final_prompt = (
        f"{llm_instruction}\n\n"
        f"--- CONTEXT ---\n"
        f"User Query: \"{user_query}\"\n"
        f"Table to Query: `{DB_CATALOG}`.`{DB_SCHEMA}`.`{table_name}`\n"
        f"Chosen Visualization: {viz_info['type']} (Justification: {viz_info['justification']})\n"
        f"Available Columns:\n{schema_str}\n"
        f"--- END CONTEXT ---\n\n"
        f"SQL Query:"
    )

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=final_prompt
    )
    
    generated_sql = response.text.strip().replace("```sql", "").replace("```", "")
    
    print(f"Executing Generated SQL:\n{generated_sql}") # For debugging
    
    with connection.cursor() as cursor:
        cursor.execute(generated_sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in rows]
    
    return results

# --- Core Orchestration Logic ---

def _select_table_and_get_schema(user_query: str, connection) -> tuple[list, str]:
    """
    First step: Gathers metadata, calls LLM to select a table, and gets its schema.
    Returns a tuple of (schema, selected_table_name).
    """
    table_metadata = get_all_tables_metadata(connection)
    if not table_metadata:
        raise ValueError(f"No tables found in {DB_CATALOG}.{DB_SCHEMA}.")

    table_context_list = [
        f"Table Name: {meta['table_name']}\nDescription: {meta['description']}"
        for meta in table_metadata
    ]
    table_info = "\n---\n".join(table_context_list)
    
    llm_instruction = (
        "The user will inquire and ask to visualize some type of data. "
        "You will be given a list of table names along with their description. "
        "You are supposed to pick the most relevant table, and strictly return only the name. "
        "DO NOT return anything but the name of the most relevant table."
    )
    final_prompt = f"{llm_instruction}\n\nuser query: {user_query}\n\ntable(s):\n{table_info}"
    
    response = client.models.generate_content(model="gemini-2.5-flash", contents=final_prompt)
    selected_table = response.text.strip()
    
    schema_data = get_table_schema(selected_table, connection)
    if not schema_data:
        raise LookupError(f"Table '{selected_table}' was selected by LLM but not found.")

    return schema_data, selected_table


def generate_visualization_from_query(user_query: str) -> dict:
    """
    Executes the full Text-to-Visualization chain, now including data insights.
    """
    # Use a single connection for the entire process for efficiency
    with databricks.sql.connect(server_hostname=DB_HOST, http_path=DB_PATH, access_token=DB_TOKEN) as connection:
        
        # 1. Select the relevant table and get its schema
        schema, table_name = _select_table_and_get_schema(user_query, connection)
        
        # 2. Choose the best visualization type
        viz_info = choose_visualization(user_query, schema)
        
        # 3. Generate and execute the SQL to get the data
        data = generate_and_execute_sql(user_query, schema, viz_info, table_name, connection)

        # 4. NEW: Generate insights based on the returned data
        # Use a sample of the data (e.g., first 10 rows) to keep the prompt concise
        insights_text = generate_data_insights(user_query, viz_info, data[:10])
        viz_info['insights'] = insights_text

    # 5. Assemble the final response object
    final_output = {
        "visualization": viz_info,
        "data": data
    }
    
    return final_output