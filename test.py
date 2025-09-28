from dotenv import load_dotenv
from google.genai import Client
from google.genai.types import GenerateContentConfig, AutomaticFunctionCallingConfig
from lib import list_tables, get_schema, parse_sql

load_dotenv()

client = Client()
chat = client.chats.create(
    model='gemini-2.5-flash',
    config=GenerateContentConfig(
        tools=[
            list_tables,
            get_schema,
            parse_sql,
        ],
        # automatic_function_calling=AutomaticFunctionCallingConfig(
        #     disable=True
        # ),
    )
)

response = chat.send_message("call list_tables, and then call get_schema for any of the returned tables and print the schema alongside table name")

print(response)