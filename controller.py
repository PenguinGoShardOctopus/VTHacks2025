from google.genai import Client
from google.genai.chats import Chat
from google.genai.types import Part, GenerateContentConfig
from lib import list_tables, get_schema, parse_sql

class Controller():
    def __init__(self, client: Client, query: str):
        self.chat: Chat = client.chats.create(
            model='gemini-2.5-flash',
            config=GenerateContentConfig(
                tools=[
                    list_tables,
                    get_schema,
                    parse_sql,
                ]
            )
        )
        self.query: str = query

    def decision_loop(self):
        instruction: Part = Part.from_text(
            text="""The following part is the User Query based on which you will perform a series of actions as presented below"""
        )
        user_query: Part = Part.from_text(text=self.query)
        self.chat.send_message(
            [

            ]
        )
