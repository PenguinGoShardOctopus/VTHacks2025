from google import genai
from dotenv import load_dotenv

# Load .env file so GEMINI_API_KEY becomes an environment variable
load_dotenv()

# Option 1: Let Client() pick it up automatically
client = genai.Client()
user_prompt = input("Enter your prompt: ")

response = client.models.generate_content(
    model="gemini-2.5-flash",
    contents=user_prompt
)

print(response.text)
