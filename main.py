import os

from fastapi import FastAPI

app = FastAPI()

@app.get("/upload")
def upload():
	val = os.getenv('DATABRICKS_API_KEY')

	return {"message": val}
