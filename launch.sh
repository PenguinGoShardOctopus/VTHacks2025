nginx -c /app/nginx.conf
uvicorn main:app --reload --port 4000