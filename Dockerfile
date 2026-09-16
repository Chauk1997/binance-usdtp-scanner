FROM python:3.12-slim
WORKDIR /app
COPY scanner-v54-mcp/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY scanner-v54-mcp/server.py .
COPY scanner-v54-mcp/schemas ./schemas
CMD ["python", "server.py"]
