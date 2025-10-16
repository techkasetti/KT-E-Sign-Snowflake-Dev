# Assembly service Dockerfile: builds a small FastAPI service that accepts assembly jobs, writes evidence to S3, and calls WRITE_EVIDENCE_BUNDLE via Snowflake connector. This pattern matches the assembly/evidence uploader designs in your repo @186 @65.
FROM python:3.10-slim
WORKDIR /app
COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ./app ./app
EXPOSE 4000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "4000"]

