Purpose: Container for assembly service with wkhtmltopdf and AWS CLI, ready for deployment. @28 @70
# Dockerfile
FROM python:3.10-slim
RUN apt-get update && apt-get install -y wkhtmltopdf xvfb libfontconfig && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY app.py /app/app.py
RUN pip install fastapi uvicorn boto3 pydantic
EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]

