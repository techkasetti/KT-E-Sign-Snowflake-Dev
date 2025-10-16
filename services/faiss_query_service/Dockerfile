# Dockerfile for FAISS query service emitted here as concrete artifact (uses prebuilt faiss cpu wheel) @63
FROM python:3.10-slim
RUN apt-get update && apt-get install -y build-essential libopenblas-dev wget && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY app.py /app/app.py
RUN pip install fastapi uvicorn boto3 "faiss-cpu==1.7.2"
EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]

