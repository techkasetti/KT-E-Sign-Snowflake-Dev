# FAISS index builder image for building sharded indexes from snapshot exports per FAISS snapshot/container recommendations in your design materials @22 @23.
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "index_builder.py"]

