FROM python:3.10-slim
RUN pip install faiss-cpu fastapi uvicorn boto3
COPY index_snapshot_loader.py /app/
CMD ["uvicorn","index_snapshot_loader:app","--host","0.0.0.0","--port","8080"]
# Container skeleton for FAISS builder/query service per FAISS container recommendations @31 @316

