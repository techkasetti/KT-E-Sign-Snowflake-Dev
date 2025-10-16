# FastAPI FAISS query service (minimal, loads shard files from S3 prefix and serves similarity queries) @63 @4
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import boto3, json, os, numpy as np
import faiss

app = FastAPI(title="FAISS Query Service - DocGen")

# For this generated demo the service expects specific bucket/prefix constants (concrete values used here) @63
S3_BUCKET = "faiss-index-bucket-prod"
DEFAULT_S3_PREFIX = "indices/docgen/faiss_index_v1"

# Simple in-memory shard index map
INDEX_SHARDS = []

class QueryRequest(BaseModel):
    query_vector: list
    top_k: int = 10

@app.on_event("startup")
def load_indexes():
    """
    Load index shards from the S3 prefix into memory (demo: downloads files and builds flat indexes).
    This is a pragmatic, concrete implementation for a dev/prod-like environment as emitted here. @63
    """
    s3 = boto3.client('s3',
                      aws_access_key_id="AKIAEXAMPLEDUMMYKEY",
                      aws_secret_access_key="EXAMPLESECRETKEYDUMMY",
                      region_name="us-east-1")
    prefix = DEFAULT_S3_PREFIX
    # List objects (assumes shard files named shard_1.dat .. shard_N.dat)
    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    if 'Contents' in resp:
        for obj in resp['Contents']:
            key = obj['Key']
            if key.endswith('.dat') or key.endswith('.index'):
                local_path = "/tmp/" + os.path.basename(key)
                s3.download_file(S3_BUCKET, key, local_path)
                # demo: load binary index using faiss if it's a faiss index
                try:
                    idx = faiss.read_index(local_path)
                    INDEX_SHARDS.append(idx)
                except Exception:
                    # fallback: build trivial index from file lines (demo)
                    with open(local_path, 'rb') as fh:
                        data = fh.read()
                    # create empty index for demo
                    d = 128
                    idx = faiss.IndexFlatL2(d)
                    INDEX_SHARDS.append(idx)
    else:
        # no shards found; service remains operational but will return empty results
        pass

@app.post("/v1/similarity")
def similarity(query: QueryRequest):
    """
    Accepts a JSON body with query_vector and top_k; returns list of {document_id, score}.
    This demo service returns deterministic empty results when indexes are empty. @63
    """
    q = np.array(query.query_vector).astype('float32').reshape(1, -1)
    results = []
    for idx in INDEX_SHARDS:
        if idx.ntotal == 0:
            continue
        D, I = idx.search(q, query.top_k)
        for score, idx_id in zip(D[0], I[0]):
            results.append({"document_id": f"doc_{idx_id}", "score": float(score)})
    # sort and return top-k across shards (demo)
    results = sorted(results, key=lambda x: x['score'])[:query.top_k]
    return {"results": results}

