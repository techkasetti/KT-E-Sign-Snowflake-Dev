# FastAPI similarity service (PoC)
from fastapi import FastAPI, HTTPException
import numpy as np, faiss

app = FastAPI()
INDEX_PATH = "/data/index.faiss"
index = None

@app.on_event("startup")
def load_index():
    global index
    index = faiss.read_index(INDEX_PATH)

@app.post("/similarity")
def similarity(query: dict):
    if 'vector' not in query:
        raise HTTPException(status_code=400, detail="vector required")
    qvec = np.array(query['vector'], dtype='float32').reshape(1, -1)
    D, I = index.search(qvec, k=min(10, index.ntotal))
    return {"distances": D.tolist(), "indices": I.tolist()}

