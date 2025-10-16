# tests/test_faiss_query_service_local.py
# Simple pytest test that calls local FAISS FastAPI service (expects service running) @63
import requests
def test_faiss_query_local():
    url = "http://localhost:8000/v1/similarity"
    payload = {"query_vector": [0.1]*128, "top_k": 5}
    r = requests.post(url, json=payload, timeout=10)
    assert r.status_code == 200
    data = r.json()
    assert "results" in data

