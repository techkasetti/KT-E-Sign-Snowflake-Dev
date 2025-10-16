# faiss_similarity_wrapper.py
# Snowpark stored proc that calls DOCGEN.FAISS_SIMILARITY(external function) and writes retrieval results to a table @63 @4
from snowflake.snowpark import Session
import json, uuid

def faiss_similarity_wrapper(session: Session, query_vector: list, top_k: int = 10, request_id: str = None):
    """
    Call the External Function and record retrieval results into DOCGEN.RETRIEVAL_RESULTS (table created below).
    """
    payload = {"query_vector": query_vector, "top_k": top_k}
    # Call external function
    df = session.sql(f"SELECT DOCGEN.FAISS_SIMILARITY(PARSE_JSON('{json.dumps(payload)}')) AS res").collect()
    if not df:
        return {"status": "no_response"}
    res = df[0]['RES']
    results = res.get('results', []) if isinstance(res, dict) else []
    # Ensure table exists
    session.sql("""
        CREATE TABLE IF NOT EXISTS DOCGEN.RETRIEVAL_RESULTS (
            RETRIEVAL_ID STRING PRIMARY KEY,
            REQUEST_ID STRING,
            DOCUMENT_ID STRING,
            SCORE FLOAT,
            RETRIEVED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
        );
    """).collect()
    # Insert results
    for r in results:
        rid = "ret_" + uuid.uuid4().hex
        doc = r.get('document_id')
        score = r.get('score')
        session.sql(f"INSERT INTO DOCGEN.RETRIEVAL_RESULTS (RETRIEVAL_ID, REQUEST_ID, DOCUMENT_ID, SCORE) VALUES ('{rid}', '{request_id or ''}', '{doc}', {float(score)});").collect()
    return {"status": "recorded", "count": len(results)}

