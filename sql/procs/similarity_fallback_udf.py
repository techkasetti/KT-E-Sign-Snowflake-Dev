# similarity_fallback_udf.py
from snowflake.snowpark import Session
def similarity_fallback(session: Session, embedding ARRAY, top_k INT):
    # Attempt native VECTOR UDF first (if available), otherwise call FAISS External Function
    try:
        res = session.sql(f"SELECT DOCGEN.NATIVE_SIMILARITY(PARSE_JSON('{{\"embedding\":{embedding},\"top_k\":{top_k}}}')) AS resp").collect()
        return res[0]['RESP']
    except Exception:
        res = session.sql(f"SELECT DOCGEN.SIMILARITY_QUERY(PARSE_JSON('{{\"query_embedding\":{embedding},\"top_k\":{top_k}}}')) AS resp").collect()
        return res[0]['RESP']

This stored-proc implements a VECTOR fallback → External Function FAISS fallback pattern per your retrieval design recommendations @153 @29. @153 @29

