def handler(session):
    session.sql("INSERT INTO DOCGEN.DOCUMENT_ACTIVITY_AGG (AGG_ID, DOCUMENT_ID, AGG_PAYLOAD) VALUES (UUID_STRING(), 'doc_agg', PARSE_JSON('{}'))").collect()
    return {"aggregated": True}

