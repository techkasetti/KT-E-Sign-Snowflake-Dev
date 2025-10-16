def handler(session):
    # placeholder: iterate providers, call external health endpoints via External Function
    session.sql("INSERT INTO DOCGEN.BATCH_PROVIDER_HEALTH_CHECKS (BPH_ID, PAYLOAD) VALUES (UUID_STRING(), PARSE_JSON('{}'))").collect()
    return {"done": True}

