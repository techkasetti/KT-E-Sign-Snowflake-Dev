def handler(session, window_hours):
    session.sql("INSERT INTO DOCGEN.ACCESS_LOG_AGGREGATES (AGG_ID, AGG_KEY, AGG_PAYLOAD) VALUES (UUID_STRING(), 'last_window', PARSE_JSON('{}'))").collect()
    return {"aggregated": True}

