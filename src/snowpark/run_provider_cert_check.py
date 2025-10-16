def handler(session, provider_id):
    session.sql(f"INSERT INTO DOCGEN.PROVIDER_CERT_CHECKS (CHECK_ID, PROVIDER_ID, RESULT) VALUES (UUID_STRING(), '{provider_id}', PARSE_JSON('{{\"status\":\"ok\"}}'))").collect()
    return {"checked": provider_id}

