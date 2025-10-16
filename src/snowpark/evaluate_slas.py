def handler(session):
    session.sql("INSERT INTO DOCGEN.SLA_BREAKDOWN (SLA_ID, NAME, TARGETS) VALUES (UUID_STRING(), 'placeholder', PARSE_JSON('{}'))").collect()
    return {"evaluated": True}

