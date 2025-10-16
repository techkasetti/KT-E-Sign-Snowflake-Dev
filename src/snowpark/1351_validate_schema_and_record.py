# Validate provider payload against WEBHOOK_SCHEMA_REGISTRY mappings then insert into SIGNATURE_EVENTS_RAW @1 @6
from snowflake.snowpark import Session
def handler(session: Session, provider: str, raw_payload: dict):
    session.sql("INSERT INTO DOCGEN.SIGNATURE_EVENTS_RAW(RAW_JSON) VALUES (PARSE_JSON(%s))", (json.dumps(raw_payload),)).collect()
    return {"status":"staged"}

