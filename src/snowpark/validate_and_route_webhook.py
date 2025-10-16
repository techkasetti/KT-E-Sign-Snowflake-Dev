# Webhook validator skeleton: HMAC check, idempotency key extraction, call UPSERT_SIGNATURE_WEBHOOK. @31 @24 @52
from snowflake.snowpark import Session
def handler(session: Session, raw_payload: dict, signature_header: str):
    return {"status":"accepted"}

