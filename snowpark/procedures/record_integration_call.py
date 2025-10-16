from snowflake.snowpark import Session
def record_integration_call(session: Session, target, payload, response, status):
    session.sql(f"INSERT INTO DOCGEN.INTEGRATION_CALLS (CALL_ID, TARGET, PAYLOAD, RESPONSE, STATUS, CALLED_AT) VALUES (UUID_STRING(), '{target}', PARSE_JSON('{json.dumps(payload)}'), PARSE_JSON('{json.dumps(response)}'), '{status}', CURRENT_TIMESTAMP())").collect()
    return {"status":"recorded"}
# Integration call recorder for External Function invocations and audits @217 @236

