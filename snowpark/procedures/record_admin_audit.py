from snowflake.snowpark import Session
def record_admin_audit(session: Session, admin_user, action, target, details):
    session.sql(f"INSERT INTO DOCGEN.ADMIN_AUDIT (AUDIT_ID, ADMIN_USER, ACTION, TARGET, DETAILS, AUDIT_TS) VALUES (UUID_STRING(), '{admin_user}', '{action}', '{target}', PARSE_JSON('{json.dumps(details)}'), CURRENT_TIMESTAMP())").collect()
    return {"status":"ok"}
# Admin audit writer used by UI-driven admin operations and entitlement toggles @31 @116

