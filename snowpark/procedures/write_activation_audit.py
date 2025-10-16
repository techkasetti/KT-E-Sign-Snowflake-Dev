Purpose: Snowpark stored procedure to write ActivationAudit entries atomically for preflight and admin activation flows. @74 @189

# write_activation_audit.py
from snowflake.snowpark import Session
import uuid, json

def write_activation_audit(session: Session, account_id: str, action_type: str, action_by: str, payload_variant):
    """
    Insert an activation audit record to DOCGEN.ACTIVATION_AUDIT.
    """
    audit_id = "audit_" + str(uuid.uuid4())
    payload_json = json.dumps(payload_variant) if payload_variant is not None else "{}"
    session.sql(f"""
        INSERT INTO DOCGEN.ACTIVATION_AUDIT (AUDIT_ID, ACCOUNT_ID, ACTION_TYPE, ACTION_BY, ACTION_PAYLOAD, RESULT_STATUS, NOTES, ACTION_TS)
        VALUES ('{audit_id}', '{account_id}', '{action_type}', '{action_by}', PARSE_JSON('{payload_json}'), 'PENDING', '', CURRENT_TIMESTAMP());
    """).collect()
    return {"audit_id": audit_id, "status": "inserted"}

