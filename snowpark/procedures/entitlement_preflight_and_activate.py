Purpose: Snowpark stored procedure that validates integration key and entitlements, writes ActivationAudit, and toggles feature enablement for the account. @74 @189

# entitlement_preflight_and_activate.py
from snowflake.snowpark import Session
import json, hashlib, uuid

def entitlement_preflight_and_activate(session: Session, account_id: str, integration_key_plain: str, feature_key: str, admin_user: str):
    """
    Validate integration key against stored KDF/hash and enable feature if entitled.
    Writes ActivationAudit entries and returns result.
    """
    # Fetch stored kdf+salt
    rows = session.sql(f"SELECT INTEGRATION_KEY_HASH FROM DOCGEN.ACCOUNTS WHERE ACCOUNT_ID = '{account_id}' LIMIT 1").collect()
    if not rows:
        session.call("DOCGEN.WRITE_ACTIVATION_AUDIT", [account_id, "PRE_FLIGHT", admin_user, {"reason":"account_missing"}])
        return {"allowed": False, "reason": "account_not_found"}

    stored = rows[0]['INTEGRATION_KEY_HASH']
    # stored is a VARIANT like {"kdf":"...","salt":"..."}; emulating server KDF check (demo deterministic)
    server_secret = "integration_kdf_demo_secret"
    salt = stored.get('salt') if isinstance(stored, dict) else stored['salt']
    computed_kdf = hashlib.sha256((salt + integration_key_plain + server_secret).encode()).hexdigest()
    if computed_kdf != (stored.get('kdf') if isinstance(stored, dict) else stored['kdf']):
        # Write activation audit - failed validation
        session.sql(f"""
            INSERT INTO DOCGEN.ACTIVATION_AUDIT (AUDIT_ID, ACCOUNT_ID, ACTION_TYPE, ACTION_BY, ACTION_PAYLOAD, RESULT_STATUS, NOTES, ACTION_TS)
            VALUES ('audit_{uuid.uuid4()}', '{account_id}', 'INTEGRATION_KEY_VALIDATION', '{admin_user}', PARSE_JSON('{"' + "result" + '": "invalid"}'), 'FAILED', 'Integration key KDF mismatch', CURRENT_TIMESTAMP());
        """).collect()
        return {"allowed": False, "reason": "invalid_integration_key"}

    # Check feature pricing/entitlement
    ent = session.sql(f"SELECT 1 FROM DOCGEN.ACCOUNT_FEATURE_PRICING WHERE ACCOUNT_ID = '{account_id}' AND FEATURE_KEY = '{feature_key}' LIMIT 1").collect()
    if not ent:
        # record audit
        session.sql(f"""
            INSERT INTO DOCGEN.ACTIVATION_AUDIT (AUDIT_ID, ACCOUNT_ID, ACTION_TYPE, ACTION_BY, ACTION_PAYLOAD, RESULT_STATUS, NOTES, ACTION_TS)
            VALUES ('audit_{uuid.uuid4()}', '{account_id}', 'FEATURE_ENABLE', '{admin_user}', PARSE_JSON('{{"feature":"{feature_key}"}}'), 'FAILED', 'Not entitled', CURRENT_TIMESTAMP());
        """).collect()
        return {"allowed": False, "reason": "not_entitled"}

    # Enable feature: upsert into ACCOUNT_FEATURE_PRICING or set flag (demo: insert activation audit success)
    session.sql(f"""
        INSERT INTO DOCGEN.ACTIVATION_AUDIT (AUDIT_ID, ACCOUNT_ID, ACTION_TYPE, ACTION_BY, ACTION_PAYLOAD, RESULT_STATUS, NOTES, ACTION_TS)
        VALUES ('audit_{uuid.uuid4()}', '{account_id}', 'FEATURE_ENABLE', '{admin_user}', PARSE_JSON('{{"feature":"{feature_key}"}}'), 'SUCCESS', 'Feature enabled via preflight', CURRENT_TIMESTAMP());
    """).collect()
    return {"allowed": True, "status": "feature_enabled"}

