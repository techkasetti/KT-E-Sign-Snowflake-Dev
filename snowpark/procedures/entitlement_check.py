# entitlement_check.py
from snowflake.snowpark import Session

def entitlement_check(session: Session, account_id: str, feature_key: str):
    """Return whether the account has entitlement for a feature."""
    df = session.sql(f"""
        SELECT 1 AS allowed
        FROM DOCGEN.ACCOUNT_FEATURE_PRICING
        WHERE ACCOUNT_ID = '{account_id}' AND FEATURE_KEY = '{feature_key}'
        LIMIT 1
    """).collect()
    allowed = len(df) > 0
    return {"account_id": account_id, "feature_key": feature_key, "allowed": allowed}

