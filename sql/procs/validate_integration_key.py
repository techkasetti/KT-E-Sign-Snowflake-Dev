# validate_integration_key.py
from snowflake.snowpark import Session
import hashlib, uuid
def validate_integration_key(session: Session, account_id: str, provided_key: str):
    hashed = hashlib.sha256(provided_key.encode()).hexdigest()
    rows = session.sql(f"SELECT ACCOUNT_ID FROM DOCGEN.ACCOUNTS WHERE ACCOUNT_ID = '{account_id}' AND INTEGRATION_KEY_HASH = '{hashed}'").collect()
    return {"valid": len(rows) > 0}

Show-once integration key validation with hash matching aligns to the show-once key pattern and key-hash storage guidance in your design docs @31 @21. @31 @21

