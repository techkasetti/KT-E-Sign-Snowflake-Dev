# generate_integration_key.py
from snowflake.snowpark import Session
import os, secrets, base64, hmac, hashlib, json

def generate_integration_key(session: Session, account_id: str):
    """
    Generate a show-once integration key and persist salted KDF/hash in DOCGEN.ACCOUNTS.
    Returns the plaintext key for one-time display; the stored value is a JSON variant with kdf and salt.
    """
    key_plain = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode('utf-8')
    server_secret = os.getenv('INTEGRATION_KDF_SECRET', 'replace_with_ci_secret')
    salt = secrets.token_hex(16)
    kdf = hmac.new(server_secret.encode(), (salt + key_plain).encode(), hashlib.sha256).hexdigest()
    meta = json.dumps({"kdf": kdf, "salt": salt})
    session.sql(f"""
        MERGE INTO DOCGEN.ACCOUNTS t
        USING (SELECT '{account_id}' AS ACCOUNT_ID, PARSE_JSON('{meta}') AS INTEGRATION_KEY_HASH) s
        ON t.ACCOUNT_ID = s.ACCOUNT_ID
        WHEN MATCHED THEN UPDATE SET INTEGRATION_KEY_HASH = s.INTEGRATION_KEY_HASH, INTEGRATION_STATUS='ACTIVE'
        WHEN NOT MATCHED THEN INSERT (ACCOUNT_ID, ACCOUNT_NAME, INTEGRATION_KEY_HASH, INTEGRATION_STATUS)
          VALUES ('{account_id}','{account_id}', s.INTEGRATION_KEY_HASH, 'ACTIVE');
    """).collect()
    return key_plain

