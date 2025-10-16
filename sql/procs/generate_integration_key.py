Purpose: generate a show‑once integration key, persist only salted hash in INTEGRATION_KEYS, and return plaintext to caller once — follows the show‑once integration key design in your admin spec @131 @396.  
# generate_integration_key.py
from snowflake.snowpark import Session
import hashlib, os, uuid, datetime, hmac, base64
def generate_integration_key(session: Session, account_id: str): """Generate a random key, store only a salted hash, return the plaintext_key once for admin display.""" salt = uuid.uuid4().hex
plaintext = base64.urlsafe_b64encode(os.urandom(32)).decode('utf-8')
kdf_input = (plaintext + salt).encode('utf-8')
key_hash = hashlib.sha256(kdf_input).hexdigest()
key_id = "ikey_" + uuid.uuid4().hex
session.sql(f""" INSERT INTO DOCGEN.INTEGRATION_KEYS (KEY_ID, ACCOUNT_ID, KEY_HASH, CREATED_AT) VALUES ('{key_id}', '{account_id}', '{key_hash}', CURRENT_TIMESTAMP()); """).collect()
# Return plaintext to caller (show-once)
return {"key_id": key_id, "integration_key": plaintext, "salt": salt}  

