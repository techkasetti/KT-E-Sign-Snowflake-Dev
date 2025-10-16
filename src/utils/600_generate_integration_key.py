# Snowpark-compatible example for show-once integration_key generator; stores only salted SHA-256 hash as recommended. @124 @16
import os, hashlib, secrets
def generate_key():
    key = secrets.token_urlsafe(32)
    salt = secrets.token_hex(16)
    hashed = hashlib.sha256((salt+key).encode()).hexdigest()
    return {"plain_key": key, "hash": hashed, "salt": salt}

