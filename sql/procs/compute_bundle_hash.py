# compute_bundle_hash.py
# Snowpark Python helper to compute deterministic bundle hash from manifest JSON
from snowflake.snowpark import Session
import hashlib, json
def compute_bundle_hash(session: Session, manifest_variant):
    """Canonicalize manifest_variant to stable JSON and return SHA256 hex digest."""
    # Convert VARIANT to string and canonicalize keys sorted
    manifest = json.loads(str(manifest_variant))
    stable = json.dumps(manifest, separators=(',', ':'), sort_keys=True, ensure_ascii=False)
    h = hashlib.sha256(stable.encode('utf-8')).hexdigest()
    return {"hash": h}

