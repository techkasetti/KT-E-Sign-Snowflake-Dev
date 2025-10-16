Purpose: assemble CompliancePacket metadata (manifest + cert chain + signed document link), persist EVIDENCE_BUNDLE, and return bundle_id. This Snowpark Python proc implements the WRITE_EVIDENCE_BUNDLE pattern described in your evidence design docs @65 @186.
# write_evidence_bundle.py
from snowflake.snowpark import Session
import json, uuid, datetime
def write_evidence_bundle(session: Session, assembly_run_id: str, document_id: str, signer_id: str, signature_hash: str, cert_chain: list, archive_location: str, metadata: dict = None):
    """Assemble and persist evidence bundle metadata into DOCGEN.EVIDENCE_BUNDLE and return bundle_id."""
    bundle_id = "bundle_" + uuid.uuid4().hex
    manifest = {
        "bundle_id": bundle_id,
        "assembly_run_id": assembly_run_id,
        "document_id": document_id,
        "signer_id": signer_id,
        "signature_hash": signature_hash,
        "cert_chain": cert_chain,
        "archive_location": archive_location,
        "metadata": metadata or {},
        "created_at": datetime.datetime.utcnow().isoformat()
    }
    session.sql(f"""
        INSERT INTO DOCGEN.EVIDENCE_BUNDLE (BUNDLE_ID, DOCUMENT_ID, ACCOUNT_ID, ARCHIVE_LOCATION, BUNDLE_HASH, METADATA, CREATED_AT)
        SELECT '{bundle_id}', '{document_id}', COALESCE((SELECT ACCOUNT_ID FROM DOCGEN.SIGNATURE_REQUESTS WHERE REQUEST_ID = '{assembly_run_id}'), 'unknown'),
               '{archive_location}', '{signature_hash}', PARSE_JSON('{json.dumps(manifest)}'), CURRENT_TIMESTAMP();
    """).collect()
    return {"bundle_id": bundle_id, "manifest": manifest}

