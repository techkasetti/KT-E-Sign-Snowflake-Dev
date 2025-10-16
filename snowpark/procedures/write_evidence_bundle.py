# WRITE_EVIDENCE_BUNDLE(bundle_meta VARIANT) - assembles CompliancePacket and writes manifest + bundle row
from snowflake.snowpark import Session
import uuid, json

def write_evidence_bundle(session: Session, bundle_meta):
    bundle_id = 'bnd_' + str(uuid.uuid4())
    session.sql(f"""
        INSERT INTO DOCGEN.SIGNATURE_EVIDENCE_BUNDLE (BUNDLE_ID, REQUEST_ID, DOCUMENT_ID, ACCOUNT_ID, BUNDLE_URL, MANIFEST, PROVENANCE_HASH, CREATED_AT)
        VALUES ('{bundle_id}', '{bundle_meta.get('request_id')}', '{bundle_meta.get('document_id')}', '{bundle_meta.get('account_id')}', '{bundle_meta.get('bundle_url')}', PARSE_JSON('{json.dumps(bundle_meta.get('manifest',{}))}'), '{bundle_meta.get('provenance_hash')}', CURRENT_TIMESTAMP());
    """).collect()
    return {"status":"written", "bundle_id": bundle_id}
# Evidence writer stores CompliancePacket manifest and metadata per evidence export patterns @2438 @2271

