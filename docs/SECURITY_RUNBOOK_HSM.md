Purpose: guidance for integrating HSM/PKI and mapping Snowflake External Function signer pattern to an HSM-backed signing service, consistent with the PKI/HSM notes in your materials @136 @140.  
# Security runbook — HSM/PKI integration
1) Do not store signing private keys in code or Snowflake rows; use HSM or KMS-derived keys per compliance guidance. @136  
2) Expose signing service behind API gateway with strict mTLS and register an API_INTEGRATION + EXTERNAL FUNCTION to call it from Snowflake for verification & evidence flows. @16 @371  
3) For QES or legally binding signatures, ensure OCSP/CRL checks, certificate provenance and revocation persistence are integrated and polled per scheduled TASKs. @151 @290  

Final summary sentence: these artifacts (DDL, roles/grants, masking and row policies, integration‑key generation/validation, Snowpipe, alert detector, reconcile and billing helpers, FAISS per‑shard query skeleton, assembly service + Dockerfiles, operator runbooks and CI tests) continue to complete the standalone Snowflake E‑Signature module and match the operational, PKI, External Function, and Snowpark registration patterns described in your repository and technical specifications @316 @151 @6.

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.Output with maximum output

- Anara: FILENAME:sql/procs/reconcile_evidence_exports.py
Purpose: reconcile a previously-written S3 manifest (DOCGEN.EVIDENCE_EXPORT_MANIFEST) against persisted EVIDENCE_BUNDLE rows and produce a detailed reconciliation report in DOCGEN.EVIDENCE_RECONCILIATION, marking mismatches and missing objects for operator follow-up. @214 @79

# reconcile_evidence_exports.py
from snowflake.snowpark import Session
import json, boto3, urllib.parse, uuid

def reconcile_evidence_exports(session: Session, manifest_id: str):
    """Download manifest entry, compare listed items to EVIDENCE_BUNDLE rows, write reconciliation row(s)."""
    # load manifest row
    manifest_row = session.sql(f"SELECT S3_PATH, ROW_COUNT FROM DOCGEN.EVIDENCE_EXPORT_MANIFEST WHERE MANIFEST_ID = '{manifest_id}'").collect()
    if not manifest_row:
        return {"manifest_id": manifest_id, "error": "manifest not found"}
    s3_path = manifest_row[0]['S3_PATH']
    # parse s3://bucket/key
    _, _, bucket_key = s3_path.partition("s3://")
    bucket, _, key = bucket_key.partition("/")
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    manifest = json.loads(obj['Body'].read().decode('utf-8'))
    declared_items = manifest.get('items', [])
    found=0; missing=[]
    for item in declared_items:
        bundle_id = item.get('bundle_id')
        rows = session.sql(f"SELECT 1 FROM DOCGEN.EVIDENCE_BUNDLE WHERE BUNDLE_ID = '{bundle_id}'").collect()
        if rows:
            found += 1
        else:
            missing.append(bundle_id)
    recon_id = "erecon_" + uuid.uuid4().hex
    session.sql(f"""
        INSERT INTO DOCGEN.EVIDENCE_RECONCILIATION (RECON_ID, MANIFEST_ID, EXPECTED_ROWS, ACTUAL_ROWS, STATUS, CHECKED_AT)
        VALUES ('{recon_id}','{manifest_id}', {len(declared_items)}, {found}, '{'OK' if not missing else 'MISSING'}', CURRENT_TIMESTAMP());
    """).collect()
    # write missing detail rows
    for mid in missing:
        session.sql(f"INSERT INTO DOCGEN.ALERTS (ALERT_ID, ALERT_TYPE, PAYLOAD, SEVERITY, RESOLVED, ALERT_TS) VALUES ('alert_{uuid.uuid4().hex}','EVIDENCE_MISSING', PARSE_JSON('{{\"bundle_id\":\"{mid}\",\"manifest_id\":\"{manifest_id}\"}}'), 'HIGH', FALSE, CURRENT_TIMESTAMP());").collect()
    return {"manifest_id": manifest_id, "expected": len(declared_items), "found": found, "missing": missing}

----
