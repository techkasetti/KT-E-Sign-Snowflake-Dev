Purpose: export a set of EVIDENCE_BUNDLE rows into a JSON manifest in S3 and write a DOCGEN.EVIDENCE_EXPORT_MANIFEST row for operator reconciliation; implements the evidence export/manifest pattern described in the runbooks. @62 @103
# export_evidence_to_s3.py
from snowflake.snowpark import Session
import json, boto3, uuid, datetime, os
def export_evidence_to_s3(session: Session, s3_bucket: str, s3_prefix: str, since_days:int = 7):
    """
    Query evidence bundles since `since_days` days ago, write a manifest JSON to S3,
    and insert a DOCGEN.EVIDENCE_EXPORT_MANIFEST row for reconciliation.
    """
    cutoff = f"DATEADD('day', -{since_days}, CURRENT_TIMESTAMP())"
    rows = session.sql(f"SELECT BUNDLE_ID, DOCUMENT_ID, ACCOUNT_ID, ARCHIVE_LOCATION, METADATA FROM DOCGEN.EVIDENCE_BUNDLE WHERE CREATED_AT >= {cutoff}").collect()
    items = []
    for r in rows:
        items.append({
            "bundle_id": r["BUNDLE_ID"],
            "document_id": r["DOCUMENT_ID"],
            "account_id": r["ACCOUNT_ID"],
            "archive_location": r["ARCHIVE_LOCATION"],
            "metadata": r["METADATA"]
        })
    manifest = {
        "manifest_id": "m_" + uuid.uuid4().hex,
        "generated_at": datetime.datetime.utcnow().isoformat(),
        "item_count": len(items),
        "items": items
    }
    s3 = boto3.client("s3")
    key = f"{s3_prefix.rstrip('/')}/evidence_manifest_{manifest['manifest_id']}.json"
    s3.put_object(Bucket=s3_bucket, Key=key, Body=json.dumps(manifest).encode('utf-8'))
    # persist manifest row
    session.sql(f"""
        INSERT INTO DOCGEN.EVIDENCE_EXPORT_MANIFEST (MANIFEST_ID, S3_PATH, ROW_COUNT, EXPORT_TS)
        VALUES ('{manifest['manifest_id']}', 's3://{s3_bucket}/{key}', {len(items)}, CURRENT_TIMESTAMP());
    """).collect()
    return {"manifest_id": manifest["manifest_id"], "s3_path": f"s3://{s3_bucket}/{key}", "count": len(items)}

