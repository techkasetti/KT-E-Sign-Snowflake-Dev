Purpose: perform retention purge with audit trail writes to DOCGEN.PURGE_AUDIT, following retention runbook and dry-run report guidance; this proc is idempotent and writes purge audit records for compliance. @118 @214

# purge_evidence_with_audit.py
from snowflake.snowpark import Session
import uuid
def purge_evidence_with_audit(session: Session, cutoff_days: int = 1095, dry_run: bool = True):
    cutoff_expr = f"DATEADD('day', -{cutoff_days}, CURRENT_TIMESTAMP())"
    rows = session.sql(f"SELECT BUNDLE_ID, ARCHIVE_LOCATION FROM DOCGEN.EVIDENCE_BUNDLE WHERE CREATED_AT <= {cutoff_expr} AND COALESCE(METADATA:legal_hold::BOOLEAN, FALSE) = FALSE").collect()
    deleted = 0
    for r in rows:
        bundle_id = r['BUNDLE_ID']
        if not dry_run:
            session.sql(f"DELETE FROM DOCGEN.EVIDENCE_BUNDLE WHERE BUNDLE_ID = '{bundle_id}';").collect()
            deleted += 1
    audit_id = 'purge_' + uuid.uuid4().hex
    session.sql(f"INSERT INTO DOCGEN.PURGE_AUDIT (PURGE_ID, ENTITY_NAME, ROWS_PURGED, CUT_OFF, EXECUTED_AT) VALUES ('{audit_id}', 'EVIDENCE_BUNDLE', {deleted if not dry_run else 0}, {cutoff_expr}, CURRENT_TIMESTAMP());").collect()
    return {"audit_id": audit_id, "rows_found": len(rows), "rows_deleted": deleted if not dry_run else 0}

----
