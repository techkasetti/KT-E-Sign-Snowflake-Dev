# enforce_retention_policy.py
# Snowpark Python proc to apply retention rules and legal-hold exemptions before purge/archive
from snowflake.snowpark import Session
import uuid
def enforce_retention_policy(session: Session, retention_days: int = 1095):
    """
    - Identify bundles older than retention_days that are not under legal hold.
    - Move them to backup clone and delete originals.
    - Record entries in PURGE_AUDIT.
    """
    cutoff = f"DATEADD('day', -{int(retention_days)}, CURRENT_TIMESTAMP())"
    # Exclude bundles with metadata.legal_hold = true
    select_sql = f"SELECT BUNDLE_ID FROM DOCGEN.EVIDENCE_BUNDLE WHERE CREATED_AT <= {cutoff} AND COALESCE(METADATA:legal_hold::BOOLEAN, FALSE) = FALSE"
    rows = session.sql(select_sql).collect()
    to_purge = [r['BUNDLE_ID'] for r in rows]
    if not to_purge:
        return {"purged": 0}
    # Copy to backup clone and delete
    for b in to_purge:
        session.sql(f"INSERT INTO DOCGEN.BACKUP_DOCUMENT_ARCHIVE_CLONE SELECT * FROM DOCGEN.EVIDENCE_BUNDLE WHERE BUNDLE_ID = '{b}';").collect()
        session.sql(f"DELETE FROM DOCGEN.EVIDENCE_BUNDLE WHERE BUNDLE_ID = '{b}';").collect()
    session.sql(f"INSERT INTO DOCGEN.PURGE_AUDIT (PURGE_ID, ENTITY_NAME, ROWS_PURGED, CUT_OFF, EXECUTED_AT) VALUES ('purge_{uuid.uuid4().hex}','EVIDENCE_BUNDLE',{len(to_purge)},{cutoff},CURRENT_TIMESTAMP());").collect()
    return {"purged": len(to_purge)}

