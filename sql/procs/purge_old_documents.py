# purge_old_documents.py
# Snowpark Python stored-proc to purge old DOCUMENT_ARCHIVE rows and record purge audit entries.
from snowflake.snowpark import Session
import uuid, datetime

def purge_old_documents(session: Session, retention_days: int = 365, dry_run: bool = True):
    """
    Purge DOCUMENT_ARCHIVE rows older than retention_days; if dry_run is True, report counts without deleting.
    Returns summary dict with rows_affected and purge_id.
    """
    cutoff_expr = f"DATEADD('day', -{int(retention_days)}, CURRENT_TIMESTAMP())"
    count_q = session.sql(f"SELECT COUNT(*) AS cnt FROM DOCGEN.DOCUMENT_ARCHIVE WHERE CREATED_AT <= {cutoff_expr}").collect()
    rows_to_purge = int(count_q[0]['CNT'])
    purge_id = "purge_" + uuid.uuid4().hex
    if dry_run:
        # Record an audit row with zero effect in dry-run mode
        session.sql(f"""
            INSERT INTO DOCGEN.PURGE_AUDIT (PURGE_ID, ENTITY_NAME, ROWS_PURGED, CUT_OFF, EXECUTED_AT)
            VALUES ('{purge_id}', 'DOCUMENT_ARCHIVE', 0, {cutoff_expr}, CURRENT_TIMESTAMP());
        """).collect()
        return {"purge_id": purge_id, "dry_run": True, "rows_to_purge": rows_to_purge}
    # Actual delete: move to backup clone then delete
    session.sql(f"""
        INSERT INTO DOCGEN.BACKUP_DOCUMENT_ARCHIVE_CLONE SELECT * FROM DOCGEN.DOCUMENT_ARCHIVE WHERE CREATED_AT <= {cutoff_expr};
    """).collect()
    session.sql(f"DELETE FROM DOCGEN.DOCUMENT_ARCHIVE WHERE CREATED_AT <= {cutoff_expr};").collect()
    session.sql(f"""
        INSERT INTO DOCGEN.PURGE_AUDIT (PURGE_ID, ENTITY_NAME, ROWS_PURGED, CUT_OFF, EXECUTED_AT)
        VALUES ('{purge_id}', 'DOCUMENT_ARCHIVE', {rows_to_purge}, {cutoff_expr}, CURRENT_TIMESTAMP());
    """).collect()
    return {"purge_id": purge_id, "dry_run": False, "rows_purged": rows_to_purge}

This purge procedure performs a safe backup-then-delete flow and writes a PURGE_AUDIT row for traceability; it supports dry-run for verification prior to production deletion @67 @101.

