from snowflake.snowpark import Session
def reconcile_evidence_exports(session: Session, manifest_id):
    rows = session.sql(f"SELECT COUNT(*) AS cnt FROM DOCGEN.SIGNATURE_EVIDENCE_BUNDLE WHERE BUNDLE_ID IN (SELECT BUNDLE_ID FROM DOCGEN.EXPORT_MANIFEST WHERE MANIFEST_ID = '{manifest_id}')").collect()
    session.sql(f"INSERT INTO DOCGEN.EXPORT_RESULTS (RESULT_ID, MANIFEST_ID, ROW_COUNT, STATUS, COMPLETED_AT) VALUES (UUID_STRING(), '{manifest_id}', {rows[0]['CNT']}, 'OK', CURRENT_TIMESTAMP())").collect()
    return {"status":"ok"}
# Reconcile exported evidence counts for manifest reconciliation and audit @62 @103

