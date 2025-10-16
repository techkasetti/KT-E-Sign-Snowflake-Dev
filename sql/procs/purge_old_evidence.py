# purge_old_evidence.py
from snowflake.snowpark import Session
def purge_old_evidence(session: Session, older_than_days: int):
    session.sql(f"DELETE FROM DOCGEN.EVIDENCE_BUNDLE WHERE CREATED_AT < DATEADD('day', -{older_than_days}, CURRENT_TIMESTAMP());").collect()
    return {"purged_before_days": older_than_days}

Retention purge procedures implement retention policies and retention task automation described in your compliance/runbook materials @75 @111. @75 @111

