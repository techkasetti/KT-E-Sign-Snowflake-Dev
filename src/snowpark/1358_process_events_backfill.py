# Reprocess backfill queue items to replay ingestion logic, rebuild provenance, and update indices @1 @6
from snowflake.snowpark import Session
def handler(session: Session):
    return {"processed":0}

