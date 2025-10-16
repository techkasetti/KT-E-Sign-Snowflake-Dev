# usage_ingest_sp.py
from snowflake.snowpark import Session

def usage_ingest(session: Session, staged_path: str):
    session.sql(f"""
        COPY INTO DOCGEN.USAGE_EVENTS_STAGE
        FROM @AI_FEATURE_HUB.DOCGEN.USAGE_STAGE/{staged_path}
        FILE_FORMAT=(FORMAT_NAME='AI_FEATURE_HUB.DOCGEN.JSONL_FORMAT')
    """).collect()

    session.sql("""
        MERGE INTO DOCGEN.TENANT_FEATURE_USAGE tgt
        USING (SELECT EVENT_ID, ACCOUNT_ID, FEATURE_KEY, UNITS, RAW_EVENT FROM DOCGEN.USAGE_EVENTS_STAGE) src
        ON tgt.ACCOUNT_ID = src.ACCOUNT_ID AND tgt.FEATURE_KEY = src.FEATURE_KEY AND tgt.USAGE_DATE = DATE(src.RAW_EVENT:usage_date::STRING)
        WHEN NOT MATCHED THEN INSERT (ACCOUNT_ID, FEATURE_KEY, USAGE_DATE, USAGE_QTY, PROVENANCE, LAST_UPDATED)
          VALUES (src.ACCOUNT_ID, src.FEATURE_KEY, DATE(src.RAW_EVENT:usage_date::STRING), src.UNITS, PARSE_JSON(src.RAW_EVENT), CURRENT_TIMESTAMP());
    """).collect()

    session.sql("TRUNCATE TABLE IF EXISTS DOCGEN.USAGE_EVENTS_STAGE").collect()
    return {"status": "ingested"}

