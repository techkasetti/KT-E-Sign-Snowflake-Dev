from snowflake.snowpark import Session
def process_raw_signature_events(session: Session, limit: int=100):
    rows = session.sql("SELECT RAW_ID, PAYLOAD FROM DOCGEN.SIGNATURE_EVENTS_RAW WHERE HANDLED = FALSE LIMIT {}".format(limit)).collect()
    for r in rows:
        payload = r['PAYLOAD']
        session.call('DOCGEN.UPSERT_SIGNATURE_WEBHOOK', payload)
        session.sql(f"UPDATE DOCGEN.SIGNATURE_EVENTS_RAW SET HANDLED = TRUE WHERE RAW_ID = '{r['RAW_ID']}'").collect()
    return {"processed": len(rows)}
# Processing loop moves staged raw events into idempotent MERGE SP @31 @130

