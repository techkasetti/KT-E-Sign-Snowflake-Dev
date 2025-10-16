def handler(session, batch_size):
    rows = session.sql(f"SELECT NOTIFICATION_ID, CHANNEL, PAYLOAD FROM DOCGEN.SIGNATURE_NOTIFICATIONS WHERE STATUS='PENDING' LIMIT {batch_size}").collect()
    for r in rows:
        session.sql(f"UPDATE DOCGEN.SIGNATURE_NOTIFICATIONS SET STATUS='SENT', SENT_AT=CURRENT_TIMESTAMP() WHERE NOTIFICATION_ID='{r[0]}'").collect()
    return {"dispatched": len(rows)}

