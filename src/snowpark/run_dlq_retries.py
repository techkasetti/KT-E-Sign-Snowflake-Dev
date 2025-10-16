def handler(session, batch_size):
    rows = session.sql(f"SELECT RETRY_ID, DLQ_ID FROM DOCGEN.DLQ_RETRY_QUEUE WHERE NEXT_ATTEMPT_AT <= CURRENT_TIMESTAMP() LIMIT {batch_size}").collect()
    return {"scheduled": len(rows)}

