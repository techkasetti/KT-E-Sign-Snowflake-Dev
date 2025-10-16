def handler(session, batch_size):
    rows = session.sql(f"SELECT QUEUE_ID, TARGET_PROC, PAYLOAD, ATTEMPTS FROM DOCGEN.RETRY_QUEUE WHERE NEXT_ATTEMPT_AT <= CURRENT_TIMESTAMP() ORDER BY NEXT_ATTEMPT_AT LIMIT {batch_size}").collect()
    return {"dispatched": len(rows)}

