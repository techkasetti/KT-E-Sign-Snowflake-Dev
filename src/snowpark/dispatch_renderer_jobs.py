def handler(session, batch_size):
    rows = session.sql(f"SELECT QUEUE_ID, RENDERER_ID, JOB_PAYLOAD FROM DOCGEN.RENDERER_QUEUE LIMIT {batch_size}").collect()
    return {"dispatched": len(rows)}

