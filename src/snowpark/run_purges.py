def handler(session):
    rows = session.sql("SELECT SCHEDULE_ID, TARGET_TABLE, RETENTION_DAYS FROM DOCGEN.PURGE_SCHEDULES WHERE ENABLED = TRUE").collect()
    return {"schedules": len(rows)}

