def handler(session): rows = session.sql("SELECT REMINDER_ID, REQUEST_ID, CHANNEL FROM DOCGEN.SCHEDULED_REMINDERS WHERE STATUS='PENDING' AND SCHEDULED_AT <= CURRENT_TIMESTAMP()").collect() return {"queued": len(rows)}

