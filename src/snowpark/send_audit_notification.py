def handler(session, audit_id, channel, payload):
    session.sql("INSERT INTO DOCGEN.AUDIT_NOTIFICATIONS (NOTIF_ID, AUDIT_ID, CHANNEL, PAYLOAD) VALUES (UUID_STRING(), :1, :2, PARSE_JSON(:3))").bind((audit_id, channel, str(payload))).collect()
    return {"notified": True}

