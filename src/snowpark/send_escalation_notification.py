def handler(session, esc_id, payload):
    session.sql("INSERT INTO DOCGEN.ESCALATION_NOTIFICATIONS (NOTIF_ID, ESC_ID, NOTIF_PAYLOAD) VALUES (UUID_STRING(), :1, PARSE_JSON(:2))").bind((esc_id, str(payload))).collect()
    return {"sent": True}

