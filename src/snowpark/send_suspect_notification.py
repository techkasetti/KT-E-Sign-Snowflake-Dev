def handler(session, suspect_id, payload):
    session.sql(f"INSERT INTO DOCGEN.SUSPECT_NOTIFICATIONS (NOTIF_ID, SUSPECT_ID, NOTIF_PAYLOAD) VALUES (UUID_STRING(), '{suspect_id}', PARSE_JSON('{payload}'))").collect()
    return {"notified": True}

