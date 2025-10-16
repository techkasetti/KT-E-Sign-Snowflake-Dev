def handler(session, provider_id, alert_body):
    session.sql(f"INSERT INTO DOCGEN.PROVIDER_SLACK_ALERTS (ALERT_ID, PROVIDER_ID, ALERT_BODY) VALUES (UUID_STRING(), '{provider_id}', PARSE_JSON('{alert_body}'))").collect()
    return {"ok": True}

