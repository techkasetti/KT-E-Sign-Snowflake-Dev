def handler(session, alert_type, alert_details):
    rows = session.sql(f"SELECT SUBSCRIBER, CHANNEL FROM DOCGEN.ALERT_SUBSCRIPTIONS WHERE ALERT_TYPE = '{alert_type}'").collect()
    return {"notified": len(rows)}

