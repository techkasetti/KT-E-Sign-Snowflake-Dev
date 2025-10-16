def handler(session):
    # dummy check and emit
    session.sql("INSERT INTO DOCGEN.SLA_METRICS (METRIC_ID, METRIC_NAME, METRIC_VALUE) VALUES (UUID_STRING(),'latency',PARSE_JSON('{\"p95\":100}'))").collect()
    return {"checked": True}

