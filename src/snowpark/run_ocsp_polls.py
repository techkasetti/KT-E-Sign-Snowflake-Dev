def handler(session):
    rows = session.sql("SELECT CERT_ID FROM DOCGEN.OCSP_POLL_JOBS").collect()
    return {"polled": len(rows)}

