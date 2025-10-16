def handler(session):
    rows = session.sql("SELECT EXPORT_ID FROM DOCGEN.COMPLIANCE_EXPORTS WHERE STATUS='PENDING'").collect()
    return {"exports_started": len(rows)}

