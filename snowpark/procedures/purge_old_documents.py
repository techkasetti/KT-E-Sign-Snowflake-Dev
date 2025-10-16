from snowflake.snowpark import Session
def purge_old_documents(session: Session, days:int, dry_run:bool):
    cutoff = session.sql(f"SELECT DATEADD('day', -{days}, CURRENT_TIMESTAMP())").collect()[0][0]
    if dry_run:
        rows = session.sql(f"SELECT COUNT(*) FROM DOCGEN.RENDERED_DOCUMENTS WHERE CREATED_AT < '{cutoff}'").collect()
        return {"to_delete": rows[0][0]}
    else:
        session.sql(f"DELETE FROM DOCGEN.RENDERED_DOCUMENTS WHERE CREATED_AT < '{cutoff}'").collect()
        return {"status":"deleted"}
# Purge procedure enforces retention rules per runbook patterns @176 @62

