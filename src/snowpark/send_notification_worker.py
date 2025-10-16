# Snowpark Python notification worker skeleton to call External Function or middleware. @31 @24 @52
from snowflake.snowpark import Session
def handler(session: Session, notif_id: str):
    rows = session.sql(f"SELECT * FROM DOCGEN.SIGNATURE_NOTIFICATIONS WHERE NOTIF_ID = '{notif_id}'").collect()
    return {"status":"queued"}

