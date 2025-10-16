# Call DOCGEN.PROVIDER_HEALTH_CHECK EF for each provider and update status table @1 @6
from snowflake.snowpark import Session
def handler(session: Session):
    return {"checked":0}

