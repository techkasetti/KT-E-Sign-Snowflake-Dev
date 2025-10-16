# Skeleton for delivering callbacks with retry handling. @31 @24 @52
from snowflake.snowpark import Session
def handler(session: Session):
    return {"delivered":0}

