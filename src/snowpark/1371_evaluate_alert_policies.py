# Evaluate configured alert policies and call DOCGEN.EMIT_ALERT when conditions match @1 @6
from snowflake.snowpark import Session
def handler(session: Session):
    return {"alerts_emitted":0}

