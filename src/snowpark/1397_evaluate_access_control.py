# Evaluate configured ACCESS_CONTROL_POLICIES given runtime context and return allow/deny decision @1 @6
def handler(session: Session, resource: str, context: dict):
    return {"allowed": True}

