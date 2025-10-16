from snowflake.snowpark import Session
def record_object_lock(session: Session, object_ref, locked_by, expires_at):
    session.sql(f"INSERT INTO DOCGEN.OBJECT_LOCKS (LOCK_ID, OBJECT_REF, LOCKED_BY, LOCKED_AT, EXPIRES_AT) VALUES (UUID_STRING(), '{object_ref}', '{locked_by}', CURRENT_TIMESTAMP(), '{expires_at}')").collect()
    return {"status":"locked"}
# Lock writer supports optimistic locking for concurrent assembly flows per design patterns @233 @181

