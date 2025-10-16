-- Append a session transition entry to history for auditing state changes. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.LOG_SESSION_TRANSITION(session_id STRING, old_status STRING, new_status STRING, meta VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.SIGNER_SESSION_HISTORY (HISTORY_ID, SESSION_ID, OLD_STATUS, NEW_STATUS, META) VALUES (UUID_STRING(), :session_id, :old_status, :new_status, :meta);
$$;

