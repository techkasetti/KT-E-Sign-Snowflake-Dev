-- Create session cookie record and return reference for web flows. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.ISSUE_SESSION_COOKIE(session_id STRING, cookie_payload VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.SIGNATURE_COOKIE_JOURNAL (COOKIE_ID, SESSION_ID, COOKIE_PAYLOAD) VALUES (UUID_STRING(), :session_id, :cookie_payload);
$$;

