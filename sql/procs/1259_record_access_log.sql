-- Helper to insert access log rows from API or procedures. @31 @24 @52
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_ACCESS_LOG(subject STRING, action STRING, resource STRING, source_ip STRING, user_agent STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$ INSERT INTO DOCGEN.SIGNATURE_ACCESS_LOGS (LOG_ID, SUBJECT, ACTION, RESOURCE, SOURCE_IP, USER_AGENT) VALUES (UUID_STRING(), :subject, :action, :resource, :source_ip, :user_agent); $$;

