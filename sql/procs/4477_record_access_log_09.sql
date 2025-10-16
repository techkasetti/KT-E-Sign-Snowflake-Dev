-- Generated per Snowflake E-Signature patterns. @31
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_ACCESS_LOG_09(log_id STRING, subject STRING, action STRING, resource STRING, source_ip STRING, user_agent STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.ACCESS_LOGS_09 (LOG_ID, SUBJECT, ACTION, RESOURCE, SOURCE_IP, USER_AGENT) VALUES (:log_id, :subject, :action, :resource, :source_ip, :user_agent);
$$

