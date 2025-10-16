USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_ACCESS_LOG(log_id STRING, subject STRING, action STRING, resource STRING, source_ip STRING, user_agent STRING)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='record_access_log';

Records access log entries via a Python stored-proc wrapper for richer payload handling. @124 @31

