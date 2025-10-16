CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_ACCESS_LOG(log_id STRING, subject STRING, action STRING, resource STRING, source_ip STRING, user_agent STRING)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='record_access_log';

Idempotent access log writer used by API endpoints and Admin actions. @125 @31

