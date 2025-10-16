CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_ACCESS_LOG_HELPER(log_id STRING, subject STRING, action STRING, resource STRING, source_ip STRING, user_agent STRING)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='record_access_log';

Alias wrapper for callers from External Functions / UI to centralize access logging semantics. @125 @31

