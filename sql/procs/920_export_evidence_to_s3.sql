-- Example stored-proc to assemble CompliancePacket JSON and write manifest to a stage/S3 location for archival; follow runbook for stage config. @28 @31
CREATE OR REPLACE PROCEDURE DOCGEN.EXPORT_EVIDENCE_TO_S3(request_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
PACKAGES=('snowflake-snowpark-python')
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/export_evidence.py
$$;

