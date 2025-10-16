-- Registration SQL for WRITE_EVIDENCE_BUNDLE Snowpark proc; use PUT to stage the Python file then CREATE PROCEDURE per runbook patterns. @31 @29
CREATE OR REPLACE PROCEDURE DOCGEN.WRITE_EVIDENCE_BUNDLE(request_id STRING, archive_path STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
PACKAGES=('snowflake-snowpark-python')
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/write_evidence_bundle.py
$$;

