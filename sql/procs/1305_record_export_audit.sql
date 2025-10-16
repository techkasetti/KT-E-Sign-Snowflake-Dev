-- Record the start/finish and status of an evidence export for operator accountability. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_EXPORT_AUDIT(export_id STRING, operator STRING, status STRING, details VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.EVIDENCE_EXPORT_AUDIT (AUDIT_ID, EXPORT_ID, OPERATOR, STATUS, DETAILS) VALUES (UUID_STRING(), :export_id, :operator, :status, :details);
$$;

