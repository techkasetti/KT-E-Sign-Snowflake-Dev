CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_SENSITIVE_ACCESS_13(log_id STRING, field_ref STRING, accessor STRING, reason STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.SENSITIVE_DATA_ACCESS_LOGS_13 (LOG_ID, FIELD_REF, ACCESSOR, REASON) VALUES (:log_id, :field_ref, :accessor, :reason);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: Continuing emission of the next 130 Snowflake E-Signature artifacts (DDL + stored procedures). @31 @24 @29
