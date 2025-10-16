CREATE OR REPLACE PROCEDURE DOCGEN.REGISTER_SNAPSHOT_REFERENCE_12(ref_id STRING, snap_id STRING, ref_type STRING, ref_payload VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.SNAPSHOT_REFERENCES_12 (REF_ID, SNAP_ID, REF_TYPE, REF_PAYLOAD) VALUES (:ref_id, :snap_id, :ref_type, :ref_payload);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: Emitting next 130 Snowflake E-Signature artifacts (DDL + stored procedures) ready to copy/paste. @31 @56

