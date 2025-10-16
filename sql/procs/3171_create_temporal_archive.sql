CREATE OR REPLACE PROCEDURE DOCGEN.CREATE_TEMPORAL_ARCHIVE(original_ref STRING, payload VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.TEMPORAL_ARCHIVES (ARCH_ID, ORIGINAL_REF, PAYLOAD) VALUES (UUID_STRING(), :original_ref, :payload);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts output at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: Emitting next 150 Snowflake artifacts (DDL, stored procedures, tasks, queues, logs and materialized views) for the E-Signature module per your request @31 @24 @52

