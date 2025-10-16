CREATE OR REPLACE PROCEDURE DOCGEN.ENQUEUE_DATA_DELETION_JOB(target_ref STRING, request_id STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.DATA_DELETION_JOBS (JOB_ID, TARGET_REF, REQUEST_ID, STATUS) VALUES (UUID_STRING(), :target_ref, :request_id, 'PENDING');
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts output at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: Continuing generation of Snowflake E-Signature artifacts (Snowpark/Snowpipe/External Function and evidence/PKI patterns) as requested @7 @36 @58

