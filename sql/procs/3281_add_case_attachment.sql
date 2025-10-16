CREATE OR REPLACE PROCEDURE DOCGEN.ADD_CASE_ATTACHMENT(case_id STRING, file_url STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.CASE_ATTACHMENTS (ATTACH_ID, CASE_ID, FILE_URL) VALUES (UUID_STRING(), :case_id, :file_url);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts output at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: Generating next tranche of 150 Snowflake artifacts (DDL and stored procedures) following the Snowpark / External Function / evidence & PKI patterns in your workspace for the E-Signature module @24 @31
