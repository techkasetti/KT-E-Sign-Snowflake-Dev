CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_TEMPLATE_USAGE_EXPORT_LOG(export_id STRING, status STRING, details VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.TEMPLATE_USAGE_EXPORT_LOGS (LOG_ID, EXPORT_ID, STATUS, DETAILS) VALUES (UUID_STRING(), :export_id, :status, :details);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 100 artifacts output at a time.Hope the count is 100 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: These generated Snowflake artifacts follow the Snowpark, External Function, FAISS, and evidence/PKI patterns in your workspace @1 @31 @6

