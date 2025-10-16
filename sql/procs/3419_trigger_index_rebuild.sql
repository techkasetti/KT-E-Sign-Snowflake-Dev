CREATE OR REPLACE PROCEDURE DOCGEN.TRIGGER_INDEX_REBUILD(index_name STRING, triggered_by STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.INDEX_REBUILDS (REBUILD_ID, INDEX_NAME, TRIGGERED_BY, STARTED_AT, STATUS) VALUES (UUID_STRING(), :index_name, :triggered_by, CURRENT_TIMESTAMP(), 'RUNNING');
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: Continuing with the next 130 Snowflake artifacts (DDL + stored procedures) for the E-Signature module; these follow the Snowpark, External Function, FAISS, and evidence/PKI patterns in your workspace @31 @24 @42

