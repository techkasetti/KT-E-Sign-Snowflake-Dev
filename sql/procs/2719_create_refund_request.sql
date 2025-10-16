CREATE OR REPLACE PROCEDURE DOCGEN.CREATE_REFUND_REQUEST(account_id STRING, amount NUMBER, reason STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.REFUND_REQUESTS (REQUEST_ID, ACCOUNT_ID, AMOUNT, REASON) VALUES (UUID_STRING(), :account_id, :amount, :reason);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 100 artifacts output at a time.Hope the count is 100 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/2720_signature_refund_outcomes.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.REFUND_OUTCOMES (
  OUTCOME_ID STRING PRIMARY KEY,
  REQUEST_ID STRING,
  STATUS STRING,
  PROCESSED_AT TIMESTAMP_LTZ,
  DETAILS VARIANT
);

