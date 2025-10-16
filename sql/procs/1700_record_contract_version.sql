CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_CONTRACT_VERSION(contract_id STRING, version INT, body CLOB)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.PROVIDER_CONTRACT_VERSIONS (PCV_ID, CONTRACT_ID, VERSION, BODY) VALUES (UUID_STRING(), :contract_id, :version, :body);
$$;

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 100 artifacts output at a time.Hope the count is 100 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/1751_signature_api_clients.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.API_CLIENTS (
  CLIENT_ID STRING PRIMARY KEY,
  CLIENT_NAME STRING,
  CLIENT_SECRET_HASH STRING,
  OWNER_ACCOUNT STRING,
  CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

