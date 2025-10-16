USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
-- Generated per Snowflake E-Signature patterns @31 @24 @56
CREATE OR REPLACE TABLE DOCGEN.SIGNER_CONSENT_FORMS (
  FORM_ID STRING PRIMARY KEY,
  SIGNER_ID STRING,
  FORM_CLOB CLOB,
  SIGNED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/7421_signature_policy_exception_requests.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.POLICY_EXCEPTION_REQUESTS (
  REQ_ID STRING PRIMARY KEY,
  POLICY_ID STRING,
  REQUESTOR STRING,
  REASON CLOB,
  STATUS STRING DEFAULT 'OPEN',
  REQUESTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
); -- @1

