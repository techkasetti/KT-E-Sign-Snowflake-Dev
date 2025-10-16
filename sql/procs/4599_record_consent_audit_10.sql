CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_CONSENT_AUDIT_10(consent_audit_id STRING, signer_id STRING, consent_text STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.CONSENT_AUDITS_10 (CONSENT_AUDIT_ID, SIGNER_ID, CONSENT_TEXT) VALUES (:consent_audit_id, :signer_id, :consent_text);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/4600_signature_session_audit.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SESSION_AUDIT_11 (
  AUDIT_ID STRING PRIMARY KEY,
  SESSION_ID STRING,
  EVENT_TYPE STRING,
  PAYLOAD VARIANT,
  EVENT_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

