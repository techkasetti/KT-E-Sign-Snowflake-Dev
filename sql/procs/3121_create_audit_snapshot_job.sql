CREATE OR REPLACE PROCEDURE DOCGEN.CREATE_AUDIT_SNAPSHOT_JOB(scope VARIANT, target_stage STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.AUDIT_SNAPSHOT_JOBS (JOB_ID, SCOPE, TARGET_STAGE) VALUES (UUID_STRING(), :scope, :target_stage);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts output at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/3172_signature_consent_notifications.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.CONSENT_NOTIFICATIONS ( NOTIF_ID STRING PRIMARY KEY, CONSENT_ID STRING, CHANNEL STRING, SENT_AT TIMESTAMP_LTZ, STATUS STRING );

