USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.AUDIT_IMMUTABLE_INDEX ( IDX_ID STRING PRIMARY KEY, AUDIT_REF STRING, HASH STRING, INDEXED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() )

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/8501_signature_policy_violation_notifications.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.POLICY_VIOLATION_NOTIFICATIONS ( NOTIF_ID STRING PRIMARY KEY, POLICY_ID STRING, TARGET_REF STRING, DETAILS VARIANT, NOTIFIED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @24 @31

