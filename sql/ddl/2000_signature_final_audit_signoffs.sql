USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.FINAL_AUDIT_SIGNOFFS ( SIGNOFF_ID STRING PRIMARY KEY, AUDIT_REF STRING, SIGNED_BY STRING, SIGNED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31 @24 @1

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 200 artifacts output at a time.Hope the count is 200 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/1821_signature_notification_delivery_logs.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.NOTIFICATION_DELIVERY_LOGS ( LOG_ID STRING PRIMARY KEY, NOTIF_ID STRING, CHANNEL STRING, STATUS STRING, ATTEMPT_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(), DETAILS VARIANT ); @31

