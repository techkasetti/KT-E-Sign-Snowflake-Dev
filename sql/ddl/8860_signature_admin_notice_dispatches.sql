USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ADMIN_NOTICE_DISPATCHES ( DISPATCH_ID STRING PRIMARY KEY, TEMPLATE_ID STRING, TARGET_SCOPE VARIANT, STATUS STRING, DISPATCHED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); -- @24 @31 @56

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/8861_signature_admin_alert_subscriptions.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ADMIN_ALERT_SUBSCRIPTIONS ( SUB_ID STRING PRIMARY KEY, ADMIN_USER STRING, ALERT_RULE_ID STRING, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

