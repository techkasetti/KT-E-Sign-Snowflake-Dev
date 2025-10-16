USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.DEPLOY_ROLLBACK_REQS ( REQ_ID STRING PRIMARY KEY, RUN_ID STRING, REQUESTOR STRING, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/6694_signature_tenant_notification_rules.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN; CREATE OR REPLACE TABLE DOCGEN.TENANT_NOTIFICATION_RULES ( RULE_ID STRING PRIMARY KEY, TENANT_ID STRING, RULE_JSON VARIANT, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

