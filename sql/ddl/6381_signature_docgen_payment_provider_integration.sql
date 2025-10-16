USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.PAYMENT_PROVIDER_INTEGRATION ( INTEG_ID STRING PRIMARY KEY, PROVIDER_NAME STRING, CONFIG VARIANT, REGISTERED_AT TIMESTAMP_LTZ ); @31 @24

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/6252_signature_admin_alert_suppression_rules.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ALERT_SUPPRESSION_RULES ( RULE_ID STRING PRIMARY KEY, ALERT_TYPE STRING, SUPPRESSION_WINDOW_SECONDS INT, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

