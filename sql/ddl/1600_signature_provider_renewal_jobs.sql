USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.PROVIDER_RENEWAL_JOBS ( JOB_ID STRING PRIMARY KEY, PROVIDER_ID STRING, STATUS STRING, STARTED_AT TIMESTAMP_LTZ, ENDED_AT TIMESTAMP_LTZ );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 200 artifacts output at a time.Hope the count is 200 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/1551_signature_monitoring_alert_rules.sql
CREATE OR REPLACE TABLE DOCGEN.MONITORING_ALERT_RULES ( RULE_ID STRING PRIMARY KEY, NAME STRING, CONDITION VARIANT, SEVERITY STRING, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31 @24

