USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.COMPLIANCE_TRAINING_RESULTS ( RES_ID STRING PRIMARY KEY, JOB_ID STRING, COMPLETION_METRICS VARIANT, COMPLETED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @1

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/7745_signature_alert_subscription_policies.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ALERT_SUBSCRIPTION_POLICIES ( POLICY_ID STRING PRIMARY KEY, NAME STRING, CRITERIA VARIANT, OWNER STRING, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );
-- @31 @54
