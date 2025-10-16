USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SIGNING_SESSION_MITIGATION_ACTIONS ( ACTION_ID STRING PRIMARY KEY, VIOL_ID STRING, ACTION_JSON VARIANT, EXECUTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/9051_signature_policy_change_log.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.POLICY_CHANGE_LOG ( CHANGE_ID STRING PRIMARY KEY, POLICY_ID STRING, CHANGED_BY STRING, CHANGE_JSON VARIANT, CHANGED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

