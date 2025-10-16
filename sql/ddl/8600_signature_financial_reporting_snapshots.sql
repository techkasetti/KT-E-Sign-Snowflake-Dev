USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.FINANCIAL_REPORTING_SNAPSHOTS ( SNAP_ID STRING PRIMARY KEY, PERIOD_START DATE, PERIOD_END DATE, METRICS VARIANT, GENERATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/8601_signature_access_policy_templates.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ACCESS_POLICY_TEMPLATES ( TEMPLATE_ID STRING PRIMARY KEY, NAME STRING, RULES VARIANT, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

