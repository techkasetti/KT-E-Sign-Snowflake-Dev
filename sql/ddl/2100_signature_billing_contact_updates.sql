USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.BILLING_CONTACT_UPDATES ( UPDATE_ID STRING PRIMARY KEY, CONTACT_ID STRING, UPDATED_BY STRING, UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 200 artifacts output at a time.Hope the count is 200 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/2101_signature_api_usage_metrics.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.API_USAGE_METRICS ( METRIC_ID STRING PRIMARY KEY, API_CLIENT STRING, ENDPOINT STRING, CALL_COUNT INT, ERROR_COUNT INT, RECORDED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31

