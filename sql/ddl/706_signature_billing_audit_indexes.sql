USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.BILLING_AUDIT_INDEXES ( IDX_ID STRING PRIMARY KEY, AUDIT_ID STRING, KEY STRING, VALUE VARIANT, INDEXED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );
-- Generated per Snowflake E-Sign patterns @31

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/707_signature_admin_alerts.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ADMIN_ALERTS ( ALERT_ID STRING PRIMARY KEY, ALERT_KEY STRING, SEVERITY STRING, PAYLOAD VARIANT, RAISED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31

