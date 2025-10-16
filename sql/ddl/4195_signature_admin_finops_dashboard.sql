USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ADMIN_FINOPS_DASHBOARD ( DASH_ID STRING PRIMARY KEY, SNAPSHOT VARIANT, GENERATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/4196_signature_compliance_dashboard_alerts.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.COMPLIANCE_DASHBOARD_ALERTS ( ALERT_ID STRING PRIMARY KEY, DASHBOARD_ID STRING, ALERT_JSON VARIANT, RAISED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(), STATUS STRING );

