USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.DOCGEN_CONTENT_DRIFT_RESULTS ( RES_ID STRING PRIMARY KEY, JOB_ID STRING, METRICS VARIANT, COMPLETED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/8932_signature_docgen_content_drift_alerts.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.DOCGEN_CONTENT_DRIFT_ALERTS ( ALERT_ID STRING PRIMARY KEY, RES_ID STRING, DETAILS VARIANT, RAISED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

