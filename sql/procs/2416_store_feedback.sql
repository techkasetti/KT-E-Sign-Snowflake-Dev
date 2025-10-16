CREATE OR REPLACE PROCEDURE DOCGEN.STORE_FEEDBACK(source STRING, subject_ref STRING, feedback VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.FEEDBACK_RECORDS (FB_ID, SOURCE, SUBJECT_REF, FEEDBACK) VALUES (UUID_STRING(), :source, :subject_ref, :feedback);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 100 artifacts output at a time.Hope the count is 100 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/2417_signature_feedback_metrics.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.FEEDBACK_METRICS ( METRIC_ID STRING PRIMARY KEY, FB_ID STRING, METRIC_NAME STRING, METRIC_VALUE VARIANT, RECORDED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

