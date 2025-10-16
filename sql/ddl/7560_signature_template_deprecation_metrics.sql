USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.TEMPLATE_DEPRECATION_METRICS ( MET_ID STRING PRIMARY KEY, TEMPLATE_ID STRING, METRICS VARIANT, RECORDED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/7561_signature_template_change_metrics.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.TEMPLATE_CHANGE_METRICS ( MET_ID STRING PRIMARY KEY, TEMPLATE_ID STRING, METRICS VARIANT, RECORDED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
