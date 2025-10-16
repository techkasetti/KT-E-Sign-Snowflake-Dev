USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.OPERATIONAL_RUN_METRICS ( METRIC_ID STRING PRIMARY KEY, RUN_REF STRING, METRICS VARIANT, RECORDED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() )

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts output at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/2851_signature_template_tag_index.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.TEMPLATE_TAG_INDEX ( TAG_ID STRING PRIMARY KEY, TEMPLATE_ID STRING, TAG STRING, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );
