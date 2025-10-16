USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.PRODUCT_FEEDBACK_RESULTS ( RES_ID STRING PRIMARY KEY, FB_ID STRING, HANDLED_BY STRING, OUTCOME VARIANT, HANDLED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/707_signature_template_retention_policies.sql @31 @24
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN; @31 @24
CREATE OR REPLACE TABLE DOCGEN.TEMPLATE_RETENTION_POLICIES ( POLICY_ID STRING PRIMARY KEY, TEMPLATE_ID STRING, RETENTION_DAYS INT, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31 @24

