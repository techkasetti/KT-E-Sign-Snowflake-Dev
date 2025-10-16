USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.RISK_REGISTER ( RISK_ID STRING PRIMARY KEY, TITLE STRING, DESCRIPTION CLOB, SEVERITY STRING, STATUS STRING DEFAULT 'OPEN', CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); -- @31

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/7861_signature_risk_assessment_templates.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.RISK_ASSESSMENT_TEMPLATES ( TEMPLATE_ID STRING PRIMARY KEY, NAME STRING, RULES VARIANT, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); -- @31

