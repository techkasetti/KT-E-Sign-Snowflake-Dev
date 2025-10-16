USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.TEMPLATE_PERMISSION_GRANTS ( GRANT_ID STRING PRIMARY KEY, REQ_ID STRING, GRANTED_TO STRING, GRANTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: These DDL artifacts continue the Snowflake E-Signature module generation and follow the Snowpark/registration and evidence/archive patterns in your workspace @31 @24

