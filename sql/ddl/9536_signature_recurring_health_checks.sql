USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.RECURRING_HEALTH_CHECKS ( CHECK_ID STRING PRIMARY KEY, TARGET_REF STRING, CRON_EXPR STRING, LAST_RUN TIMESTAMP_LTZ );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/9537_signature_operator_oncall_rota.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.OPERATOR_ONCALL_ROTA ( ROTA_ID STRING PRIMARY KEY, OP_REF STRING, START_AT TIMESTAMP_LTZ, END_AT TIMESTAMP_LTZ, ROLE STRING, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );
