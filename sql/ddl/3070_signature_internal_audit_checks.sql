USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.INTERNAL_AUDIT_CHECKS ( CHECK_ID STRING PRIMARY KEY, NAME STRING, QUERY_CLOB CLOB, LAST_RUN_AT TIMESTAMP_LTZ );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/3001_signature_legal_hold_requests.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.LEGAL_HOLD_REQUESTS ( HOLD_ID STRING PRIMARY KEY, TENANT_ID STRING, SCOPE VARIANT, REQUESTOR STRING, STATUS STRING, REQUESTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31

