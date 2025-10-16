USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.CSR_REQUEST_REGISTRY ( REQ_ID STRING PRIMARY KEY, REQUESTOR STRING, CSR_TEXT CLOB, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @1

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/3780_signature_csr_approve_requests.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.CSR_APPROVE_REQUESTS ( REQ_ID STRING PRIMARY KEY, CSR_ID STRING, APPROVER STRING, STATUS STRING, REQUESTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(), DECIDED_AT TIMESTAMP_LTZ );

