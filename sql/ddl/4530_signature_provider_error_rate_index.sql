USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.PROVIDER_ERROR_RATE_INDEX ( IDX_ID STRING PRIMARY KEY, PROVIDER_ID STRING, ERROR_RATE NUMBER, RECORDED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/4481_signature_policy_change_requests.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.POLICY_CHANGE_REQUESTS ( REQ_ID STRING PRIMARY KEY, POLICY_ID STRING, REQUESTOR STRING, CHANGE_JSON VARIANT, STATUS STRING DEFAULT 'OPEN', REQUESTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); -- @36

