USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.OPERATOR_AUDIT_SCORECARD ( SCORE_ID STRING PRIMARY KEY, OPERATOR_REF STRING, SCORES VARIANT, GENERATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/4858_signature_session_timeout_policies.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SESSION_TIMEOUT_POLICIES ( POLICY_ID STRING PRIMARY KEY, NAME STRING, TIMEOUT_SECONDS INT, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); -- @31 @24 @56

