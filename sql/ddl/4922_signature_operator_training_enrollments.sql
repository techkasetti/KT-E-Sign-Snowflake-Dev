USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.OPERATOR_TRAINING_ENROLLMENTS ( ENR_ID STRING PRIMARY KEY, REQ_ID STRING, OPERATOR_REF STRING, ENROLLED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); -- @31 @24 @56

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/4923_signature_archival_retention_index.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ARCHIVAL_RETENTION_INDEX ( INDEX_ID STRING PRIMARY KEY, TARGET_REF STRING, RETENTION_POLICY_ID STRING, EVALUATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); -- @31 @24
