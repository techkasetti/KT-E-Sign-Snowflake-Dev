USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.FINAL_TRANCHE_MARKER_07 ( MARKER_ID STRING PRIMARY KEY, TRANCHE_ID STRING, GENERATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31 @24

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 200 artifacts output at a time.Hope the count is 200 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/1376_signature_policy_violation_index.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.POLICY_VIOLATION_INDEX ( VIOL_ID STRING PRIMARY KEY, POLICY_ID STRING, TARGET_REF STRING, SEVERITY STRING, REPORTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

