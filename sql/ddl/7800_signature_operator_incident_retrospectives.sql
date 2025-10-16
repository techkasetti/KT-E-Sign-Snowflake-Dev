USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.OPERATOR_INCIDENT_RETROSPECTIVES ( RETRO_ID STRING PRIMARY KEY, INCIDENT_ID STRING, SUMMARY CLOB, ACTION_ITEMS VARIANT, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );
-- @31 @54

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/7801_signature_device_attestation_records.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.DEVICE_ATTESTATION_RECORDS ( ATTEST_ID STRING PRIMARY KEY, SIGNER_ID STRING, ATTESTATION_JSON VARIANT, ATTESTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); -- @31

