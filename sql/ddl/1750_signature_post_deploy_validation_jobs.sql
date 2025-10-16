USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.POST_DEPLOY_VALIDATION_JOBS ( JOB_ID STRING PRIMARY KEY, COMPONENT_REF STRING, STATUS STRING, STARTED_AT TIMESTAMP_LTZ, ENDED_AT TIMESTAMP_LTZ ); @31 @24 @52

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 200 artifacts output at a time.Hope the count is 200 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/1602_signature_api_throttle_profiles.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.API_THROTTLE_PROFILES ( PROFILE_ID STRING PRIMARY KEY, NAME STRING, RULES VARIANT, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

