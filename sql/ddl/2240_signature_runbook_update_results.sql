USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.RUNBOOK_UPDATE_RESULTS ( RES_ID STRING PRIMARY KEY, JOB_ID STRING, OUTCOME VARIANT, COMPLETED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 200 artifacts output at a time.Hope the count is 200 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/2241_signature_incident_service_mappings.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.INCIDENT_SERVICE_MAPPINGS (
  MAP_ID STRING PRIMARY KEY,
  INCIDENT_ID STRING,
  SERVICE_REF STRING,
  MAPPED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
); @31

