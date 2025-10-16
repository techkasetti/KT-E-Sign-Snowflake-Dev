USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.MODEL_DRIFT_RETRAINING_JOBS ( JOB_ID STRING PRIMARY KEY, MODEL_ID STRING, TRIGGER_REASON STRING, STATUS STRING DEFAULT 'PENDING', CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/9201_signature_model_drift_retraining_results.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.MODEL_DRIFT_RETRAINING_RESULTS ( RES_ID STRING PRIMARY KEY, JOB_ID STRING, METRICS VARIANT, COMPLETED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); -- @31
