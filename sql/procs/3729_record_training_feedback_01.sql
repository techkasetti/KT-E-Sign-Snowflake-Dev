CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_TRAINING_FEEDBACK_01(model_id STRING, input VARIANT, rating INT, comments STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.TRAINING_FEEDBACK_01 (FEEDBACK_ID, MODEL_ID, INPUT, RATING, COMMENTS) VALUES (UUID_STRING(), :model_id, :input, :rating, :comments);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/3730_signature_model_registry.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.MODEL_REGISTRY (
  MODEL_ID STRING PRIMARY KEY,
  NAME STRING,
  VERSION STRING,
  META VARIANT,
  REGISTERED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

