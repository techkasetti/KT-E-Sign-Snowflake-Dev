CREATE OR REPLACE PROCEDURE DOCGEN.RUN_MODEL_CANARY(model_id STRING, percent FLOAT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='run_model_canary';

Implements canary release orchestration to route a small percentage of scoring to new model. @376 @31

