CREATE OR REPLACE PROCEDURE DOCGEN.UPDATE_MODEL_REGISTRY(model_id STRING, meta VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='update_model_registry';

Updates model registry with metadata and triggers per-tenant rollout policies. @352 @31

