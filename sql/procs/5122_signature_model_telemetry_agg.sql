CREATE OR REPLACE PROCEDURE DOCGEN.AGGREGATE_MODEL_TELEMETRY(window_hours INT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='aggregate_model_telemetry';

Aggregates telemetry for model drift detection and alerts. @66 @31

