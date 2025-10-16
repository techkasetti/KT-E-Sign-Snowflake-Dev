CREATE OR REPLACE PROCEDURE DOCGEN.INGEST_SIGNATURE_TELEMETRY(stage_payload VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='ingest_signature_telemetry';

Ingests high-frequency telemetry from signers (device metrics, fps, etc.) into MODEL_TELEMETRY. @61 @31

