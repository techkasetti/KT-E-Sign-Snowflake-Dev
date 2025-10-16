USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE PROCEDURE DOCGEN.WRITE_SIGNATURE_AUDIT(audit VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='write_signature_audit';

Persist detailed audit entries (including device, IP, liveness metadata) for compliance. @491 @31

