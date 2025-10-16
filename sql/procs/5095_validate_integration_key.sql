CREATE OR REPLACE PROCEDURE DOCGEN.VALIDATE_INTEGRATION_KEY(account_id STRING, plaintext_key STRING)
RETURNS BOOLEAN
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='validate_integration_key';

Validation proc for integration-key middleware preflight calls. @31 @44

