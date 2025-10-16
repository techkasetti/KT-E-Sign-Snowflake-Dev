-- PUT file://snowpark/procedures/upsert_signature_webhook.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.UPSERT_SIGNATURE_WEBHOOK(stage_payload VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
IMPORTS = ('@~/procedures/upsert_signature_webhook.py')
HANDLER = 'upsert_signature_webhook';
-- Registration follows PUT -> CREATE PROCEDURE pattern in runbooks @31 @36

