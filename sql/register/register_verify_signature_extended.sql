-- PUT file://snowpark/procedures/verify_signature_extended.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.VERIFY_SIGNATURE_EXTENDED(request_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
IMPORTS = ('@~/procedures/verify_signature_extended.py')
HANDLER = 'verify_signature_extended';
-- Register verification SP following bundle registration flow @31 @68

