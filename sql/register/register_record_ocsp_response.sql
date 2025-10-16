-- PUT file://snowpark/procedures/record_ocsp_response.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_OCSP_RESPONSE(cert_fingerprint STRING, ocsp_response VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
IMPORTS = ('@~/procedures/record_ocsp_response.py')
HANDLER = 'record_ocsp_response';
-- OCSP response recorder proc registered per evidence/PKI runbook @52 @68

