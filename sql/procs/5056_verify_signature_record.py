PUT file://sql/procs/verify_signature_record.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.VERIFY_SIGNATURE_RECORD(bundle_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
IMPORTS = ('@~/procedures/verify_signature_record.py')
HANDLER = 'verify_signature';

(Snowpark Python handler implements certificate chain validation, OCSP lookup, and writes SIGNATURE_VERIFICATIONS.) @94 @31

