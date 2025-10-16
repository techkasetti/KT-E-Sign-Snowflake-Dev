-- PUT file://snowpark/procedures/record_integration_call.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_INTEGRATION_CALL(target STRING, payload VARIANT, response VARIANT, status STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
IMPORTS = ('@~/procedures/record_integration_call.py')
HANDLER = 'record_integration_call';
-- Registration for integration call audit proc per External Function patterns @82 @217

