-- PUT file://snowpark/procedures/process_raw_signature_events.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.PROCESS_RAW_SIGNATURE_EVENTS(limit INT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
IMPORTS = ('@~/procedures/process_raw_signature_events.py')
HANDLER = 'process_raw_signature_events';
-- Register raw event processor per registration patterns @31 @44

