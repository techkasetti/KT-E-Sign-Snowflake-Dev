-- PUT file://snowpark/procedures/record_object_lock.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_OBJECT_LOCK(object_ref STRING, locked_by STRING, expires_at TIMESTAMP_LTZ)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
IMPORTS = ('@~/procedures/record_object_lock.py')
HANDLER = 'record_object_lock';
-- Lock registration script follows PUT->CREATE pattern in runbooks @31 @44

