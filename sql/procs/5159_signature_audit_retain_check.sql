CREATE OR REPLACE PROCEDURE DOCGEN.CHECK_AUDIT_RETENTION()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='check_audit_retention';

Validates audit data retention across configured retention windows. @36 @31

