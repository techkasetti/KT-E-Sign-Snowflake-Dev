-- PUT file://snowpark/procedures/record_admin_audit.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_ADMIN_AUDIT(admin_user STRING, action STRING, target STRING, details VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
IMPORTS = ('@~/procedures/record_admin_audit.py')
HANDLER = 'record_admin_audit';
-- Admin audit proc registration for governance records @116 @31

