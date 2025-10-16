CREATE OR REPLACE PROCEDURE DOCGEN.EXPORT_ADMIN_AUDIT(account_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='export_admin_audit';

Exports admin audit entries as part of compliance packet generation. @104 @31

