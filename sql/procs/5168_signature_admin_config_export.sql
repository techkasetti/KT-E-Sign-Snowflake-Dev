CREATE OR REPLACE PROCEDURE DOCGEN.EXPORT_ADMIN_CONFIG(account_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='export_admin_config';

Exports configuration for an account for backup or transfer to another environment. @21 @31

