CREATE OR REPLACE PROCEDURE DOCGEN.IMPORT_ADMIN_CONFIG(account_id STRING, config VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='import_admin_config';

Imports admin configuration and records policy change audits. @21 @31

