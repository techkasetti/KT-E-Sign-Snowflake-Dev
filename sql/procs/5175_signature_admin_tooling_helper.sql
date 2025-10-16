CREATE OR REPLACE PROCEDURE DOCGEN.ADMIN_TOOLING_HELPER(action VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='admin_tooling_helper';

Admin tooling helper to support script-driven admin operations and migrations. @331 @31

