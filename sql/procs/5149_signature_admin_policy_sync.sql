CREATE OR REPLACE PROCEDURE DOCGEN.SYNC_ADMIN_POLICIES()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='sync_admin_policies';

Synchronizes admin policy definitions to per-account effective policy records. @37 @31

