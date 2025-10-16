CREATE OR REPLACE PROCEDURE DOCGEN.SYNC_ROLES_WITH_IDENTITY_PROVIDER()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='sync_roles_with_idp';

Synchronizes Snowflake admin role assignments with external identity provider metadata when configured. @36 @31

