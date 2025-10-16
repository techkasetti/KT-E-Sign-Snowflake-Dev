CREATE OR REPLACE PROCEDURE DOCGEN.ADMIN_AUDIT_NOTIFY(audit_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='admin_audit_notify';

Notifies system operators on critical admin audit events (policy changes, key rotations). @104 @31

