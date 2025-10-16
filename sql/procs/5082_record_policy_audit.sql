CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_POLICY_AUDIT(paudit_id STRING, version_id STRING, changed_by STRING, changes VARIANT)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='record_policy_audit';

Stores policy change audits for governance and compliance. @123 @31

