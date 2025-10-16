CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_POLICY_AUDIT_INLINE(paudit_id STRING, version_id STRING, changed_by STRING, changes VARIANT)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='record_policy_audit';

Inline audit writer invoked by Admin API when policies change. @219 @31

