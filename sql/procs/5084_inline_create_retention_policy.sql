CREATE OR REPLACE PROCEDURE DOCGEN.CREATE_RETENTION_POLICY_INLINE(policy_id STRING, name STRING, tables ARRAY, retention_days INT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='create_retention_policy';

Inline helper for creating retention policy records programmatically. @120 @31

