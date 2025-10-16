USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE PROCEDURE DOCGEN.CREATE_RETENTION_POLICY(policy_id STRING, name STRING, tables ARRAY, retention_days INT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='create_retention_policy';

Stored proc template to create retention schedules and wire TASKs for purge enforcement. @120 @31

