CREATE OR REPLACE PROCEDURE DOCGEN.CREATE_POLICY_VERSION(policy_id STRING, body CLOB, created_by STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='create_policy_version';

Version-controlled policy writer to support rollbacks and audits. @15 @31

