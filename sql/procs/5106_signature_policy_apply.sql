CREATE OR REPLACE PROCEDURE DOCGEN.APPLY_POLICY_TO_ACCOUNT(policy_id STRING, account_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='apply_policy_to_account';

Applies policy templates to tenant accounts and records audit. @15 @31

