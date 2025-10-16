CREATE OR REPLACE PROCEDURE DOCGEN.GENERATE_POLICY_REPORT(account_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='generate_policy_report';

Generates per-account policy compliance reports for auditors. @15 @31

