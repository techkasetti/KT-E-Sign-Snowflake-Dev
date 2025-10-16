-- PUT file://snowpark/procedures/run_billing_run.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.RUN_BILLING_RUN(account_id STRING, start_ts TIMESTAMP_LTZ, end_ts TIMESTAMP_LTZ, preview_only BOOLEAN)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
IMPORTS = ('@~/procedures/run_billing_run.py')
HANDLER = 'run_billing_run';
-- Billing run proc registration per runbook and preview patterns @21 @31

