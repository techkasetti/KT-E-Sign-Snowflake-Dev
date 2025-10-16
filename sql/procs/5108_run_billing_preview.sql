PUT file://sql/procs/run_billing_preview.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.RUN_BILLING_PREVIEW(account_id STRING, preview_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
IMPORTS = ('@~/procedures/run_billing_preview.py')
HANDLER = 'run_billing_preview';

Preview billing stored-proc that returns deterministic line_items + invoice_hash. @35 @31

