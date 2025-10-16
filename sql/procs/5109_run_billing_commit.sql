PUT file://sql/procs/run_billing_commit.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.RUN_BILLING_COMMIT(preview_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
IMPORTS = ('@~/procedures/run_billing_commit.py')
HANDLER = 'run_billing_commit';

Commits previewed invoice into BILLING_RUN / BILLING_LINE_ITEM persisted records. @35 @31

