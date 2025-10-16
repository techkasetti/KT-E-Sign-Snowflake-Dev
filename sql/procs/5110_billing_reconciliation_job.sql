CREATE OR REPLACE PROCEDURE DOCGEN.RUN_BILLING_RECONCILE(batch_limit INT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='run_billing_reconcile';

Reconciliation worker that cross-checks Snowflake invoice runs with external billing systems. @35 @31

