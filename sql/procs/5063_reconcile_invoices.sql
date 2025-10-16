USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE PROCEDURE DOCGEN.RECONCILE_INVOICES(batch_size INT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
IMPORTS = ('@~/procedures/reconcile_invoices.py')
HANDLER = 'reconcile_invoices';

Invoice reconciliation worker to verify preview hashes vs committed runs and produce reconciliation artifacts. @35 @31

