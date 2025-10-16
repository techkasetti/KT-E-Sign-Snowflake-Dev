-- PUT file://snowpark/procedures/reconcile_evidence_exports.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.RECONCILE_EVIDENCE_EXPORTS(manifest_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
IMPORTS = ('@~/procedures/reconcile_evidence_exports.py')
HANDLER = 'reconcile_evidence_exports';
-- Export reconciliation proc for evidence export patterns @62 @113

