-- PUT file://snowpark/procedures/purge_old_documents.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.PURGE_OLD_DOCUMENTS(days INT, dry_run BOOLEAN)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
IMPORTS = ('@~/procedures/purge_old_documents.py')
HANDLER = 'purge_old_documents';
-- Register purge proc per retention enforcement guidance @176 @62

