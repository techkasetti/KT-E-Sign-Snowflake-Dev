CREATE OR REPLACE PROCEDURE DOCGEN.WRITE_PKI_AUDIT(entry VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='write_pki_audit';

Writes PKI lifecycle audit events (key rotation, signing approvals) to audit tables. @113 @31

