#!/usr/bin/env bash
set -e
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "PUT file://sql/procs/write_evidence_bundle.py @~/procedures/ AUTO_COMPRESS=TRUE;"
snowsql -a client_prod_001 -u svc_docgen -r DOCGEN_ADMIN -q "CREATE OR REPLACE PROCEDURE DOCGEN.WRITE_EVIDENCE_BUNDLE(assembly_run_id STRING, document_id STRING, signer_id STRING, signature_hash STRING, cert_chain VARIANT, archive_location STRING, metadata VARIANT) RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.10' IMPORTS=('@~/procedures/write_evidence_bundle.py') HANDLER='write_evidence_bundle';"
echo "WRITE_EVIDENCE_BUNDLE registered." @65 @21

