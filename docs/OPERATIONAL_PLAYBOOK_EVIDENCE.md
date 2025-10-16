Purpose: Runbook entries for evidence lifecycle: assemble -> zip -> export -> reconcile -> archive. @70 @113
# Evidence operational playbook
1) Verify the assembly run produced a DOCUMENT_ARCHIVE row.
2) CALL DOCGEN.EVIDENCE_ZIPPER_SQL('<request_id>','operator') and confirm DOCGEN.EVIDENCE_BUNDLE row created.
3) To export bundles to S3: CALL DOCGEN.EXPORT_EVIDENCE_TO_S3('<manifest_id>','s3://docgen-evidence-archive',365)
4) To verify export and reconcile: CALL DOCGEN.RECONCILE_EVIDENCE_EXPORTS('<manifest_id>')
5) To archive older evidence: CALL DOCGEN.ARCHIVE_OLD_EVIDENCE(1095)
6) For OCSP checks on certificates: CALL DOCGEN.CHECK_OCSP_AND_UPDATE('<cert_fingerprint>') — review DOCGEN.PKI_OCSP_STATUS for results.

