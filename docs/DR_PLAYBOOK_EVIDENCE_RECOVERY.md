Purpose: Disaster recovery playbook for evidence bundles: snapshot, export, restore steps and SLA expectations for recovery. @241
# DR Playbook — Evidence recovery
1) If evidence manifests are missing or S3 objects are incomplete, run CALL DOCGEN.RECONCILE_EVIDENCE_EXPORTS('<manifest_id>') to get reconciliation state. @113
2) If artifacts are lost and backups exist in DOCGEN.BACKUP_DOCUMENT_ARCHIVE_CLONE, reconstruct with CALL DOCGEN.RESTORE_TABLE('<snapshot_table>', 'EVIDENCE_BUNDLE'). @241
3) For HSM/signature key compromise, rotate keys in HSM, update certificate store rows and re-run verification procs to flag impacted EvidenceBundles. @96
4) Operators must notify compliance and preserve forensic snapshots (use SNAPSHOT_TABLE pattern) and record incident in DOCGEN.PURGE_AUDIT.

