# Acceptance criteria for E‑Signature Snowflake module (dev/stage)
1) Core DDL deployed without errors and row-counts for seed data match expected values. @67 @285
2) Snowpipe ingestion path accepts demo JSONL and populates DOCGEN.SIGNATURE_EVENTS_RAW and STREAM_SIG_EVENTS_RAW fires. @130 @264
3) UPSERT_SIGNATURE_WEBHOOK handles idempotent duplicate calls and signer/request statuses converge to COMPLETED after signature events. @272 @273
4) EXPORT_EVIDENCE_TO_S3 writes a manifest row and uploads a JSONL manifest to S3; RECONCILE_EVIDENCE_EXPORTS reports OK when counts match. @66 @208
5) OCSP checking via External Function updates DOCGEN.PKI_OCSP_STATUS and verify_signature_extended returns verified=True for GOOD OCSP. @68 @101
6) Masking & row-access policies hide signer emails for non-admin roles and enforce per-account isolation. @101 @76
7) FAISS external function call records retrieval rows into DOCGEN.RETRIEVAL_RESULTS and service test returns results. @34 @261

This acceptance checklist maps each runbook verification to the corresponding module behavior for operational sign-off and pilot validation @67 @101.

— End of tranche: added full export/purge/PKI+OCSP, masking policies, Streamlit monitor, final registration helpers, FAISS service integration helpers, reconciliation procs, manifest and acceptance criteria to complete the E‑Signature Snowflake module surface for staging and pilot validation per the Snowpark/External Function patterns in your design docs @67 @68 @101.

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.Output with maximum output

- Anara: Below are additional Snowflake-side artifacts for the E‑Signature module: assembly service + stored procedures, evidence zipper implementation, HSM signer stub integration flow, assembly/task registration, evidence export verification views, retention enforcement task, operational CI scripts, and deployment helpers — each file is copy/paste-ready and follows the Snowpark/External Function, FAISS snapshot, evidence/PKI and registration patterns in your design docs. @31 @70

