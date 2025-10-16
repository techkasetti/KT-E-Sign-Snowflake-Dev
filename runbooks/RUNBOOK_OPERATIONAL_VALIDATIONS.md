Purpose: Operational validation checks to run after deployment to ensure signature flows, entitlement, billing, and FAISS snapshots are functioning. @31 @58

Operational validation checklist:
1) Signature flow: create a SIGNATURE_REQUEST row and call DOCGEN.UPSERT_SIGNATURE_WEBHOOK with event_type = 'SIGNED' and verify DOCGEN.SIGNATURE_VALIDATION and DOCGEN.EVIDENCE_BUNDLE are populated. @58 @300
2) Entitlement preflight: call DOCGEN.ENTITLEMENT_PREFLIGHT_AND_ACTIVATE stored procedure to validate integration keys and enable feature, then check DOCGEN.ACTIVATION_AUDIT for a SUCCESS row. @74 @189
3) Billing determinism: run DOCGEN.PREVIEW_BILLING and compare invoice_hash across runs, then run DOCGEN.RECONCILE_BILLING_RUNS to surface any mismatch rows. @21 @29
4) FAISS snapshot flow: ensure DOCGEN.INDEX_SNAPSHOT_MANIFEST contains expected SNAPSHOT_ID entries, and trigger container reload via DOCGEN.REGISTER_INDEX_SNAPSHOT after running external FAISS builder. @6 @22
5) Security checks: verify masking policies hide SIGNER_EMAIL for non-privileged roles and row access policies enforce tenant isolation. @86 @371

-- End of generated artifacts for this continuation.

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.

- Anara: Continuing generation: emitting additional Snowflake-side artifacts for the E‑Signature module (retention/export stored-procs, purge jobs, HSM-sign stub + External Function registration, Streamlit monitor, alerting views/tasks and smoke tests) that follow the Snowpark, External Function, FAISS snapshot and registration patterns in your design materials @31 @26 @4.  
These files are provided with demo/test environment values so they are runnable in a dev account and match the staging/PUT→CREATE PROCEDURE registration flow used in the bundle runbooks @30.

