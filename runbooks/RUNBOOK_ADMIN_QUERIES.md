# Admin query runbook (demo)
1) Recent signers: run queries/sample_admin_queries.sql and review pending signers.  
2) Evidence bundle validation: query DOCGEN.EVIDENCE_BUNDLE for bundles and cross-check bundle_hash values.  
3) Billing reconciliation: run DOCGEN.RECONCILE_BILLING_RUNS('acct_demo','2024-01-01','2024-01-31') and inspect DOCGEN.BILLING_RECONCILIATION_MISMATCH for details.

