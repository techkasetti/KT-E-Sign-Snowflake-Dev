# RUNBOOK: Deploy Snowflake E‑Signature + Admin Backend
1) Required env / secrets (store in CI secrets or vault):
   - SNOW_ACCOUNT, SNOW_USER, SNOW_PWD, SNOW_ROLE
   - STORAGE_INTEGRATION, YOUR_S3_BUCKET
   - API_AWS_ROLE_ARN, API_GATEWAY_HOST
   - INTEGRATION_KDF_SECRET (KMS or CI secret)
   - Container registry credentials for FAISS image
2) Apply DDL:
   snowsql -a <account> -u <user> -f sql/ddl/ai_feature_hub_schema.sql
   snowsql -a <account> -u <user> -f sql/ddl/signature_domain_schema.sql
   snowsql -a <account> -u <user> -f sql/attach_policies_and_grants.sql
3) Configure storage integration and cloud resources (S3 bucket, role).
4) Put Python files to user stage:
   snowsql -a <account> -u <user> -q "PUT file://snowpark/procedures/*.py @~/procedures/ AUTO_COMPRESS=TRUE;"
5) Run register/register_all_procs.sh to CREATE PROCEDUREs.
6) Register External Functions:
   snowsql -a <account> -u <user> -f sql/external_functions/external_functions_register.sql
7) Create Snowpipe stages/pipes:
   snowsql -a <account> -u <user> -f sql/snowpipe/snowpipe_setup.sql
8) Build/push FAISS container and run infra/deploy_snowpark_container.sh
9) Run tests:
   pytest -q tests/test_signature_flow.py
10) Validate:
   - Confirm DOCGEN.SIGNATURE_VALIDATION rows exist after VERIFY_SIGNATURE.
   - Confirm DOCGEN.EVIDENCE_BUNDLE rows exist after WRITE_EVIDENCE_BUNDLE.
Operational security notes:
- Do not embed credentials in repo or container images.
- Use KMS/HSM for production-level key management and HSM integration for QES-level signing.
- Use masking policies for PII columns and restrict roles to least privilege.

--- End of file set ---

Notes & next steps:
- These files implement the Snowflake-side domain model and processes required to operate a standalone backend for E‑Signature + Admin and include test harnesses and CI to validate ingestion → verification → archival flows. @31 @16
- Replace placeholders and supply environment secrets in CI before running any registration or deployment steps. @18
- If you want, I can now (A) package this file set as a ZIP in the workspace, (B) write them into a workspace note, or (C) emit a single apply_patch.diff / unified diff for git apply; tell me which format to produce next.

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.

- Anara: I will now emit full-implementation Snowflake artifacts populated with non-production demo values (replace them before deploying); these artifacts follow the Snowpark stored-proc, External Function/API_INTEGRATION, FAISS snapshot/container, and security patterns in your documents. @31 @42 @26

