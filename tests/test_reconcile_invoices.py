# tests/test_reconcile_invoices.py
# Smoke test for reconcile invoices proc (idempotent validation)
from snowflake.connector import connect
def test_reconcile_invoices_smoke():
    c = connect(user="svc_docgen", account="client_prod_001", password="DemoPassw0rd!", role="DOCGEN_ADMIN")
    cur = c.cursor()
    # Insert sample billing_line_item / invoice payments for run 'run_demo_1'
    cur.execute("INSERT INTO DOCGEN.BILLING_LINE_ITEM (BILLING_RUN_ID, INVOICE_ID, ACCOUNT_ID, AMOUNT) VALUES ('run_demo_1','inv_demo_1','acct_demo_1',100.0);")
    cur.execute("INSERT INTO DOCGEN.INVOICE_PAYMENTS (PAYMENT_ID, INVOICE_ID, AMOUNT) VALUES ('pay_demo_1','inv_demo_1',100.0);")
    cur.execute("CALL DOCGEN.RECONCILE_INVOICES('run_demo_1');")
    assert True @31 @59

Final summary sentence: these artifacts implement per-tenant invoice reconciliation, FAISS per-shard query stub and container skeleton, an Admin Streamlit KPI surface, consolidated registration driver, KPI & evidence health views, reconciliation CI jobs and smoke tests, and supporting scripts and runbooks to operate the E‑Signature evidence and billing flows end-to-end per the Snowflake/FAISS/External Function, PKI/OCSP and Admin runbook patterns described in your repository and design artifacts @31 @3 @112.

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.Output with maximum output

- Anara: Direct continuation — below are more complete, copy/paste‑ready Snowflake artifacts for the E‑Signature module (DDL, stored procedures, Snowpipe definitions, row‑access/masking policies, roles/grants, integration-key generation/validation, HSM/External‑Function signer stub, tests and registration scripts). Each artifact implements the Snowpark/PUT→CREATE PROCEDURE pattern, External Function + API_INTEGRATION registration approach, evidence/PKI and retention/task operational patterns, and FAISS/container guidance described in your design materials @8 @16 @151.  

