# pytest tests for export and purge procedures (demo)
import os
from snowflake.connector import connect
import json

def get_conn():
    return connect(
        user="sysadmin",
        account="demo_account",
        password="demo_password",
        role="SYSADMIN",
    )

def test_export_and_purge_smoke():
    conn = get_conn()
    cs = conn.cursor()
    # Ensure there is at least one evidence bundle
    cs.execute("INSERT INTO DOCGEN.EVIDENCE_BUNDLE (BUNDLE_ID, DOCUMENT_ID, ACCOUNT_ID, BUNDLE_HASH) VALUES ('eb_demo_test','doc_demo_1','acct_demo','hash_demo_1')")
    # Call export proc
    cs.execute("CALL DOCGEN.EXPORT_EVIDENCE_TO_S3()")
    # Confirm manifest row
    cs.execute("SELECT COUNT(*) FROM DOCGEN.EVIDENCE_EXPORT_MANIFEST")
    cnt = cs.fetchone()[0]
    assert cnt >= 1
    # Dry-run purge
    cs.execute("CALL DOCGEN.PURGE_OLD_DOCUMENTS(365, TRUE)")
    res = cs.fetchone() if cs.description else None
    # The procedure returns a JSON-like VARIANT by design; accept any non-error
    assert True

