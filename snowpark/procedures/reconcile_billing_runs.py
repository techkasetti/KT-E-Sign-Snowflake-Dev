Purpose: Compare preview invoice_hash values with committed billing runs and record mismatches for audit and remediation. @21 @29

# reconcile_billing_runs.py
from snowflake.snowpark import Session
import uuid, json

def reconcile_billing_runs(session: Session, account_id: str, period_start: str, period_end: str):
    """
    Compute preview invoice_hash and compare against latest committed billing run for the period.
    Record reconciliation run and any mismatches into DOCGEN.BILLING_RECONCILIATION_* tables.
    """
    preview = session.call("DOCGEN.PREVIEW_BILLING", [account_id, period_start, period_end])
    preview_hash = preview.get('invoice_hash')
    recon_id = "recon_" + str(uuid.uuid4())
    # Find committed billing_run for this account & period
    rows = session.sql(f"""
        SELECT BILLING_RUN_ID, PREVIEW_HASH, TOTAL_AMOUNT FROM DOCGEN.BILLING_RUN
        WHERE ACCOUNT_ID = '{account_id}' AND RUN_PERIOD_START = '{period_start}' AND RUN_PERIOD_END = '{period_end}' AND STATUS = 'COMMITTED'
        ORDER BY CREATED_AT DESC LIMIT 1
    """).collect()

    if not rows:
        session.sql(f"""
            INSERT INTO DOCGEN.BILLING_RECONCILIATION_RUN (RECONC_RUN_ID, ACCOUNT_ID, RUN_TS, EXPECTED_INVOICE_HASH, ACTUAL_INVOICE_HASH, STATUS)
            VALUES ('{recon_id}', '{account_id}', CURRENT_TIMESTAMP(), '{preview_hash}', NULL, 'NO_COMMITTED_RUN')
        """).collect()
        return {"status": "no_committed_run", "preview_hash": preview_hash}

    committed = rows[0]
    billing_run_id = committed['BILLING_RUN_ID']
    actual_hash = committed['PREVIEW_HASH']
    status = 'MATCH' if actual_hash == preview_hash else 'MISMATCH'
    session.sql(f"""
        INSERT INTO DOCGEN.BILLING_RECONCILIATION_RUN (RECONC_RUN_ID, ACCOUNT_ID, RUN_TS, EXPECTED_INVOICE_HASH, ACTUAL_INVOICE_HASH, STATUS)
        VALUES ('{recon_id}', '{account_id}', CURRENT_TIMESTAMP(), '{preview_hash}', '{actual_hash}', '{status}')
    """).collect()

    if status == 'MISMATCH':
        # Persist mismatch detail (simple differencing: store both hashes)
        mismatch_id = "mm_" + str(uuid.uuid4())
        session.sql(f"""
            INSERT INTO DOCGEN.BILLING_RECONCILIATION_MISMATCH (MISMATCH_ID, RECONC_RUN_ID, BILLING_RUN_ID, FIELD_NAME, EXPECTED_VAL, ACTUAL_VAL)
            VALUES ('{mismatch_id}', '{recon_id}', '{billing_run_id}', 'invoice_hash', PARSE_JSON('"{preview_hash}"'), PARSE_JSON('"{actual_hash}"'));
        """).collect()
    return {"status": status, "preview_hash": preview_hash, "actual_hash": actual_hash}

