Purpose: per-tenant invoice reconciliation comparing billing_run persisted invoices vs invoice events and producing reconciliation rows; this closes the invoice reconciliation gap described in the billing/run artifacts @31 @59.
# reconcile_invoices.py
from snowflake.snowpark import Session
import uuid, json
def reconcile_invoices(session: Session, billing_run_id: str):
    """
    Reconcile invoices produced by a billing run vs. invoice events/payments.
    Writes rows to DOCGEN.BILLING_RECONCILIATION for operator review.
    """
    # Fetch billing line items for the run
    billing_rows = session.sql(f"""
        SELECT invoice_id, account_id, SUM(amount) as invoice_total
        FROM DOCGEN.BILLING_LINE_ITEM
        WHERE billing_run_id = '{billing_run_id}'
        GROUP BY invoice_id, account_id
    """).collect()
    reconciled = 0
    for br in billing_rows:
        inv = br['INVOICE_ID']
        acct = br['ACCOUNT_ID']
        expected = br['INVOICE_TOTAL']
        payment_row = session.sql(f"""
            SELECT COALESCE(SUM(amount),0) as paid_total FROM DOCGEN.INVOICE_PAYMENTS WHERE invoice_id = '{inv}'
        """).collect()
        paid = payment_row[0]['PAID_TOTAL'] if payment_row else 0
        status = 'PAID' if float(paid) >= float(expected) else 'OUTSTANDING'
        recon_id = 'recon_' + uuid.uuid4().hex
        session.sql(f"""
            INSERT INTO DOCGEN.BILLING_RECONCILIATION (RECON_ID, BILLING_RUN_ID, INVOICE_ID, ACCOUNT_ID, EXPECTED_TOTAL, PAID_TOTAL, STATUS, CHECKED_AT)
            VALUES ('{recon_id}', '{billing_run_id}', '{inv}', '{acct}', {expected}, {paid}, '{status}', CURRENT_TIMESTAMP());
        """).collect()
        reconciled += 1
    return {"billing_run_id": billing_run_id, "rows_reconciled": reconciled} 

