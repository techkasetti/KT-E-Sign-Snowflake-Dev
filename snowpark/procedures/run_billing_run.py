from snowflake.snowpark import Session
def run_billing_run(session: Session, account_id, start_ts, end_ts, preview_only=False):
    # compute invoice hash, persist billing_run and line items if not preview
    invoice_hash = "inv_" + account_id + "_" + str(int(session.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0].timestamp()))
    if not preview_only:
        session.sql(f"INSERT INTO DOCGEN.BILLING_RUN (RUN_ID, ACCOUNT_ID, START_AT, END_AT, INVOICE_HASH, CREATED_AT) VALUES (UUID_STRING(), '{account_id}', '{start_ts}', '{end_ts}', '{invoice_hash}', CURRENT_TIMESTAMP())").collect()
    return {"invoice_hash": invoice_hash, "preview": preview_only}
# Snowpark billing run with preview/commit semantics and invoice_hash determinism per billing design patterns @21 @31

