Purpose: maintain a fast preflight entitlement cache (materialized view refresh pattern) used by Admin/UI and preflight checks @31 @11.
# update_entitlement_cache.py
from snowflake.snowpark import Session
def update_entitlement_cache(session: Session):
    session.sql("""
        CREATE OR REPLACE MATERIALIZED VIEW DOCGEN.MV_ENTITLEMENT_CACHE AS
        SELECT a.ACCOUNT_ID, f.FEATURE_KEY, f.ENABLED, f.MODIFIED_AT
        FROM DOCGEN.ACCOUNT_FEATURE_ENTITLEMENTS f
        JOIN DOCGEN.ACCOUNTS a ON a.ACCOUNT_ID = f.ACCOUNT_ID;
    """).collect()
    return {"status":"ok"}

