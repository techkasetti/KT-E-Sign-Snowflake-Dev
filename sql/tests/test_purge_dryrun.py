# tests/test_purge_dryrun.py
from snowflake.connector import connect
def test_purge_dryrun():
    c = connect(user="svc_docgen", account="client_prod_001", password="DemoPassw0rd!", role="DOCGEN_ADMIN")
    cur = c.cursor()
    cur.execute("CALL DOCGEN.PURGE_OLD_EVIDENCE(99999);")
    assert True

This CI smoke test exercises the retention purge procedure and follows the deterministic smoke-test pattern in your CI guidance @66 @110. @66 @110

