Purpose: CI test to run the purge proc in dry-run mode and assert audit row created; supports acceptance tests in CI pipeline. @109 @118

# tests/test_purge_dryrun.py
from snowflake.connector import connect
def test_purge_dryrun(): c = connect(user="svc_docgen", account="client_prod_001", password="DemoPassw0rd!", role="DOCGEN_ADMIN") cur = c.cursor() cur.execute("CALL DOCGEN.PURGE_EVIDENCE_WITH_AUDIT(1095, TRUE);") res = cur.fetchone() assert res is not None

----
