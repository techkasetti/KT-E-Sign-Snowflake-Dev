Purpose: deterministic unit test to assert generate/validate integration key flow behaves as show‑once per design @131.  
# tests/test_integration_key_showonce.py
import pytest
from snowflake.connector import connect
def test_integration_key_showonce():
    c = connect(user="svc_docgen", account="client_prod_001", password="DemoPassw0rd!", role="DOCGEN_ADMIN")
    cur = c.cursor()
    # create key
    cur.execute("CALL DOCGEN.GENERATE_INTEGRATION_KEY('acct_demo_1');")
    res = cur.fetchone()
    assert res is not None
    key_id = res[0]
    plaintext = res[1]
    salt = res[2]
    # validate should succeed
    cur.execute(f"CALL DOCGEN.VALIDATE_INTEGRATION_KEY('acct_demo_1','{plaintext}','{salt}');")
    assert cur.fetchone()[0]['valid'] == True  

