# tests/test_assembly_and_write.py
# Simulate assembly service calling WRITE_EVIDENCE_BUNDLE via Snowflake
from snowflake.connector import connect
import base64, json
def test_assembly_and_write():
    c = connect(user="svc_docgen", account="client_prod_001", password="DemoPassw0rd!", role="DOCGEN_ADMIN")
    cur = c.cursor()
    # Simulate assembly call: call WRITE_EVIDENCE_BUNDLE with sample params
    cur.execute("CALL DOCGEN.WRITE_EVIDENCE_BUNDLE('assembly_test_1','doc_test_1','s_demo_1','sighash_test', PARSE_JSON('[\"cert1\",\"cert2\"]'),'s3://docgen-evidence-archive/doc_test_1.pdf', PARSE_JSON('{\"note\":\"smoke\"}'))")
    res = cur.fetchone()
    assert res is not None

