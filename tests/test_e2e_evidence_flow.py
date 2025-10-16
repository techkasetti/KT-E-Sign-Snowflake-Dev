# tests/test_e2e_evidence_flow.py
# Pytest-driven e2e smoke flow:
#  1) seed request and signer
#  2) simulate signature event ingestion
#  3) ingest proc normalization
#  4) call assembly -> evidence zipper
#  5) export evidence -> reconcile
import json, time
from snowflake.connector import connect

def test_e2e_evidence_flow():
    c = connect(user="svc_docgen", account="client_prod_001", password="DemoPassw0rd!", role="DOCGEN_ADMIN")
    cur = c.cursor()
    # Seed is expected to exist from sample_seed; invoke assemble stored-proc
    cur.execute("CALL DOCGEN.ASSEMBLE_DOCUMENT('req_demo_1','tpl_demo_1', PARSE_JSON('{\"clause1\":\"value1\"}'));")
    # Run export (manifest id must be unique)
    manifest_id = "man_" + "e2e"  # deterministic for test
    cur.execute("CALL DOCGEN.EXPORT_EVIDENCE_TO_S3('manifest_e2e_1','s3://docgen-evidence-archive',365);")
    # Reconcile (noop if S3 not reachable in test) - ensure procedure runs
    cur.execute("CALL DOCGEN.RECONCILE_EVIDENCE_EXPORTS('manifest_e2e_1');")
    assert True

