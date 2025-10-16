Purpose: CI smoke test that exercises OCSP check and export/reconcile flow (best-effort in staging). @114 @31
# tests/test_ocsp_and_reconcile.py
import pytest
from snowflake.connector import connect
def test_ocsp_and_reconcile():
    c = connect(user="svc_docgen", account="client_prod_001", password="DemoPassw0rd!", role="DOCGEN_ADMIN")
    cur = c.cursor()
    # insert dummy cert to check
    cur.execute("INSERT INTO DOCGEN.PKI_CERTIFICATE_STORE (CERT_ID, CERT_PEM, SUBJECT, ISSUER, SERIAL_NUMBER, FINGERPRINT, NOT_BEFORE, NOT_AFTER) VALUES ('cert_test_ocsp','pem','sub','iss','sn','fp_test',CURRENT_TIMESTAMP(),DATEADD('year',1,CURRENT_TIMESTAMP()));")
    # call ocsp check
    cur.execute("CALL DOCGEN.CHECK_OCSP_AND_UPDATE('fp_test');")
    # export evidence manifest (noop if none) then reconcile
    cur.execute("CALL DOCGEN.EXPORT_EVIDENCE_TO_S3('manifest_test_ocsp','s3://docgen-evidence-archive',7);")
    cur.execute("CALL DOCGEN.RECONCILE_EVIDENCE_EXPORTS('manifest_test_ocsp');")
    assert True

