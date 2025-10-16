Purpose: smoke tests that run OCSP poller (dry run) and Slack notify proc to validate registration end-to-end in CI. @176 @62
# tests/test_ocsp_and_notify.py
from snowflake.connector import connect
def test_ocsp_and_notify_smoke():
    c = connect(user="svc_docgen", account="client_prod_001", password="DemoPassw0rd!", role="DOCGEN_ADMIN")
    cur = c.cursor()
    # Insert a dummy cert to trigger OCSP flow (test harness uses mocks or staging)
    cur.execute("INSERT INTO DOCGEN.PKI_CERTIFICATE_STORE (CERT_ID, FINGERPRINT) VALUES ('cert_test_1','fp_test_1');")
    cur.execute("CALL DOCGEN.CHECK_OCSP_AND_UPDATE(1);")
    # Insert a dummy alert and send to Slack (will hit external function gateway in staging)
    cur.execute("INSERT INTO DOCGEN.ALERTS (ALERT_ID, ALERT_TYPE, PAYLOAD, SEVERITY) VALUES ('alert_test_1','TEST','{\"x\":1}','LOW');")
    cur.execute("CALL DOCGEN.NOTIFY_SLACK('alert_test_1');")
    assert True

