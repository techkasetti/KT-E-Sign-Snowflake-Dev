# ocsp_probe.py
from snowflake.snowpark import Session
import requests, json
def ocsp_probe(session: Session, fingerprint: str):
    # Proxy call to external OCSP gateway (via External Function from Snowflake) or return simulated response
    res = session.sql(f"SELECT DOCGEN.OCSP_CHECK(PARSE_JSON('{{\"fingerprint\":\"{fingerprint}\"}}')) AS resp").collect()
    return res[0]['RESP'] if res else {"status":"UNKNOWN"}

This proc delegates OCSP checks to a registered External Function as recommended by the HSM/OCSP integration guidance @72 @74. @72 @74

