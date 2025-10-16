-- Simple alert detector view to surface OCSP failures or revocations for ops to monitor; tied to operational runbooks. @30 @28
CREATE OR REPLACE VIEW DOCGEN.V_OCSP_FAILS AS
SELECT CERT_ID, OCSP_RESPONSE FROM DOCGEN.SIGNATURE_CERTIFICATES WHERE OCSP_RESPONSE:status != 'GOOD';

