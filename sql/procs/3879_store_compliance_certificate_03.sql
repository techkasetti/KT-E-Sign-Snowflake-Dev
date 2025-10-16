CREATE OR REPLACE PROCEDURE DOCGEN.STORE_COMPLIANCE_CERTIFICATE_03(bundle_id STRING, cert_chain VARIANT, ocsp_snapshot VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.COMPLIANCE_CERTIFICATES_03 (CERT_ID, BUNDLE_ID, CERT_CHAIN, OCSP_SNAPSHOT) VALUES (UUID_STRING(), :bundle_id, :cert_chain, :ocsp_snapshot);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/3880_signature_notifications_04.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SIGNATURE_NOTIFICATIONS_04 ( NOTIFY_ID STRING PRIMARY KEY, BUNDLE_ID STRING, CHANNEL STRING, PAYLOAD VARIANT, SENT_AT TIMESTAMP_LTZ, STATUS STRING, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

