-- PUT file://snowpark/procedures/detect_signature_anomalies.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.DETECT_SIGNATURE_ANOMALIES()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
IMPORTS = ('@~/procedures/detect_signature_anomalies.py')
HANDLER = 'detect_signature_anomalies';
-- Register anomaly detection SP per task orchestration patterns @31 @2363

