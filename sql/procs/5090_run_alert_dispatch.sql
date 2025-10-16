CREATE OR REPLACE PROCEDURE DOCGEN.RUN_ALERT_DISPATCH(limit INT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='run_alert_dispatch';

Alert dispatch runner that queries integration_health and anomaly detectors. @62 @31

