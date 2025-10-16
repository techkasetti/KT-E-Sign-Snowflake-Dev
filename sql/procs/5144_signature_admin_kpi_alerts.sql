CREATE OR REPLACE PROCEDURE DOCGEN.CHECK_ADMIN_KPI_ALERTS()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='check_admin_kpi_alerts';

Checks KPIs against thresholds and produces alerts when SLOs approach violation. @344 @31

