CREATE OR REPLACE PROCEDURE DOCGEN.GENERATE_DAILY_REPORTS()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='generate_daily_reports';

Generates daily signatures/billing/compliance reports and stores results in REPORTING_JOB_RESULTS. @116 @31

