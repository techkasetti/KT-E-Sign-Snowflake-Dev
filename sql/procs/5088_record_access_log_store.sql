CREATE OR REPLACE PROCEDURE DOCGEN.STORE_ACCESS_LOGS_BATCH(batch VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='store_access_logs_batch';

Batch writer optimized for CI/test harness and high-throughput logging. @125 @31

