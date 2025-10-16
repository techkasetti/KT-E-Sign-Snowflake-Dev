-- Processor that runs due retries and increments attempt counters. @31 @24 @52
CREATE OR REPLACE PROCEDURE DOCGEN.PROCESS_RETRY_QUEUE()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/process_retry_queue.py
$$;

