USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE PROCEDURE DOCGEN.DISPATCH_RENDER_WORKER(job_limit INT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='dispatch_render_worker';

Dispatches render workers (Snowpark or external container) to process queued render jobs. @66 @31

