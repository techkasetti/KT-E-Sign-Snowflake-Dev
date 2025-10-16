PUT file://sql/procs/run_signature_scoring.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.RUN_SIGNATURE_SCORING()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
IMPORTS = ('@~/procedures/run_signature_scoring.py')
HANDLER = 'run_signature_scoring';

Runs ML scoring pipelines (embedding + features) and writes score outputs to REPORTING_METRICS. @500 @31

