CREATE OR REPLACE PROCEDURE DOCGEN.AGGREGATE_SIGNATURE_USAGE(window_hours INT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='aggregate_signature_usage';

Scheduled proc to aggregate usage into billing-friendly line items. @29 @31

