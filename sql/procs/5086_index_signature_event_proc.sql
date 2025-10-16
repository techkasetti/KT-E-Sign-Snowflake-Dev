CREATE OR REPLACE PROCEDURE DOCGEN.INDEX_SIGNATURE_EVENT_PROC(sei_id STRING, event_ref STRING, event_type STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
IMPORTS = ('@~/procedures/index_signature_event.py')
HANDLER = 'index_signature_event';

Indexing stored proc to populate event indexes used by operator queries. @132 @31

