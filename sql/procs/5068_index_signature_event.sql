PUT file://sql/procs/index_signature_event.py @~/procedures/ AUTO_COMPRESS=TRUE;
CREATE OR REPLACE PROCEDURE DOCGEN.INDEX_SIGNATURE_EVENT(sei_id STRING, event_ref STRING, event_type STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
IMPORTS = ('@~/procedures/index_signature_event.py')
HANDLER = 'index_signature_event';

Indexing worker to populate signature_event_index and support search. @132 @31

