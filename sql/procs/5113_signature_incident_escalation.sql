CREATE OR REPLACE PROCEDURE DOCGEN.ESCALATE_INCIDENT(incident_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='escalate_incident';

Escalation workflow invoked by incident response task. @105 @31

