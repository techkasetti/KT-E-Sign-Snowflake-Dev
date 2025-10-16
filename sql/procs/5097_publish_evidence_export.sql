CREATE OR REPLACE PROCEDURE DOCGEN.PUBLISH_EVIDENCE_EXPORT(bundle_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='publish_evidence_export';

Procedure to orchestrate CompliancePacket packaging and export to object storage. @36 @113

