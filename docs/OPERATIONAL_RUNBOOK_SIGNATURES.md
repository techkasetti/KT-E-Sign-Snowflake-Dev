# Operational Runbook — E-Signature Module (Dev/Stage)
1) Deploy DDL: run sql/ddl/signature_domain_schema.sql
2) Upload Python procs: run register/register_signature_procs.sh
3) Seed demo data: run snowsql -f sql/seed/sample_signature_seed.sql
4) Enable tasks: ALTER TASK DOCGEN.TASK_INGEST_SIGNATURE_EVENTS RESUME; ALTER TASK DOCGEN.TASK_SIGNATURE_ANALYTICS RESUME;
5) To ingest payloads: upload JSONL to S3 path s3://docgen-ingest-bucket/events/ then Snowpipe will COPY INTO DOCGEN.SIGNATURE_EVENTS_RAW
6) Monitor: query DOCGEN.V_SIGNATURE_COMPLETION_KPI, DOCGEN.ALERTS, or run the streamlit app streamlit/monitor/app.py
7) Purge: CALL DOCGEN.PURGE_OLD_DOCUMENTS(365, FALSE) (after review)
8) Evidence export: CALL DOCGEN.EXPORT_EVIDENCE_TO_S3('evidence-export', 365)

