USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.OPERATOR_SHIFT_COMPLIANCE_REPORTS ( REPORT_ID STRING PRIMARY KEY, ROSTER_ID STRING, PERIOD_START TIMESTAMP_LTZ, PERIOD_END TIMESTAMP_LTZ, METRICS VARIANT, GENERATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() )

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/9001_signature_sync_audit_queue.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SYNC_AUDIT_QUEUE ( QUEUE_ID STRING PRIMARY KEY, OBJECT_REF STRING, ACTION STRING, PAYLOAD VARIANT, ENQUEUED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31

