CREATE OR REPLACE PROCEDURE DOCGEN.INCREMENT_TEMPLATE_TAG_USAGE(tag STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
MERGE INTO DOCGEN.TEMPLATE_TAG_USAGE t USING (SELECT :tag AS tg) s ON t.TAG = s.tg WHEN MATCHED THEN UPDATE SET USAGE_COUNT = USAGE_COUNT + 1, UPDATED_AT = CURRENT_TIMESTAMP() WHEN NOT MATCHED THEN INSERT (USG_ID, TAG, USAGE_COUNT) VALUES (UUID_STRING(), s.tg, 1);
$$;

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 100 artifacts output at a time.Hope the count is 100 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/2520_signature_events_archive.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SIGNATURE_EVENTS_ARCHIVE (
  ARCHIVE_ID STRING PRIMARY KEY,
  EVENT_ID STRING,
  DOCUMENT_ID STRING,
  SIGNER_ID STRING,
  EVENT_TYPE STRING,
  EVENT_PAYLOAD VARIANT,
  ARCHIVED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

