CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_ARCHIVE_REFERENCE(original_ref STRING, archive_location STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.ARCHIVE_REFERENCES (REF_ID, ORIGINAL_REF, ARCHIVE_LOCATION) VALUES (UUID_STRING(), :original_ref, :archive_location);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 100 artifacts output at a time.Hope the count is 100 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/2920_signature_policy_versions.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.POLICY_VERSIONS ( VERSION_ID STRING PRIMARY KEY, POLICY_ID STRING, VERSION_NUMBER INT, CONTENT VARIANT, CREATED_BY STRING, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
