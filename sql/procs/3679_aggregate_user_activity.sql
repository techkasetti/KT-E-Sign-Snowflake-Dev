CREATE OR REPLACE PROCEDURE DOCGEN.AGGREGATE_USER_ACTIVITY(user_ref STRING, activity_counts VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
INSERT INTO DOCGEN.USER_ACTIVITY_AGGREGATES (AGG_ID, USER_REF, ACTIVITY_COUNTS) VALUES (UUID_STRING(), :user_ref, :activity_counts);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/3680_signature_user_session_tokens_archive.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.USER_SESSION_TOKENS_ARCHIVE ( TOKEN_ID STRING PRIMARY KEY, SESSION_ID STRING, USER_REF STRING, EXPIRED_AT TIMESTAMP_LTZ, ARCHIVED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );
-- @1

