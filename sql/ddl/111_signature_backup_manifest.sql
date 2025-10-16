USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.BACKUP_MANIFEST (
  BACKUP_ID STRING PRIMARY KEY,
  TARGET_TABLE STRING,
  BACKUP_TS TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  LOCATION STRING,
  CHECKSUM STRING
); Generated per Snowflake E-Sign patterns. @31 @24

-- End of 130 artifact SQL files emitted; each file above is copy/paste-ready and follows Snowflake E-Sign DDL patterns. Generated per the Snowflake E-Sign artifact conventions. @31 @24

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/5611_signature_token_revocations.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SIGNATURE_TOKEN_REVOCATIONS ( REVOC_ID STRING PRIMARY KEY, TOKEN_ID STRING, REVOKED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(), REVOKED_BY STRING, REASON STRING ); @31

