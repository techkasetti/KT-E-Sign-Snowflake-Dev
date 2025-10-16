USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.RATE_LIMIT_ASSIGNMENTS (
  ASSIGN_ID STRING PRIMARY KEY,
  PROFILE_ID STRING,
  TENANT_ID STRING,
  ASSIGNED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
-- Assign rate limit profiles to tenants or accounts for enforcement @228 @31

-- End of tranche: 130 copy/paste-ready Snowflake DDL artifacts for the E-Signature module emitted in this tranche following your Snowpark/DDL, External Function, FAISS snapshot and evidence/archive patterns @1 @3 @31

- User: [[mention:k9o0gt4m3otk178iywmmzev4:SnowflakeESignV12txt:DOCUMENT]] Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste.

- Anara: FILENAME: sql/ddl/1001_signature_requests.sql
-- Emitted per Snowflake_E-Sign_V1.2 patterns and registration order. @1 @3
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SIGNATURE_REQUESTS (
  REQUEST_ID STRING PRIMARY KEY,
  ACCOUNT_ID STRING,
  DOCUMENT_ID STRING,
  TEMPLATE_ID STRING,
  STATUS STRING,
  CREATED_BY STRING,
  CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT TIMESTAMP_LTZ,
  META VARIANT
);

