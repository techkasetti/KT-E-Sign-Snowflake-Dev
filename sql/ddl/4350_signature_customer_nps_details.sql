USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.CUSTOMER_NPS_DETAILS (
  DETAIL_ID STRING PRIMARY KEY,
  RUN_ID STRING,
  RESPONDER STRING,
  RESPONSE JSON,
  RESPONDED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
-- Generated per Snowflake E-Sign DDL patterns. @31 @24

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 150 artifacts generation / output to screen at a time.Hope the count is 150 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: Emitting next 150 Snowflake DDL artifacts (copy/paste-ready) following the Snowpark / External Function / evidence & PKI patterns in your workspace @31 @24 @56

