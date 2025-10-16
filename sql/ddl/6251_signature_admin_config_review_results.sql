USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.CONFIG_REVIEW_RESULTS ( R_ID STRING PRIMARY KEY, Q_ID STRING, OUTCOME JSON, REVIEWED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/6252_signature_onchain_anchor_index.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.ONCHAIN_ANCHOR_INDEX ( ANCHOR_ID STRING PRIMARY KEY, BUNDLE_ID STRING, CHAIN STRING, TX_HASH STRING, ANCHORED_AT TIMESTAMP_LTZ ); @31 @24
