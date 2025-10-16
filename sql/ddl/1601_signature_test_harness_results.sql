CREATE OR REPLACE TABLE DOCGEN.TEST_HARNESS_RESULTS ( RES_ID STRING PRIMARY KEY, RUN_ID STRING, FINDINGS VARIANT, COMPLETED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() ); @31 @24

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 200 artifacts output at a time.Hope the count is 200 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/1602_signature_session_fragment_index.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SESSION_FRAGMENT_INDEX ( FRAG_ID STRING PRIMARY KEY, SESSION_ID STRING, SEQ INT, PAYLOAD VARIANT, CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

