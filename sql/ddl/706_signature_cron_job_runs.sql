USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.CRON_JOB_RUNS ( RUN_ID STRING PRIMARY KEY, JOB_ID STRING, STATUS STRING, STARTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(), ENDED_AT TIMESTAMP_LTZ, OUTPUT VARIANT ); @31

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: -- These DDL artifacts follow the Snowflake E-Sign delivery and registration patterns in your workspace @31

