USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SYSTEM_RESTORE_RESULTS ( RES_ID STRING PRIMARY KEY, JOB_ID STRING, STATUS STRING, DETAILS VARIANT, COMPLETED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/651_signature_provider_billing_invoices.sql
USE DATABASE AI_FEATURE_HUB; USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.PROVIDER_BILLING_INVOICES ( INVOICE_ID STRING PRIMARY KEY, PROVIDER_ID STRING, RUN_ID STRING, AMOUNT NUMBER, DUE_DATE TIMESTAMP_LTZ ); @31

