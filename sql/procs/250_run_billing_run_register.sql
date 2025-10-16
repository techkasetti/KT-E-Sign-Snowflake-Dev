-- Registration SQL for RUN_BILLING_RUN that commits a billing preview into BILLING_RUN/BILLING_LINE_ITEM; follows the PUT/CREATE pattern. @29 @49
CREATE OR REPLACE PROCEDURE DOCGEN.RUN_BILLING_RUN(account_id STRING, run_date TIMESTAMP_LTZ, preview_hash STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
PACKAGES=('snowflake-snowpark-python')
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/run_billing_run.py
$$;

