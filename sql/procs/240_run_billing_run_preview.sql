-- PREVIEW_BILLING stored proc returns deterministic line_items + invoice_hash for preview per billing design. @49 @31
CREATE OR REPLACE PROCEDURE DOCGEN.PREVIEW_BILLING(account_id STRING, preview_date TIMESTAMP_LTZ)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
PACKAGES=('snowflake-snowpark-python')
HANDLER='handler'
AS
$$
# staged preview handler at @~/procedures/preview_billing.py
$$;

