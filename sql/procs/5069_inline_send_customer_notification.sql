CREATE OR REPLACE PROCEDURE DOCGEN.SEND_CUSTOMER_NOTIFICATION(cn_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='send_customer_notification';

Inline notification sender stub intended to call External Function email/SMS providers. @109 @31

