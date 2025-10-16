CREATE OR REPLACE PROCEDURE DOCGEN.SEND_NOTIFY_TEMPLATE(template_id STRING, target VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='send_notify_template';

Sends notification templates to targets via the notification worker. @24 @31

