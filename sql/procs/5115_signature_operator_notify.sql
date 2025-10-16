CREATE OR REPLACE PROCEDURE DOCGEN.NOTIFY_OPERATORS(payload VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='notify_operators';

Sends operator notifications (Slack/email) when high-severity incidents or anomalies are detected. @65 @31

