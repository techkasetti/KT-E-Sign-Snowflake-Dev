CREATE OR REPLACE PROCEDURE DOCGEN.WRITE_OPERATOR_ACTION(operator STRING, action STRING, target STRING, payload VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='write_operator_action';

Procedure to persist operator actions and correlate with incident reports. @344 @31

