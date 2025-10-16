CREATE OR REPLACE PROCEDURE DOCGEN.RECONCILE_OPERATOR_ACTIONS()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='reconcile_operator_actions';

Reconciles operator action logs against incidents and runbooks. @344 @31

