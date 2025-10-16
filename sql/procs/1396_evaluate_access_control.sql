-- Evaluate access control policies before allowing sensitive operations like export or template edits @1 @6.
CREATE OR REPLACE PROCEDURE DOCGEN.EVALUATE_ACCESS_CONTROL(resource STRING, context VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/evaluate_access_control.py
$$;

