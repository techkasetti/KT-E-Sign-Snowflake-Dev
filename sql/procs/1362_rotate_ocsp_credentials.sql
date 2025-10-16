-- Placeholder proc to indicate rotation workflow for OCSP/HSM credentials must be managed in CI/secret store @1 @10.
CREATE OR REPLACE PROCEDURE DOCGEN.ROTATE_OCSP_CREDENTIALS()
RETURNS VARIANT
LANGUAGE SQL
AS
$$ RETURN OBJECT_CONSTRUCT('status','ok'); $$;

