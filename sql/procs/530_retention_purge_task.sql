-- Retention enforcement stored-proc to purge/archive evidence bundles past retention as described in runbooks. @30 @28
CREATE OR REPLACE PROCEDURE DOCGEN.ENFORCE_EVIDENCE_RETENTION(retention_days INT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
  DELETE FROM DOCGEN.SIGNATURE_EVIDENCE_BUNDLE WHERE CREATED_AT < DATEADD(day, -:retention_days, CURRENT_TIMESTAMP());
$$;

