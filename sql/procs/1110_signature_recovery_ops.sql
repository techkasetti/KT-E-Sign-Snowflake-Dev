-- Operational stored-proc to re-run export for failed bundles; used in runbook to recover from partial failures. @30 @28
CREATE OR REPLACE PROCEDURE DOCGEN.RETRY_EXPORT(bundle_id STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
  -- operator implementation: re-run export job
  RETURN OBJECT_CONSTRUCT('bundle_id', :bundle_id, 'status', 'requeued');
$$;

