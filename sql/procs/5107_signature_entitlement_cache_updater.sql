CREATE OR REPLACE PROCEDURE DOCGEN.REFRESH_ENTITLEMENT_CACHE(account_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='refresh_entitlement_cache';

Refreshes per-account entitlement cache used by preflight checks for performance. @37 @31

