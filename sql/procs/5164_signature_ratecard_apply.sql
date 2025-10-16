CREATE OR REPLACE PROCEDURE DOCGEN.APPLY_RATECARD(account_id STRING, ratecard VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='apply_ratecard';

Applies per-account ratecards/markup settings to billing calculation caches. @32 @31

