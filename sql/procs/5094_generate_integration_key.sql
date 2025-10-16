USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE PROCEDURE DOCGEN.GENERATE_INTEGRATION_KEY(account_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='generate_integration_key';

Generates show-once integration keys and records hashed value for security patterns. @31 @43

