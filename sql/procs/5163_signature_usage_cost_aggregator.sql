CREATE OR REPLACE PROCEDURE DOCGEN.AGGREGATE_USAGE_COSTS()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='aggregate_usage_costs';

Aggregates usage metrics into billing-ready cost summaries. @29 @31

