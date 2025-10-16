Purpose: CI SQL snippet to run billing reconciliation as part of acceptance tests. @21 @29

USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;

-- Example: call reconcile for demo account and assert status = MATCH
CALL DOCGEN.RECONCILE_BILLING_RUNS('acct_demo', '2024-01-01', '2024-01-31');

-- Validate results
SELECT * FROM DOCGEN.BILLING_RECONCILIATION_RUN WHERE ACCOUNT_ID = 'acct_demo' ORDER BY RUN_TS DESC LIMIT 1;

