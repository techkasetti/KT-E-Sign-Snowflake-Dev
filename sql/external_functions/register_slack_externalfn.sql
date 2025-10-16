Purpose: External Function registration to post alerts/notifications to Slack via a secure gateway per External Function patterns; used by notify procedures and alert flows. @62 @31
-- register_slack_externalfn.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE API INTEGRATION AI_FEATURE_HUB.SLACK_INTEGRATION
ENABLED = TRUE
API_AWS_ROLE_ARN = 'arn:aws:iam::111122223333:role/slack-integration-role'
ALLOWED_PREFIXES = ('https://hooks.slack.com/');
CREATE OR REPLACE EXTERNAL FUNCTION DOCGEN.SLACK_NOTIFY(payload VARIANT)
RETURNS VARIANT
API_INTEGRATION = AI_FEATURE_HUB.SLACK_INTEGRATION
HEADERS = ('Content-Type' = 'application/json')
AS 'https://hooks.slack.com/services/your/webhook/path';

