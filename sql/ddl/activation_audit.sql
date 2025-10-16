Purpose: Persist admin activation events (ActivationAudit) for entitlement and integration key actions to support reconciliation and audit trails. @74 @189

USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;

CREATE OR REPLACE TABLE DOCGEN.ACTIVATION_AUDIT (
  AUDIT_ID STRING PRIMARY KEY,
  ACCOUNT_ID STRING,
  ACTION_TYPE STRING,          -- e.g., 'INTEGRATION_KEY_CREATED','FEATURE_ENABLED','FEATURE_DISABLED'
  ACTION_BY STRING,            -- admin user or service account
  ACTION_PAYLOAD VARIANT,
  ACTION_TS TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
  RESULT_STATUS STRING,
  NOTES STRING
);

