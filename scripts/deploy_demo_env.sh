#!/usr/bin/env bash
# Demo deploy script: sets demo env vars and runs register driver (dev purposes only)
export SNOW_ACCOUNT="demo_account"
export SNOW_USER="sysadmin"
export SNOW_ROLE="SYSADMIN"
export STORAGE_INTEGRATION="DEMO_STORAGE_INTEGRATION"
export YOUR_S3_BUCKET="demo-bucket-12345"
export API_AWS_ROLE_ARN="arn:aws:iam::123456789012:role/demo-api-role"
export API_GATEWAY_HOST="https://api-demo.example.com"
export INTEGRATION_KDF_SECRET="demo_secret_for_kdf"
# Run DDLs
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -f sql/ddl/ai_feature_hub_schema.sql
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -f sql/ddl/signature_domain_schema.sql
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -f sql/ddl/tmp_tables_and_analytics.sql
# Attach grants and policies
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -f sql/attach_policies_and_grants.sql
# Create Snowpipe & stages
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -f sql/snowpipe/snowpipe_setup.sql
# Put Python procs to stage and register
./register/register_all_procs.sh
# Register external functions
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -f sql/external_functions/external_functions_register.sql
# Create materialized views and tasks
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -f sql/materialized_views/signature_analytics_views.sql
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -f sql/tasks/create_tasks_demo.sql
# Seed demo data
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -f sql/seed/sample_seed_demo.sql
echo "Demo deploy complete (dev values). Replace demo env vars with production values before running in prod."  

