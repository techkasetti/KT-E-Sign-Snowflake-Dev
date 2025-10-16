#!/usr/bin/env bash
# Run a quick smoke test sequence (demo)
source ./scripts/deploy_demo_env.sh
# Run sample seed
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -f sql/seed/sample_seed_demo.sql
# Call upsert webhook (demo JSON)
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CALL DOCGEN.UPSERT_SIGNATURE_WEBHOOK(PARSE_JSON('{\"event_id\":\"evt_demo_1\",\"request_id\":\"req_demo_1\",\"signer_id\":\"s_demo_1\",\"event_type\":\"SIGNED\",\"ip\":\"1.2.3.4\",\"ua\":\"demo-agent\"}'))"
# Verify
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CALL DOCGEN.VERIFY_SIGNATURE('req_demo_1','s_demo_1')"
# Assemble evidence
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -q "CALL DOCGEN.WRITE_EVIDENCE_BUNDLE(PARSE_JSON('{\"request_id\":\"req_demo_1\",\"signer_id\":\"s_demo_1\"}'))"

