-- Smoke test: stage sample signature webhook JSONL and call ingestion; asserts row exists per acceptance criteria. @27 @22
-- Sample JSONL must be PUT to @~/staged/signature_sample.jsonl before running this script.
CALL DOCGEN.UPSERT_SIGNATURE_WEBHOOK(PARSE_JSON('{
  "EVENT_ID":"evt-0001","REQUEST_ID":"req-0001","DOCUMENT_ID":"doc-0001","SIGNER_ID":"signer-1","ACCOUNT_ID":"acct-1","EVENT_TYPE":"SIGNED"
}'));

