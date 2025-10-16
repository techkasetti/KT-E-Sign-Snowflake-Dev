-- Test: call UPSERT_SIGNATURE_WEBHOOK twice with same EVENT_ID and assert only one event row exists (idempotency). @14 @11
CALL DOCGEN.UPSERT_SIGNATURE_WEBHOOK(PARSE_JSON('{"EVENT_ID":"evt-dup","REQUEST_ID":"req-dup","DOCUMENT_ID":"doc-dup","SIGNER_ID":"signer-dup","ACCOUNT_ID":"acct-dup","EVENT_TYPE":"SIGNED"}'));
CALL DOCGEN.UPSERT_SIGNATURE_WEBHOOK(PARSE_JSON('{"EVENT_ID":"evt-dup","REQUEST_ID":"req-dup","DOCUMENT_ID":"doc-dup","SIGNER_ID":"signer-dup","ACCOUNT_ID":"acct-dup","EVENT_TYPE":"SIGNED"}'));

