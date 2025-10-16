# Security hardening guidance for DocGen E-Signature Snowflake module
- Enforce least-privilege roles: Operator roles (DOCGEN_ADMIN) and service roles (DOCGEN_SERVICE) must be separated; monitoring role DOCGEN_MONITOR is read-only. @281 @337
- Do not store secrets in code; use CI secret manager and Snowflake external functions with API_INTEGRATION for secure callouts. @16 @148
- Mask PII fields with masking policies and enforce row-access policies for account isolation as implemented in row_access_and_masking.sql. @29 @234
- Replace HSM signer stub with vendor HSM integration and ensure OCSP responder External Function responses are validated before accepting signatures as legal evidence. @114 @197
