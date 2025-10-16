# Evidence export JSON schema (for the evidence zipper and S3 exports)
- bundle_id: string
- document_id: string
- account_id: string
- bundle_hash: string
- metadata: object (keys: assembled_at, request_id, created_by, signature_summary)
- events: array of event objects (event_id, signer_id, event_type, ip, ua, ts)
- validations: array of validation objects (validation_id, method, result, cert_chain)
- files: array of file descriptors (path, checksum, mime)

