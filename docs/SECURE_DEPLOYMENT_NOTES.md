# Secure deployment notes (production)
- Replace demo S3 buckets with your production S3 bucket and ensure STORAGE_INTEGRATION is configured with an IAM role with least privilege.
- Replace demo credentials used in CI/Streamlit with CI secrets; never commit secrets in repo.
- Use a dedicated service role (DOCGEN_SERVICE) with least privileges to execute procedures; assign DOCGEN_ADMIN only to operators.
- For HSM signing, deploy a hardened API gateway and register External Function API_INTEGRATION with the gateway principal/ARN; validate mTLS or IAM role mapping.
- Harden row access and masking policies to protect PII and evidence metadata.

