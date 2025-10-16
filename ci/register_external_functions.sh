# Registration helper: run from CI with environment substitutions; registers API_INTEGRATION and External Functions as shown in the runbook. @29 @31
#!/bin/bash
set -e
snowsql -f sql/external_functions/300_ocsp_external_function.sql
snowsql -f sql/external_functions/301_hsm_signer_external_function.sql
snowsql -f sql/external_functions/302_faiss_similarity_ef.sql

