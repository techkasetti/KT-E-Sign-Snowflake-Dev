-- Persist AML/KYC check result invoked by pre-sign checks or external identity verification. CREATE OR REPLACE PROCEDURE DOCGEN.RECORD_AML_CHECK(signer_id STRING, provider STRING, result STRING, details VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$ INSERT INTO DOCGEN.AML_KYC_CHECKS (CHECK_ID, SIGNER_ID, CHECK_PROVIDER, RESULT, DETAILS) VALUES (UUID_STRING(), :signer_id, :provider, :result, :details); $$;

