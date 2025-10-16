-- Simple alert view to be used by alert detectors if reconciliation fails (counts mismatch). @30 @29
CREATE OR REPLACE VIEW DOCGEN.V_EVIDENCE_MISMATCH AS
SELECT e.BUNDLE_ID, e.BUNDLE_URL, r.event_count
FROM DOCGEN.SIGNATURE_EVIDENCE_BUNDLE e
LEFT JOIN DOCGEN.V_EVIDENCE_RECONCILE r ON e.BUNDLE_ID = r.BUNDLE_ID
WHERE r.event_count IS NULL OR r.event_count = 0;

