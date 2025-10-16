-- Seed per-feature ratecard (production-like concrete values)
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;

INSERT INTO DOCGEN.RATE_CARD (FEATURE_KEY, BASE_UNIT_PRICE, UNIT, CREATED_AT)
VALUES
('esign_basic', 0.05, 'per-sign', CURRENT_TIMESTAMP()),
('esign_advanced', 0.20, 'per-sign', CURRENT_TIMESTAMP()),
('esign_qes', 2.50, 'per-sign', CURRENT_TIMESTAMP());

