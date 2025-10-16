-- Operator user registry to map operator identities to notification endpoints for paging @1 @6.
CREATE OR REPLACE TABLE DOCGEN.OPERATORS ( OPERATOR_ID STRING PRIMARY KEY, NAME STRING, CONTACT VARIANT, ON_CALL BOOLEAN DEFAULT FALSE );

