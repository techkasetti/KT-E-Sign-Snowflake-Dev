-- Validate incoming payload against registry mapping and insert into raw staging as required by parsing flow @1 @6.
CREATE OR REPLACE PROCEDURE DOCGEN.VALIDATE_SCHEMA_AND_RECORD(provider STRING, raw_payload VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
HANDLER='handler'
AS
$$
# staged handler at @~/procedures/validate_schema_and_record.py
$$;

