-- Utility UDF to extract nested JSON keys used in parsing/analytics pipelines @1 @6.
CREATE OR REPLACE FUNCTION DOCGEN.JSON_EXTRACT_PATH(data VARIANT, path ARRAY) RETURNS VARIANT LANGUAGE SQL AS $$ (SELECT data:path[0]) $$;

