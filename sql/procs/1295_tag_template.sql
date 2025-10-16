-- Add or update tags for a template for governance and searchability. @1 @31
CREATE OR REPLACE PROCEDURE DOCGEN.TAG_TEMPLATE(template_id STRING, tags ARRAY)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
UPDATE DOCGEN.SIGNATURE_TEMPLATE_METADATA SET TAGS = :tags WHERE TEMPLATE_ID = :template_id;
$$;

