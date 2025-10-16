CREATE OR REPLACE PROCEDURE DOCGEN.UPSERT_ADMIN_SETTING_04(setting_id STRING, name STRING, value VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
MERGE INTO DOCGEN.ADMIN_SETTINGS_04 t USING (SELECT :setting_id AS sid, :name AS nm, :value AS val) s ON t.SETTING_ID = s.sid WHEN MATCHED THEN UPDATE SET NAME = s.nm, VALUE = s.val, UPDATED_AT = CURRENT_TIMESTAMP() WHEN NOT MATCHED THEN INSERT (SETTING_ID, NAME, VALUE) VALUES (s.sid, s.nm, s.val);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: FILENAME: sql/ddl/3956_signature_meta_index_05.sql
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.SIGNATURE_META_INDEX_05 (
  META_ID STRING PRIMARY KEY,
  BUNDLE_ID STRING,
  META_KEY STRING,
  META_VALUE VARIANT,
  CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

