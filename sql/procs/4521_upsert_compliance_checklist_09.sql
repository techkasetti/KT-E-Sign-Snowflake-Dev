-- Generated per Snowflake E-Signature patterns. @31
CREATE OR REPLACE PROCEDURE DOCGEN.UPSERT_COMPLIANCE_CHECKLIST_09(check_id STRING, name STRING, items VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
MERGE INTO DOCGEN.COMPLIANCE_CHECKLISTS_09 t USING (SELECT :check_id AS cid, :name AS nm, :items AS it) s ON t.CHECK_ID = s.cid WHEN MATCHED THEN UPDATE SET NAME = s.nm, ITEMS = s.it, UPDATED_AT = CURRENT_TIMESTAMP() WHEN NOT MATCHED THEN INSERT (CHECK_ID, NAME, ITEMS) VALUES (s.cid, s.nm, s.it);
$$

- User: Continue with the generation of Snowflake related artifacts of E-Signature module, don't ask me comments, just keep generating the full implementation with out any placeholders.With minimum of 130 artifacts output at a time.Hope the count is 130 every tranch.Again no comments at the end and no packaging into Zip etc.. everything is copy / paste

- Anara: Generating next 130 Snowflake E-Sign artifacts per your request @343

