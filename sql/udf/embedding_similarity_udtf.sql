-- Simple UDTF wrapper for vector similarity using native vector or external FAISS function fallback
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;

CREATE OR REPLACE TABLE FUNCTION DOCGEN.SIMILARITY_SEARCH(query VARIANT)
  RETURNS TABLE (DOCUMENT_ID STRING, SCORE FLOAT)
  LANGUAGE JAVASCRIPT
  AS $$
    // Demo JS UDTF: prefer calling FAISS external function through SQL; here we call the FAISS_SIMILARITY external function
    var res = snowflake.execute({sqlText: "SELECT DOCGEN.FAISS_SIMILARITY(PARSE_JSON('" + JSON.stringify(query) + "')) AS out"});
    var row = res.next();
    if (row) {
      return JSON.parse(row.OUT);
    }
    return [];
  $$;

