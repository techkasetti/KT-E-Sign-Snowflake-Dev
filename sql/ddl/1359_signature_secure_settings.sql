-- Store non-secret settings and refs (do NOT store secrets here) for operator configuration and feature toggles @1 @6.
CREATE OR REPLACE TABLE DOCGEN.SIGNATURE_SETTINGS ( SETTING_KEY STRING PRIMARY KEY, SETTING_VALUE VARIANT, UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() );

