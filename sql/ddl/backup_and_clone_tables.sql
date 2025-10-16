Purpose: Create backup clone table used by purge_procedure to safely copy rows prior to deletion. @133 @914
USE DATABASE AI_FEATURE_HUB;
USE SCHEMA DOCGEN;
CREATE OR REPLACE TABLE DOCGEN.BACKUP_DOCUMENT_ARCHIVE_CLONE LIKE DOCGEN.DOCUMENT_ARCHIVE;

