CREATE OR REPLACE PROCEDURE DOCGEN.SNAPSHOT_ANALYTICS()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='snapshot_analytics';

Creates scheduled analytics snapshots for quick dashboarding and SLO checks. @344 @31

