CREATE OR REPLACE PROCEDURE DOCGEN.RUN_ACCESS_REVIEW()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
HANDLER='run_access_review';

Generates access review artifacts and emails reviewers using Notification Worker. @344 @31

