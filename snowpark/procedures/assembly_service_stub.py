# Assembly service stub that Snowflake can call via External Function or via middleware
def assemble_document(template_id, merge_fields):
    # call external renderer or return rendered URL placeholder
    return {"rendered_url": "s3://demo/rendered/doc.pdf", "hash": "hash_demo"}
# Assembly service must be implemented as external renderer per doc assembly gap analysis @31 @41

