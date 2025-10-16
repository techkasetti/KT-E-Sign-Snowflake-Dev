Purpose: Create a signature request and seed signer entries; returns request_id and signer records.
# create_signature_request.py
from snowflake.snowpark import Session
import uuid, json, datetime

def create_signature_request(session: Session, account_id: str, document_id: str, template_id: str, requester_id: str, signers: list):
    """
    signers: list of dicts {signer_email, signer_name, signer_order, auth_method}
    """
    request_id = "req_" + uuid.uuid4().hex
    session.sql(f"""
        INSERT INTO DOCGEN.SIGNATURE_REQUESTS (REQUEST_ID, ACCOUNT_ID, DOCUMENT_ID, TEMPLATE_ID, REQUESTER_ID, STATUS, CREATED_AT)
        VALUES ('{request_id}', '{account_id}', '{document_id}', '{template_id}', '{requester_id}', 'PENDING', CURRENT_TIMESTAMP());
    """).collect()

    created = []
    for s in sorted(signers, key=lambda x: int(x.get('signer_order', 1))):
        signer_id = "signer_" + uuid.uuid4().hex
        email = s.get('signer_email', 'unknown@example.com').replace("'", "''")
        name = s.get('signer_name', '')
        order = int(s.get('signer_order', 1))
        auth = s.get('auth_method', 'email')
        payload = s.get('auth_payload', {})
        session.sql(f"""
            INSERT INTO DOCGEN.SIGNERS (SIGNER_ID, REQUEST_ID, ACCOUNT_ID, SIGNER_EMAIL, SIGNER_NAME, SIGNER_ORDER, AUTH_METHOD, AUTH_PAYLOAD, STATUS, CREATED_AT)
            VALUES ('{signer_id}', '{request_id}', '{account_id}', '{email}', '{name}', {order}, '{auth}', PARSE_JSON('{json.dumps(payload)}'), 'PENDING', CURRENT_TIMESTAMP());
        """).collect()
        created.append({"signer_id": signer_id, "email": email, "order": order})
    return {"request_id": request_id, "signers": created}

