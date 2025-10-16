def handler(session, signer_id, phone, ttl_minutes): import random, hashlib, time code = str(random.randint(100000,999999)) session.sql(f"INSERT INTO DOCGEN.PHONE_VERIFICATIONS (VERIF_ID, SIGNER_ID, PHONE_NUMBER, VERIF_CODE, EXPIRES_AT) VALUES (UUID_STRING(), '{signer_id}', '{phone}', '{code}', DATEADD(minute, {ttl_minutes}, CURRENT_TIMESTAMP()))").collect() return {'verif_code': code}

