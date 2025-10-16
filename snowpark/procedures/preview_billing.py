# preview_billing.py (simplified)
from snowflake.snowpark import Session
import json, hashlib

def preview_billing(session: Session, account_id: str, period_start: str, period_end: str):
    rows = session.sql(f"""
        SELECT FEATURE_KEY, SUM(USAGE_QTY) AS UNITS
        FROM DOCGEN.TENANT_FEATURE_USAGE
        WHERE ACCOUNT_ID = '{account_id}'
          AND USAGE_DATE BETWEEN '{period_start}' AND '{period_end}'
        GROUP BY FEATURE_KEY
    """).collect()

    line_items = []
    for r in rows:
        feature = r['FEATURE_KEY']
        units = r['UNITS']
        price_row = session.sql(f"""
            SELECT BASE_UNIT_PRICE FROM DOCGEN.ACCOUNT_FEATURE_PRICING
            WHERE ACCOUNT_ID = '{account_id}' AND FEATURE_KEY = '{feature}'
            ORDER BY EFFECTIVE_FROM DESC LIMIT 1
        """).collect()
        base = price_row[0]['BASE_UNIT_PRICE'] if price_row else 0
        total = units * base
        line_items.append({"feature_key": feature, "units": units, "base": float(base), "total": float(total)})

    invoice_hash = hashlib.sha256(json.dumps(line_items, sort_keys=True).encode()).hexdigest()
    return {"line_items": line_items, "invoice_hash": invoice_hash}

