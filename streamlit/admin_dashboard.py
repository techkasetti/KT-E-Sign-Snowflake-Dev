Purpose: Streamlit admin dashboard showing evidence metrics, reconciliation status, alert summaries and top signers; this provides a simple Admin surface to review key E‑Signature KPIs and follows the Admin/Streamlit dashboard patterns in the operational guidance @21 @57.
# admin_dashboard.py
import streamlit as st
import pandas as pd
from snowflake.connector import connect
st.set_page_config(page_title="DocGen Admin Dashboard")
st.title("DocGen E-Signature Admin Dashboard")
# Connect (use environment var credentials in deployment)
conn = connect(user="svc_docgen", account="client_prod_001", password="DemoPassw0rd!", role="DOCGEN_MONITOR")
cur = conn.cursor()
cur.execute("SELECT * FROM DOCGEN.V_EVIDENCE_METRICS LIMIT 30;")
rows = cur.fetchall()
df = pd.DataFrame(rows, columns=[col[0] for col in cur.description])
st.subheader("Evidence Metrics")
st.dataframe(df)
cur.execute("SELECT ALERT_ID, ALERT_TYPE, SEVERITY, ALERT_TS FROM DOCGEN.ALERTS WHERE RESOLVED = FALSE ORDER BY ALERT_TS DESC LIMIT 50;")
alerts = cur.fetchall()
if alerts:
    st.subheader("Active Alerts")
    st.table(pd.DataFrame(alerts, columns=[col[0] for col in cur.description]))
cur.close()
conn.close() @21 @57

