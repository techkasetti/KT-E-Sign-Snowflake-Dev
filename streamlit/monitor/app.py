# Streamlit monitoring app for E-Signature module (dev)
import streamlit as st
import pandas as pd
import subprocess, json

st.title("DocGen E-Signature Monitor (Demo)")
st.markdown("KPI panels and recent alerts")

def run_snowsql_query(q):
    # Simple helper: in demo environment this calls snowsql and returns CSV; in production use secure connectors
    p = subprocess.Popen(["snowsql", "-a", "client_prod_001", "-u", "svc_docgen", "-r", "DOCGEN_MONITOR", "-q", q, "--output-format=csv"], stdout=subprocess.PIPE)
    out, _ = p.communicate()
    txt = out.decode("utf-8")
    return txt

kpi_q = "SELECT CAST(EVENT_TS::DATE AS DATE) AS day, COUNT(*) AS events_count FROM DOCGEN.SIGNATURE_EVENTS WHERE EVENT_TS >= DATEADD('day', -7, CURRENT_TIMESTAMP()) GROUP BY 1 ORDER BY 1 DESC LIMIT 14;"
kpi_csv = run_snowsql_query(kpi_q)
st.subheader("Signatures per day (last 7d)")
st.text(kpi_csv)

alerts_q = "SELECT ALERT_ID, ALERT_TYPE, PAYLOAD, ALERT_TS FROM DOCGEN.ALERTS WHERE RESOLVED = FALSE ORDER BY ALERT_TS DESC LIMIT 50;"
alerts_csv = run_snowsql_query(alerts_q)
st.subheader("Pending Alerts")
st.text(alerts_csv)

This Streamlit app provides a simple operations dashboard that queries Snowflake via snowsql for live KPIs and pending alerts for demo monitoring and operator use @51 @121.

