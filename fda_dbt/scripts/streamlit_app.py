"""
FDA FAERS EDA Dashboard (Snowflake-native Streamlit)

This script provides a lightweight demonstration of in-warehouse analytics for FDA FAERS datasets.
It connects to the active Snowflake session using Snowpark, queries validated marts, and visualizes:

1. Reported Serious Outcomes for top antidepressants
   - Death (DE)
   - Life-Threatening (LT)
   - Hospitalization (HO)
   - Disability (DS)

Key Features:
- Snowflake-native: runs entirely within Snowflake using Snowpark session.
- Multi-tab layout for separate EDA views.
- Multi-color charts for better visual distinction.
- Raw data tables shown below charts for verification.

Date: 2026-02-05
"""

import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
st.set_page_config(
    page_title="FAERS Antidepressant Serious Outcomes",
    layout="wide"
)

st.title("Reported Serious Outcomes")
st.subheader("Top Antidepressants – 2025 Q1/Q2 FAERS Reports")

st.markdown(
    "Only 6 months of FAERS data shown. "
    '<span style="color:red; font-weight:bold;">'
    "High death counts may reflect disease severity, age, and treatment context rather than drug causality."
    '</span>',
    unsafe_allow_html=True
)

# -------------------------------------------------
# SNOWFLAKE SESSION
# -------------------------------------------------
session = get_active_session()

# -------------------------------------------------
# SQL QUERY
# -------------------------------------------------
query = """
SELECT
    d.drugname,
    o.outc_cod AS outcome_category,
    COUNT(DISTINCT d.caseid) AS total_cases
FROM marts.dim_drug d
JOIN marts.dim_outcome o
    ON d.caseid = o.caseid
WHERE d.drugname IN (
    'SERTRALINE', 'FLUOXETINE', 'CITALOPRAM', 'ESCITALOPRAM',
    'VENLAFAXINE', 'DULOXETINE','BUPROPION', 'TRAZODONE','AMITRIPTYLINE'
)
AND o.outc_cod IN ('DE', 'LT', 'HO', 'DS')
GROUP BY d.drugname, o.outc_cod
ORDER BY d.drugname, total_cases DESC
"""

df = session.sql(query).to_pandas()
df.columns = [c.lower() for c in df.columns]

# -------------------------------------------------
# CLEAN LABELS
# -------------------------------------------------
outcome_labels = {
    "DE": "Death",
    "LT": "Life-Threatening",
    "HO": "Hospitalization",
    "DS": "Disability"
}
df["outcome_category"] = df["outcome_category"].map(outcome_labels)

# -------------------------------------------------
# CREATE TABS
# -------------------------------------------------
tab_all, tab_de, tab_lt, tab_hos, tab_ds = st.tabs([
    "All Outcomes",
    "Death Only",
    "Life-Threatening Only",
    "Hospitalization Only",
    "Disability Only"])

# -----------------------------
# TAB 1: All Outcomes (Main)
# -----------------------------
with tab_all:
    chart_all = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("drugname:N", title="Antidepressant"),
            y=alt.Y("total_cases:Q", title="Total Cases"),
            color=alt.Color(
                "outcome_category:N",
                title="Outcome",
                scale=alt.Scale(scheme="tableau10")
            ),
            tooltip=["drugname", "outcome_category", "total_cases"]
        )
        .properties(height=450)
    )
    st.altair_chart(chart_all, use_container_width=True)
    st.subheader("Patient Counts Table (All Outcomes)")
    st.dataframe(df, use_container_width=True, hide_index=True)

# -----------------------------
# TAB 2: Death Only
# -----------------------------
with tab_de:
    df_de = df[df["outcome_category"] == "Death"]
    chart_de = (
        alt.Chart(df_de)
        .mark_bar()
        .encode(
            x=alt.X("drugname:N", title="Antidepressant"),
            y=alt.Y("total_cases:Q", title="Total Cases"),
            color=alt.Color(
                "drugname:N",
                title="Antidepressant",
                scale=alt.Scale(scheme="tableau10")
            ),
            tooltip=["drugname", "total_cases"]
        )
        .properties(height=450)
    )
    st.altair_chart(chart_de, use_container_width=True)
    st.subheader("Patient Counts Table (Death)")
    st.dataframe(df_de, use_container_width=True, hide_index=True)

# -----------------------------
# TAB 3: Life-Threatening Only
# -----------------------------
with tab_lt:
    df_lt = df[df["outcome_category"] == "Life-Threatening"]
    chart_lt = (
        alt.Chart(df_lt)
        .mark_bar()
        .encode(
            x=alt.X("drugname:N", title="Antidepressant"),
            y=alt.Y("total_cases:Q", title="Total Cases"),
            color=alt.Color(
                "drugname:N",
                title="Antidepressant",
                scale=alt.Scale(scheme="tableau10")
            ),
            tooltip=["drugname", "total_cases"]
        )
        .properties(height=450)
    )
    st.altair_chart(chart_lt, use_container_width=True)
    st.subheader("Patient Counts Table (Life-Threatening)")
    st.dataframe(df_lt, use_container_width=True, hide_index=True)

# -----------------------------
# TAB 4: Hospitalization Only
# -----------------------------
with tab_hos:
    df_hos = df[df["outcome_category"] == "Hospitalization"]
    chart_hos = (
        alt.Chart(df_hos)
        .mark_bar()
        .encode(
            x=alt.X("drugname:N", title="Antidepressant"),
            y=alt.Y("total_cases:Q", title="Total Cases"),
            color=alt.Color(
                "drugname:N",
                title="Antidepressant",
                scale=alt.Scale(scheme="tableau10")
            ),
            tooltip=["drugname", "total_cases"]
        )
        .properties(height=450)
    )
    st.altair_chart(chart_hos, use_container_width=True)
    st.subheader("Patient Counts Table (Hospitalization)")
    st.dataframe(df_hos, use_container_width=True, hide_index=True)

# -----------------------------
# TAB 5: Disability Only
# -----------------------------
with tab_ds:
    df_ds = df[df["outcome_category"] == "Disability"]
    chart_ds = (
        alt.Chart(df_ds)
        .mark_bar()
        .encode(
            x=alt.X("drugname:N", title="Antidepressant"),
            y=alt.Y("total_cases:Q", title="Total Cases"),
            color=alt.Color(
                "drugname:N",
                title="Antidepressant",
                scale=alt.Scale(scheme="tableau10")
            ),
            tooltip=["drugname", "total_cases"]
        )
        .properties(height=450)
    )
    st.altair_chart(chart_ds, use_container_width=True)
    st.subheader("Patient Counts Table (Disability)")
    st.dataframe(df_ds, use_container_width=True, hide_index=True)

# -----------------------------
# DATA TABLE
# -----------------------------
with st.expander("View underlying data"):
    st.dataframe(df, use_container_width=True)

# -----------------------------
# FOOTNOTE
# -----------------------------
st.markdown(
    "Only 6 months of FAERS data shown. "
    '<span style="color:red; font-weight:bold;">'
    "High death counts may reflect disease severity, age, and treatment context rather than drug causality."
    '</span>',
    unsafe_allow_html=True
)
