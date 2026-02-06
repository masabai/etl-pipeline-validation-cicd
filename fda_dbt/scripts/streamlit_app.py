"""
FDA FAERS EDA Dashboard (Snowflake-native Streamlit)

This script provides a lightweight demonstration of in-warehouse analytics for FDA FAERS datasets.
It connects to the active Snowflake session using Snowpark, queries validated marts, and visualizes:

1. Patient distribution by Age Group × Sex
2. Top 10 Countries by report counts

Key Features:
- Snowflake-native: runs entirely within Snowflake using Snowpark session.
- Multi-tab layout for separate EDA views.
- Multi-color charts for better visual distinction.
- Raw data tables shown below charts for verification.

Date: 2026-02-05
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
import altair as alt

# ---------------- Streamlit Page Configuration ----------------
st.set_page_config(layout="wide", page_title="FDA FAERS EDA Dashboard")
st.title("FDA FAERS: EDA Dashboard")

# ---------------- Snowflake Session ----------------
# Uses the active Snowflake session from Streamlit Snowpark
session = get_active_session()
DB_PATH = "ETL_TESTING.MARTS"  # Schema containing marts/validated fact tables

# ---------------- Tabs for Each EDA ----------------
tab_age_sex, tab_country = st.tabs(["Age × Sex", "Top Countries"])

# -------- Tab 1: Age × Sex Distribution ----------
with tab_age_sex:
    # Query: count unique patients by broad age group and sex
    query_age_sex = f"""
    SELECT
        CASE
            WHEN age < 18 THEN 'Pediatric'
            WHEN age >= 65 THEN 'Elderly'
            ELSE 'Adult'
        END AS age_group,
        sex,
        COUNT(DISTINCT caseid) AS num_patients
    FROM {DB_PATH}.FACT_ADVERSE_EVENTS
    WHERE age IS NOT NULL
    GROUP BY age_group, sex
    ORDER BY age_group, num_patients DESC
    """
    df_age_sex = session.sql(query_age_sex).to_pandas()
    df_age_sex.columns = [c.upper() for c in df_age_sex.columns]

    # Pivot data for multi-color bar chart
    chart_data = df_age_sex.pivot(index='AGE_GROUP', columns='SEX', values='NUM_PATIENTS').fillna(0)

    # Display bar chart first
    st.subheader("Patient Distribution by Age Group and Sex")
    st.bar_chart(chart_data)

    # Display raw table below for verification
    st.subheader("Patient Counts Table")
    st.dataframe(df_age_sex, use_container_width=True, hide_index=True)

# -------- Tab 2: Top 10 Countries by Report Count ----------
with tab_country:
    # Query: count unique reports per reporting country
    query_country = f"""
    SELECT occr_country, COUNT(DISTINCT caseid) AS num_reports
    FROM {DB_PATH}.FACT_ADVERSE_EVENTS
    WHERE occr_country IS NOT NULL
    GROUP BY occr_country
    ORDER BY num_reports DESC
    LIMIT 10
    """
    df_country = session.sql(query_country).to_pandas()
    df_country.columns = [c.upper() for c in df_country.columns]

    st.subheader("Top 10 Countries by Report Count")

    # Multi-color bar chart using Altair
    chart = alt.Chart(df_country).mark_bar().encode(
        x=alt.X('NUM_REPORTS', title='Number of Reports'),
        y=alt.Y('OCCR_COUNTRY', sort='-x', title='Country'),
        color='OCCR_COUNTRY',  # multi-color by country
        tooltip=['OCCR_COUNTRY', 'NUM_REPORTS']
    ).properties(height=400)

    st.altair_chart(chart, use_container_width=True)

    # Display raw table below for verification
    st.subheader("Raw Table")
    st.dataframe(df_country, use_container_width=True, hide_index=True)
