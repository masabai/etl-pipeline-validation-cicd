
  
    

create or replace transient table ETL_TESTING.STAGING_EDA.report_by_country
    
    
    
    as (

-- ===============================================
-- EDA Model: Top 10 Countries by Report Count
-- ===============================================
-- Purpose:
--   Identify the top 10 countries with the highest number of unique adverse event reports.
--   Provides a global overview of where reports are most frequently submitted.
--
-- Logic:
--   1. Counts DISTINCT caseid to ensure each report is counted only once.
--   2. Excludes rows where occr_country is null.
--   3. Aggregates by country and sorts descending to get the top reporters.
--   4. Limits output to 10 countries for concise EDA visualization.

select
    occr_country,                      -- country where the adverse event occurred
    count(distinct caseid) as num_reports  -- number of unique reports per country
from ETL_TESTING.STAGING_MARTS.fact_adverse_events  -- cleaned fact table
where occr_country is not null         -- exclude missing countries
group by occr_country                   -- aggregate by country
order by num_reports desc               -- sort by number of reports descending
limit 10                                -- keep only top 10 countries
    )
;


  