
  
    

create or replace transient table ETL_TESTING.STAGING_EDA.patient_demo
    
    
    
    as (

-- ===============================================
-- EDA Model: Patient Counts by Age Group and Sex
-- ===============================================
-- Purpose:
--   Summarize the number of unique patients by broad age groups and sex.
--   This provides a small, meaningful overview of the patient distribution
--
-- Logic:
--   1. Age is categorized into three groups:
--        - Pediatric (<18)
--        - Adult (18–64)
--        - Elderly (65+)
--   2. Counts are based on DISTINCT caseid to avoid double-counting patients.
--   3. Null ages are excluded from the analysis.
--   4. Results are ordered by age_group and descending patient counts.

select
    case
        when age < 18 then 'Pediatric'
        when age >= 65 then 'Elderly'
        else 'Adult'
    end as age_group,  -- categorized age group
    sex,               -- patient sex
    count(distinct caseid) as num_patients  -- unique patient count
from ETL_TESTING.STAGING_MARTS.fact_adverse_events  -- source fact table (already cleaned)
where age is not null
group by age_group, sex
order by age_group, num_patients desc
    )
;


  