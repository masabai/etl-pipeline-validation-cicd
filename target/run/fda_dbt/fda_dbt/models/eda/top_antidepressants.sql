
  
    

create or replace transient table ETL_TESTING.STAGING_EDA.top_antidepressants
    
    
    
    as (

-- ===============================================
-- EDA Model: Top Antidepressants by Serious Outcomes (DS)
-- ===============================================
-- Purpose:
--   Summarize the number of unique cases for top antidepressants by outcome category,
--   focusing on Disability (DS) cases.
--
-- Logic:
--   1. Only selected top antidepressants are included.
--   2. Counts are based on DISTINCT caseid to avoid double-counting.
--   3. Only outcomes DE, LT, HO, DS are considered.
--   4. Final result shows the maximum number of DS cases per drug.


    select
        d.drugname,
        o.outc_cod as outcome_category,
        count(distinct d.caseid) as total_cases
    from ETL_TESTING.MARTS.dim_drug d
    join ETL_TESTING.MARTS.dim_outcome o
        on d.caseid = o.caseid
    where d.drugname in (
        'SERTRALINE', 'FLUOXETINE', 'CITALOPRAM', 'ESCITALOPRAM',
        'VENLAFAXINE', 'DULOXETINE','BUPROPION', 'TRAZODONE','AMITRIPTYLINE'
    )
      and o.outc_cod in ('DE', 'LT', 'HO', 'DS')
    group by
        d.drugname,
        o.outc_cod
    order by
        d.drugname,
        total_cases desc
    )
;


  