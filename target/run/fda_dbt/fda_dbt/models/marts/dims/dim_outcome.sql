
  
    

create or replace transient table ETL_TESTING.STAGING_MARTS.dim_outcome
    
    
    
    as (

with base as (

    select
        primaryid,
        caseid,
        outc_cod,
        row_number() over(partition by primaryid, caseid order by outc_cod) as outc_sk
    from ETL_TESTING.STAGING_CLEAN.clean_fda_outc

)

select *
from base
    )
;


  