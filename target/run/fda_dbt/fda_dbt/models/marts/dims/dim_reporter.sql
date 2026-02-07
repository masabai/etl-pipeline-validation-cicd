
  
    

create or replace transient table ETL_TESTING.STAGING_MARTS.dim_reporter
    
    
    
    as (

with base as (

    select
        primaryid,
        caseid,
        rpsr_cod,
        row_number() over(partition by primaryid, caseid order by rpsr_cod) as rpsr_sk
    from ETL_TESTING.STAGING_CLEAN.clean_fda_rpsr

)

select *
from base
    )
;


  