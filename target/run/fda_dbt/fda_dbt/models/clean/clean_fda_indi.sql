
  
    

create or replace transient table ETL_TESTING.STAGING_CLEAN.clean_fda_indi
    
    
    
    as (

with src as (
    select *
    from ETL_TESTING.STAGING_STAGING.stg_fda_indi
),

final as (
    select
        row_number() over(order by primaryid) as indi_sk,
        *
    from src
)

select *
from final
    )
;


  