
  
    

create or replace transient table ETL_TESTING.STAGING_CLEAN.clean_fda_reac
    
    
    
    as (

with src as (
    select *
    from ETL_TESTING.STAGING_STAGING.stg_fda_reac
),

final as (
    select
        row_number() over(order by primaryid) as reac_sk,
        *
    from src
)

select *
from final
    )
;


  