
  
    

create or replace transient table ETL_TESTING.STAGING_MARTS.dim_reaction
    
    
    
    as (

with src as (
    select *
    from ETL_TESTING.STAGING_CLEAN.clean_fda_reac
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


  