

with src as (
    select *
    from ETL_TESTING.STAGING_STAGING.stg_fda_ther
),

final as (
    select
        row_number() over(order by primaryid) as ther_sk,
        *
    from src
)

select *
from final