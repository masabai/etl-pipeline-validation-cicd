

with src as (
    select *
    from ETL_TESTING.STAGING_STAGING.stg_fda_outc
),

final as (
    select
        row_number() over(order by primaryid) as outc_sk,
        *
    from src
)

select *
from final