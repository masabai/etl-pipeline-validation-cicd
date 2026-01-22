{{ config(materialized='table') }}

with src as (
    select *
    from {{ ref('stg_fda_outc') }}
),

final as (
    select
        row_number() over(order by primaryid) as outc_sk,
        *
    from src
)

select *
from final
