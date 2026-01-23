{{ config(materialized='table') }}

with src as (
    select *
    from {{ ref('clean_fda_reac') }}
),

final as (
    select
        row_number() over(order by primaryid) as indi_sk,
        *
    from src
)

select *
from final
