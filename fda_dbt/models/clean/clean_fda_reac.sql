{{ config(materialized='table') }}

with src as (
    select *
    from {{ ref('stg_fda_reac') }}
),

final as (
    select
        row_number() over(order by primaryid) as reac_sk,
        *
    from src
)

select *
from final
