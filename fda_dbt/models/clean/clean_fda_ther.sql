{{ config(materialized='table') }}

with src as (
    select *
    from {{ ref('stg_fda_ther') }}
),

final as (
    select
        row_number() over(order by primaryid) as ther_sk,
        *
    from src
)

select *
from final
