{{ config(materialized='table') }}

with src as (
    select *
    from {{ ref('stg_fda_indi') }}
),

final as (
    select
        row_number() over(order by primaryid) as indi_sk,
        *
    from src
)

select *
from final
