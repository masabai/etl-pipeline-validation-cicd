{{ config(materialized='table') }}

with src as (
    select *
    from {{ ref('stg_fda_rpsr') }}
),

final as (
    select
        row_number() over(order by primaryid) as rpsr_sk,
        *
    from src
)

select *
from final
