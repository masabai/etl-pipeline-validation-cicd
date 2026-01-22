{{ config(materialized='table') }}

with src as (
    select *
    from {{ ref('stg_fda_drug') }}
),

final as (
    select
        row_number() over(order by primaryid) as drug_sk,
        *
    from src
)

select *
from final
