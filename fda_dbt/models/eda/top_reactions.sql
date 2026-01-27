{{ config(
    materialized='table',
    schema='EDA'
) }}

with reaction_counts as (
    select reac_sk, count(distinct caseid) as num_cases
    from {{ ref('fact_adverse_events') }}
    group by reac_sk
)
select r.pt as reaction, f.num_cases
from reaction_counts f
join {{ ref('dim_reaction') }} r on f.reac_sk = r.reac_sk
order by num_cases desc
limit 10
